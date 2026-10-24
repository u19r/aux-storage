//! A user-stream append decides its mode inside one native write attempt.

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_common::{partition::PartitionInfo, retry::retry_native_conflicts};
use storage_types::{
    SemanticDecision, StorageError, StorageResult, StreamItemId, StreamName, TableName,
    TimestampMillis, UserStreamName,
};

use crate::{
    Stream, StreamAppendAllocation, StreamAppendOperationId, StreamAttempt, StreamDataType,
    StreamError, StreamPartitioningMode, StreamPort, StreamResult, StreamRowValue,
    StreamValidationKind, TableAppendCapture, TableAppendRequest, constants::STREAM_WRITE_RETRY,
};

#[derive(Clone)]
enum AppendTarget {
    User(UserStreamName),
    Table(StreamAppendOperationId, TableName),
}

/// Validated append payload with its request-time sample and timestamp.
#[derive(Clone)]
pub struct UserStreamAppend {
    data: Vec<u8>,
    partition_key: Option<String>,
    captured_at: TimestampMillis,
    sample_entropy: u128,
}

impl UserStreamAppend {
    pub fn validate_data(item_data: &[u8]) -> StreamResult<()> {
        if item_data.is_empty() {
            return Err(StreamError::validation(StreamValidationKind::ItemDataEmpty));
        }
        if item_data.len() > crate::MAX_STREAM_ITEM_DATA_BYTES {
            return Err(StreamError::validation(
                StreamValidationKind::ItemDataTooLarge,
            ));
        }
        Ok(())
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn captured_at(&self) -> TimestampMillis {
        self.captured_at
    }

    pub fn try_new(
        data: Vec<u8>,
        partition_key: Option<String>,
        captured_at: TimestampMillis,
        sample_entropy: u128,
    ) -> StreamResult<Self> {
        Self::validate_data(&data)?;
        Ok(Self {
            data,
            partition_key,
            captured_at,
            sample_entropy,
        })
    }

    fn into_row(self, name: StreamName) -> StreamRowValue {
        StreamRowValue::new(name, self.data, StreamDataType::Text, self.captured_at)
    }
}

/// One accepted append over the catalog and protected partition observations.
pub struct AcceptedUserAppend<'native, A: StreamAttempt> {
    operation: PhysicalRecordOperation<'native, A>,
    row: UserStreamAppendRow,
}

// Only this owner can construct accepted rows. Public callers cannot bypass
// payload validation or manufacture the protected partition observation.
enum UserStreamAppendRow {
    Single(StreamRowValue),
    KeyOrdered {
        partition: PartitionInfo,
        row: StreamRowValue,
    },
}

#[derive(Clone)]
struct AppendInput {
    target: AppendTarget,
    append: UserStreamAppend,
    item_id_allocation: crate::StreamItemIdAllocation,
    request: Option<TableAppendRequest>,
}

pub(crate) async fn append_item<P: StreamPort>(
    port: &P,
    name: UserStreamName,
    item_data: &[u8],
    partition_key: Option<&str>,
) -> StreamResult<StreamItemId> {
    append(port, AppendTarget::User(name), item_data, partition_key).await
}

pub(crate) async fn append_table_item<P: StreamPort>(
    port: &P,
    operation: StreamAppendOperationId,
    table_name: TableName,
    item_data: &[u8],
) -> StreamResult<StreamItemId> {
    append(
        port,
        AppendTarget::Table(operation, table_name),
        item_data,
        None,
    )
    .await
}

async fn append<P: StreamPort>(
    port: &P,
    target: AppendTarget,
    item_data: &[u8],
    partition_key: Option<&str>,
) -> StreamResult<StreamItemId> {
    UserStreamAppend::validate_data(item_data)?;
    let input = prepare_append_input(port, target, item_data, partition_key);
    let request = input.request.clone();
    let capture = port.table_append_capture();
    let (staged, stamp) = retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let input = input.clone();
        let capture = capture.clone();
        port.with_committed_stream_attempt(move |attempt| {
            Box::pin(async move { execute_append(attempt, &capture, input).await })
        })
    })
    .await
    .map_err(|error| append_error(error, request.as_ref()))?;
    staged
        .resolve(stamp)
        .map_err(|error| append_error(error, request.as_ref()))
}

async fn execute_append<A: StreamAttempt, C: TableAppendCapture<A>>(
    attempt: &mut A,
    capture: &C,
    input: AppendInput,
) -> StorageResult<StreamAppendAllocation> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        if let Some(request) = &input.request
            && let crate::TableAppendReconciliation::Durable(id) =
                capture.reconcile(attempt, request).await?
        {
            return Ok(StreamAppendAllocation::AttemptAllocated(id));
        }
        let captured = match &input.request {
            Some(request) => capture.capture(attempt, request, &input.append).await?,
            None => None,
        };
        let staged = prepare_append(attempt, input).await?;
        if let Some(captured) = captured {
            capture.store_response(attempt, captured, staged).await?;
        }
        Ok(staged)
    }
    .await;
    settlement.complete(result)
}

fn prepare_append_input<P: StreamPort>(
    port: &P,
    target: AppendTarget,
    item_data: &[u8],
    partition_key: Option<&str>,
) -> AppendInput {
    let request = match &target {
        AppendTarget::Table(operation, table) => Some(TableAppendRequest::new(
            *operation,
            table.clone(),
            item_data,
        )),
        AppendTarget::User(_) => None,
    };
    AppendInput {
        request,
        target,
        append: UserStreamAppend {
            data: item_data.to_vec(),
            partition_key: partition_key.map(str::to_owned),
            captured_at: port.now(),
            sample_entropy: rand::random(),
        },
        item_id_allocation: port.item_id_allocation(),
    }
}

async fn prepare_append<A: StreamAttempt>(
    attempt: &mut A,
    input: AppendInput,
) -> StorageResult<StreamAppendAllocation> {
    let accepted = match input.target {
        AppendTarget::User(name) => {
            let stream = crate::stream_catalog::load_user_stream_in_attempt(attempt, &name)
                .await?
                .into_result()?;
            accept_user_append_in_attempt(attempt, stream, input.append)
                .await?
                .into_result()?
        }
        AppendTarget::Table(_, name) => AcceptedUserAppend {
            operation: PhysicalRecordOperation::new(attempt),
            row: UserStreamAppendRow::Single(
                input.append.into_row(StreamName::table_stream(&name)),
            ),
        },
    };
    accepted.append(input.item_id_allocation).await
}

pub async fn accept_user_append_in_attempt<'native, A: StreamAttempt>(
    attempt: &'native mut A,
    stream: Stream,
    input: UserStreamAppend,
) -> StorageResult<SemanticDecision<AcceptedUserAppend<'native, A>>> {
    let mut operation = PhysicalRecordOperation::new(attempt);
    match input.prepare_row(operation.as_mut(), stream).await? {
        SemanticDecision::Accepted(row) => Ok(SemanticDecision::Accepted(AcceptedUserAppend {
            operation,
            row,
        })),
        SemanticDecision::Rejected(error) => Ok(SemanticDecision::Rejected(error)),
    }
}

impl UserStreamAppend {
    async fn prepare_row<A: StreamAttempt>(
        self,
        attempt: &mut A,
        stream: Stream,
    ) -> StorageResult<SemanticDecision<UserStreamAppendRow>> {
        if stream.partitioning_mode == StreamPartitioningMode::Single {
            return Ok(SemanticDecision::Accepted(UserStreamAppendRow::Single(
                self.into_row(stream.internal_id),
            )));
        }
        if A::PARTITIONING_SUPPORT != crate::StreamPartitioningSupport::KeyOrdered {
            return Err(StorageError::internal(
                "stream catalog requests unsupported key-ordered partitioning",
            ));
        }
        let Some(key) = self.partition_key.as_deref().filter(|key| !key.is_empty()) else {
            return Ok(SemanticDecision::Rejected(
                StreamError::validation(StreamValidationKind::MissingPartitionKey)
                    .into_storage_error(),
            ));
        };
        let family = attempt.load_partition_family(&stream.internal_id).await?;
        let partition = family.route_ordered_log(key.as_bytes())?;
        if let Some(sample) = partition.sample_append(
            key.as_bytes(),
            self.data.len(),
            self.sample_entropy,
            self.captured_at,
        ) {
            attempt
                .store_partition_sample(&stream.internal_id, &sample)
                .await?;
        }
        Ok(SemanticDecision::Accepted(
            UserStreamAppendRow::KeyOrdered {
                partition: partition.clone(),
                row: self.into_row(stream.internal_id),
            },
        ))
    }
}

impl<A: StreamAttempt> AcceptedUserAppend<'_, A> {
    async fn append(
        self,
        allocation: crate::StreamItemIdAllocation,
    ) -> StorageResult<StreamAppendAllocation> {
        use crate::StreamItemIdAllocation::{AttemptAllocated, CommitVersionstamped};
        let Self { mut operation, row } = self;
        let attempt = operation.as_mut();
        let result = match (row, allocation) {
            (UserStreamAppendRow::Single(row), AttemptAllocated) => attempt
                .append_attempt_allocated_user_item(row)
                .await
                .map(StreamAppendAllocation::AttemptAllocated),
            (UserStreamAppendRow::Single(row), CommitVersionstamped) => attempt
                .append_versionstamped_user_item(row)
                .await
                .map(StreamAppendAllocation::Versionstamped),
            (UserStreamAppendRow::KeyOrdered { partition, row }, AttemptAllocated) => attempt
                .append_attempt_allocated_partitioned_user_item(&partition, row)
                .await
                .map(StreamAppendAllocation::AttemptAllocated),
            (UserStreamAppendRow::KeyOrdered { partition, row }, CommitVersionstamped) => attempt
                .append_partitioned_user_item(&partition, row)
                .await
                .map(StreamAppendAllocation::Versionstamped),
        };
        operation.complete(result)
    }
}

fn append_error(error: StorageError, request: Option<&TableAppendRequest>) -> StreamError {
    if error.is_commit_unknown()
        && let Some(request) = request
    {
        return StreamError::from_storage_error(StorageError::sync_submission_ambiguous(
            Some(request.operation().token()),
            "table append commit outcome is unresolved",
        ));
    }
    StreamError::from_storage_error(error)
}

pub(crate) async fn reconcile_table_append<P: StreamPort>(
    port: &P,
    request: TableAppendRequest,
) -> StreamResult<crate::TableAppendReconciliation> {
    let capture = port.table_append_capture();
    port.with_stream_attempt(storage_types::AttemptMode::ReadOnly, move |attempt| {
        Box::pin(async move { capture.reconcile(attempt, &request).await })
    })
    .await
    .map_err(StreamError::from_storage_error)
}
