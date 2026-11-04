//! A user-stream append decides its mode inside one native write attempt.

use infrastructure_provider::physical_attempt::PhysicalRecordOperation;
use storage_common::{partition::PartitionInfo, retry::retry_native_conflicts};
use storage_types::{
    SemanticDecision, StorageError, StorageResult, StreamItemId, StreamName, TimestampMillis,
    UserStreamName,
};

use crate::{
    Stream, StreamAppendAllocation, StreamAttempt, StreamDataType, StreamError,
    StreamPartitioningMode, StreamPort, StreamResult, StreamRowValue, StreamValidationKind,
    constants::STREAM_WRITE_RETRY,
};

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

pub(crate) async fn append_item<P: StreamPort>(
    port: &P,
    name: UserStreamName,
    item_data: &[u8],
    partition_key: Option<&str>,
) -> StreamResult<StreamItemId> {
    let input = UserStreamAppend::try_new(
        item_data.to_vec(),
        partition_key.map(str::to_owned),
        port.now(),
        rand::random(),
    )?;
    let allocation = port.item_id_allocation();
    let (staged, stamp) = retry_native_conflicts(port, STREAM_WRITE_RETRY, || {
        let input = input.clone();
        let name = name.clone();
        port.with_committed_stream_attempt(move |attempt| {
            Box::pin(async move {
                let stream = crate::stream_catalog::load_user_stream_in_attempt(attempt, &name)
                    .await?
                    .into_result()?;
                accept_user_append_in_attempt(attempt, stream, input)
                    .await?
                    .into_result()?
                    .append(allocation)
                    .await
            })
        })
    })
    .await
    .map_err(StreamError::from_storage_error)?;
    staged
        .resolve(stamp)
        .map_err(StreamError::from_storage_error)
}

/// The catalog owner has protected this bound incarnation in the same attempt.
pub async fn append_table_row_in_attempt<A: StreamAttempt>(
    attempt: &mut A,
    table: &storage_types::BoundTableInfo,
    append: UserStreamAppend,
    allocation: crate::StreamItemIdAllocation,
) -> StorageResult<StreamAppendAllocation> {
    AcceptedUserAppend {
        operation: PhysicalRecordOperation::new(attempt),
        row: UserStreamAppendRow::Single(append.into_row(table.history_id.append_stream_name())),
    }
    .append(allocation)
    .await
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
