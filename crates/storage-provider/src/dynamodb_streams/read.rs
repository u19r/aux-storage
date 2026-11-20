use storage_types::{
    BoundTableInfo, GetRecordsRequest, GetRecordsResponse, GetStreamRecordsRequest, StorageEnum,
    StorageError, StorageResult, TableHistoryTarget, TableName, context::WrappedError as _,
    stream_arn_table_name, stream_identity,
};

use super::{
    iterator::{StreamIterator, invalid_shard_iterator, validate_streams_page_limit},
    record::dynamodb_stream_record,
};
use crate::{
    DynamoPort, StreamRetentionPort,
    dynamo_table_stream::{
        TableStreamImageSelection, TableStreamReadOptions, read_table_stream_page,
    },
};

pub trait DynamoStreamRead: Send + Sync + 'static {
    type Response: Send + 'static;
    fn table_name(&self) -> &TableName;
    fn stream_arn(&self) -> &str;
    /// The error for a stream the request names but the table does not have.
    fn stream_missing(&self) -> StorageError {
        stream_not_found()
    }
    fn metadata_failure(&self, error: StorageError) -> StorageResult<Self::Response>
    where Self: Sized {
        Err(match error.to_enum() {
            StorageEnum::TableNotFound { .. } => self.stream_missing(),
            _ => error,
        })
    }
    fn read<A: StreamRetentionPort>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send;

    fn execute<A: StreamRetentionPort>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send
    where
        Self: Sized,
    {
        async move {
            require_stream_table(table, self.table_name(), self.stream_arn())
                .map_err(|_| self.stream_missing())?;
            self.read(attempt, table, now).await
        }
    }
}

pub(crate) async fn read_dynamo_stream<P: DynamoPort, S: DynamoStreamRead>(
    port: &P,
    request: S,
) -> StorageResult<S::Response> {
    let now = port.now();
    let state = std::sync::Arc::new(request);
    crate::dynamo_operation_port::read_operation(
        port,
        crate::constants::RANGE_READ,
        move |attempt, session| {
            Box::pin(async move {
                let request = state.as_ref();
                let observed = session.resolve_table(attempt, request.table_name()).await;
                let (table, publication) = match observed {
                    Ok(observed) => observed,
                    Err(error) => return Ok((request.metadata_failure(error)?, None)),
                };
                if let Err(error) = table.require_data_readable() {
                    return Ok((request.metadata_failure(error)?, None));
                }
                let response = request.execute(attempt, &table, now).await?;
                Ok((response, publication))
            })
        },
    )
    .await
}

fn require_stream_table(
    table: &BoundTableInfo,
    table_name: &TableName,
    arn: &str,
) -> StorageResult<()> {
    if &table.info.table_name != table_name
        || !table.info.has_enabled_stream()
        || table
            .stream_identity()
            .is_none_or(|identity| stream_identity(&table.info.table_name, identity).0 != arn)
    {
        return Err(stream_not_found());
    }
    Ok(())
}

/// An unknown shard of an existing stream.
pub(crate) fn shard_not_found() -> StorageError {
    StorageEnum::ResourceNotFound {
        resource_type: "shard",
        resource_id: String::new(),
    }
    .into()
}

/// A stream ARN naming no stream, reported with the ARN as DescribeStream does.
pub(crate) fn stream_arn_not_found(arn: &str) -> StorageError {
    StorageEnum::ResourceNotFound {
        resource_type: "stream",
        resource_id: arn.to_owned(),
    }
    .into()
}

pub(crate) fn stream_not_found() -> StorageError {
    StorageEnum::ResourceNotFound {
        resource_type: "stream",
        resource_id: String::new(),
    }
    .into()
}

pub struct GetDynamoStreamRecords {
    iterator: StreamIterator,
    table_name: TableName,
    limit: u32,
}

impl TryFrom<GetRecordsRequest> for GetDynamoStreamRecords {
    type Error = StorageError;
    fn try_from(request: GetRecordsRequest) -> StorageResult<Self> {
        validate_streams_page_limit(request.limit, 1_000)?;
        let iterator = StreamIterator::decode(&request.shard_iterator)?;
        let table_name = stream_arn_table_name(iterator.stream_arn.as_str())?;
        Ok(Self {
            iterator,
            table_name,
            limit: request.limit.unwrap_or(1_000),
        })
    }
}

impl DynamoStreamRead for GetDynamoStreamRecords {
    type Response = GetRecordsResponse;
    fn table_name(&self) -> &TableName {
        &self.table_name
    }
    fn stream_arn(&self) -> &str {
        &self.iterator.stream_arn
    }
    async fn read<A: StreamRetentionPort>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> StorageResult<Self::Response> {
        let start = self.iterator.page_token.clone();
        let response = read_table_stream_page(
            attempt,
            table,
            &GetStreamRecordsRequest::table_at_incarnation(
                TableHistoryTarget::from(table),
                start,
                Some(self.limit),
            ),
            TableStreamReadOptions {
                now,
                selection: TableStreamImageSelection::DynamoDbImages,
                shard_id: Some(&self.iterator.shard_id),
            },
        )
        .await
        .map_err(|error| match error.as_ref() {
            StorageEnum::HistoryUnavailable => StorageEnum::TrimmedDataAccess.into(),
            StorageEnum::ResourceNotFound { resource_type, .. } if *resource_type == "shard" => {
                invalid_shard_iterator()
            }
            _ => error,
        })?;
        let specification = table
            .info
            .stream_specification
            .as_ref()
            .ok_or_else(stream_not_found)?;
        let records = response.records;
        let checkpoint = response.checkpoint;
        let next = checkpoint.or_else(|| records.last().and_then(|record| record.cursor.clone()));
        let stream_arn = self.iterator.stream_arn.clone();
        Ok(GetRecordsResponse {
            next_shard_iterator: (!response.sealed_end).then(|| match next {
                Some(token) => StreamIterator::new(
                    self.iterator.stream_arn.clone(),
                    self.iterator.shard_id.clone(),
                )
                .with_page_token(Some(token))
                .encode(),
                None => self.iterator.encode(),
            }),
            records: records
                .into_iter()
                .map(|record| dynamodb_stream_record(record, specification, &stream_arn))
                .collect(),
        })
    }
}
