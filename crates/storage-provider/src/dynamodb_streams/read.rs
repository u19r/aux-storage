use storage_types::{
    AttemptMode, BoundTableInfo, GetRecordsRequest, GetRecordsResponse, GetStreamRecordsRequest,
    StorageEnum, StorageError, StorageResult, TableHistoryTarget, context::WrappedError as _,
    stream_identity,
};

use super::{
    iterator::{StreamIterator, validate_streams_page_limit},
    record::dynamodb_stream_record,
};
use crate::{
    DynamoPort,
    dynamo_table_stream::{
        DynamoTableStreamAttempt, TableStreamImageSelection, TableStreamReadOptions,
        read_table_stream_page,
    },
};

pub trait DynamoStreamRead: Send + 'static {
    type Response: Send + 'static;
    fn target(&self) -> &TableHistoryTarget;
    fn stream_arn(&self) -> &str;
    fn metadata_failure(self, error: StorageError) -> StorageResult<Self::Response>
    where Self: Sized {
        Err(match error.to_enum() {
            StorageEnum::TableNotFound { .. } => stream_not_found(),
            _ => error,
        })
    }
    fn read<A: DynamoTableStreamAttempt>(
        self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send;

    fn execute<A: DynamoTableStreamAttempt>(
        self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> impl Future<Output = StorageResult<Self::Response>> + Send
    where
        Self: Sized,
    {
        async move {
            require_stream_table(table, self.target(), self.stream_arn())?;
            self.read(attempt, table, now).await
        }
    }
}

pub(crate) async fn read_dynamo_stream<P: DynamoPort, S: DynamoStreamRead>(
    port: &P,
    request: S,
) -> StorageResult<S::Response> {
    let now = port.now();
    let cache = port.metadata_cache();
    let publication_cache = cache.clone();
    let (response, publication) = port
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let observed =
                    crate::resolve_table_in_attempt(attempt, &cache, &request.target().table_name)
                        .await;
                let (table, publication) = match observed {
                    Ok(observed) => observed,
                    Err(error) => return Ok((request.metadata_failure(error)?, None)),
                };
                let response = request.execute(attempt, &table, now).await?;
                Ok((response, publication))
            })
        })
        .await?;
    publication_cache.publish(publication);
    Ok(response)
}

fn require_stream_table(
    table: &BoundTableInfo,
    target: &TableHistoryTarget,
    arn: &str,
) -> StorageResult<()> {
    if table.history_id != target.history_id
        || table.info.table_name != target.table_name
        || !table.info.has_enabled_stream()
        || stream_identity(&table.info.table_name, table.stream_identity()).0 != arn
    {
        return Err(stream_not_found());
    }
    Ok(())
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
    target: TableHistoryTarget,
    limit: u32,
}

impl TryFrom<GetRecordsRequest> for GetDynamoStreamRecords {
    type Error = StorageError;
    fn try_from(request: GetRecordsRequest) -> StorageResult<Self> {
        validate_streams_page_limit(request.limit, 1_000)?;
        let iterator = StreamIterator::decode(&request.shard_iterator)?;
        let target = TableHistoryTarget::try_from(iterator.stream_arn.as_str())?;
        Ok(Self {
            iterator,
            target,
            limit: request.limit.unwrap_or(1_000),
        })
    }
}

impl DynamoStreamRead for GetDynamoStreamRecords {
    type Response = GetRecordsResponse;
    fn target(&self) -> &TableHistoryTarget {
        &self.target
    }
    fn stream_arn(&self) -> &str {
        &self.iterator.stream_arn
    }
    async fn read<A: DynamoTableStreamAttempt>(
        self,
        attempt: &mut A,
        table: &BoundTableInfo,
        now: storage_types::TimestampMillis,
    ) -> StorageResult<Self::Response> {
        let start = self.iterator.page_token.clone();
        let response = read_table_stream_page(
            attempt,
            table,
            &GetStreamRecordsRequest::table_at_incarnation(self.target, start, Some(self.limit)),
            TableStreamReadOptions {
                now,
                selection: TableStreamImageSelection::DynamoDbImages,
                shard_id: Some(&self.iterator.shard_id),
            },
        )
        .await
        .map_err(|error| match error.as_ref() {
            StorageEnum::HistoryUnavailable => StorageEnum::TrimmedDataAccess.into(),
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
                Some(token) => {
                    StreamIterator::new(self.iterator.stream_arn, self.iterator.shard_id)
                        .with_page_token(Some(token))
                        .encode()
                }
                None => self.iterator.encode(),
            }),
            records: records
                .into_iter()
                .map(|record| dynamodb_stream_record(record, specification, &stream_arn))
                .collect(),
        })
    }
}
