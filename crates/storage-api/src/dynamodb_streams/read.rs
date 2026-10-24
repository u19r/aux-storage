use http_error::HttpApiError;
use storage_types::{
    DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE, DYNAMODB_STREAM_RECORDS_LIMIT_MAX, DescribeTableRequest,
    GetRecordsRequest, GetRecordsResponse, GetShardIteratorRequest, GetShardIteratorResponse,
    GetStreamRecordsRequest, StreamRecord, TableDescription, TableName,
};

use super::{
    iterator::{StreamIterator, parse_stream_arn, validate_streams_page_limit},
    record::dynamodb_stream_record,
};
use crate::{constants::MERGED_SHARD_ID, manager::StorageApiManager, types::Response};

pub(crate) struct DynamoStreamReader<'manager, M: ?Sized> {
    pub(crate) manager: &'manager M,
}

impl<M: StorageApiManager + ?Sized> DynamoStreamReader<'_, M> {
    pub(super) async fn load_stream_table(
        &self,
        stream_arn: &str,
    ) -> Result<TableDescription, HttpApiError> {
        let parsed = parse_stream_arn(stream_arn)?;
        let response = self
            .manager
            .describe_table(DescribeTableRequest {
                table_name: storage_types::TableName::new(parsed.table_name),
            })
            .await
            .map_err(|error| {
                if error.error_type.rsplit('#').next() == Some("ResourceNotFoundException") {
                    stream_not_found()
                } else {
                    error
                }
            })?;
        let Response::DescribeTable(response) = response else {
            return Err(storage_types::StorageError::internal(
                "table description response differs",
            )
            .into());
        };
        if response.table.latest_stream_arn.as_deref() != Some(stream_arn) {
            return Err(stream_not_found());
        }
        Ok(response.table)
    }
}

pub(super) fn stream_not_found() -> HttpApiError {
    HttpApiError::dynamodb_protocol_error(
        "ResourceNotFoundException",
        DYNAMODB_RESOURCE_NOT_FOUND_MESSAGE,
        400,
    )
}

impl<M: StorageApiManager + ?Sized> DynamoStreamReader<'_, M> {
    pub(crate) async fn get_shard_iterator(
        &self,
        request: GetShardIteratorRequest,
    ) -> Result<Response, HttpApiError> {
        let stream = self.load_stream_table(&request.stream_arn).await?;
        if stream.stream_specification.is_none() || request.shard_id != MERGED_SHARD_ID {
            return Err(stream_not_found());
        }
        let iterator = self.create_iterator(request, stream.table_name).await?;
        Ok(Response::GetShardIterator(GetShardIteratorResponse {
            shard_iterator: iterator.encode(),
        }))
    }

    async fn create_iterator(
        &self,
        request: GetShardIteratorRequest,
        table_name: TableName,
    ) -> Result<StreamIterator, HttpApiError> {
        let iterator = StreamIterator::new(request.stream_arn, request.shard_id);
        Ok(match request.shard_iterator_type.as_str() {
            "TRIM_HORIZON" => iterator,
            "LATEST" => {
                let latest = self.manager.latest_table_stream_pointer(table_name).await?;
                iterator.with_page_token(latest.map(|id| id.to_string()))
            }
            "AT_SEQUENCE_NUMBER" => iterator.with_sequence_number(request.sequence_number, false),
            "AFTER_SEQUENCE_NUMBER" => iterator.with_sequence_number(request.sequence_number, true),
            _ => {
                return Err(HttpApiError::validation_error(format!(
                    "Value '{}' at 'shardIteratorType' failed to satisfy constraint: Member must \
                     satisfy enum value set: [TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, \
                     AFTER_SEQUENCE_NUMBER]",
                    request.shard_iterator_type
                )));
            }
        })
    }

    pub(crate) async fn get_records(
        &self,
        request: GetRecordsRequest,
    ) -> Result<Response, HttpApiError> {
        validate_streams_page_limit(request.limit, DYNAMODB_STREAM_RECORDS_LIMIT_MAX)?;
        let iterator = StreamIterator::decode(&request.shard_iterator)?;
        if iterator.shard_id != MERGED_SHARD_ID {
            return Err(stream_not_found());
        }
        let table = self.load_stream_table(&iterator.stream_arn).await?;
        let specification = table
            .stream_specification
            .as_ref()
            .ok_or_else(stream_not_found)?;
        let start = iterator.page_start()?;
        let (records, checkpoint) = self
            .load_records(
                table.table_name,
                start,
                request.limit.unwrap_or(DYNAMODB_STREAM_RECORDS_LIMIT_MAX),
            )
            .await?;
        let next = checkpoint.or_else(|| records.last().and_then(|record| record.cursor.clone()));
        Ok(Response::GetRecords(GetRecordsResponse {
            next_shard_iterator: next.map(|token| {
                StreamIterator::new(iterator.stream_arn, iterator.shard_id)
                    .with_page_token(Some(token))
                    .encode()
            }),
            records: records
                .into_iter()
                .map(|record| dynamodb_stream_record(record, specification))
                .collect(),
        }))
    }

    async fn load_records(
        &self,
        name: TableName,
        start: Option<String>,
        limit: u32,
    ) -> Result<(Vec<StreamRecord>, Option<String>), HttpApiError> {
        let response = self
            .manager
            .get_stream_records(GetStreamRecordsRequest::table(name, start, Some(limit)))
            .await?;
        let Response::GetStreamRecords(response) = response else {
            return Err(
                storage_types::StorageError::internal("stream records response differs").into(),
            );
        };
        Ok(response.into_parts())
    }
}
