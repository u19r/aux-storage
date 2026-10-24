use http_error::HttpApiError;
use storage_types::{
    DescribeStreamRequest, DescribeStreamResponse, SequenceNumberRange, Shard, StreamDescription,
};

use super::{
    DynamoStreamReader,
    iterator::{parse_stream_arn, validate_streams_page_limit},
    read::stream_not_found,
};
use crate::{constants::MERGED_SHARD_ID, manager::StorageApiManager, types::Response};

impl<M: StorageApiManager + ?Sized> DynamoStreamReader<'_, M> {
    pub(crate) async fn describe_stream(
        &self,
        request: DescribeStreamRequest,
    ) -> Result<Response, HttpApiError> {
        validate_streams_page_limit(request.limit, 100)?;
        let stream = self.load_stream_table(&request.stream_arn).await?;
        let stream_view_type = stream
            .stream_specification
            .and_then(|spec| spec.stream_view_type)
            .ok_or_else(stream_not_found)?;
        let shards = describe_shards(&request);
        let stream_label = parse_stream_arn(&request.stream_arn)?
            .stream_label
            .to_owned();
        Ok(Response::DescribeStream(DescribeStreamResponse {
            stream_description: StreamDescription {
                stream_arn: request.stream_arn,
                stream_label,
                stream_status: "ENABLED".to_string(),
                stream_view_type,
                creation_request_date_time: stream.created_at,
                table_name: stream.table_name,
                key_schema: stream.key_schema,
                shards,
                last_evaluated_shard_id: None,
            },
        }))
    }
}

fn describe_shards(request: &DescribeStreamRequest) -> Vec<Shard> {
    if request
        .shard_filter
        .as_ref()
        .is_some_and(|filter| filter.filter_type == "CHILD_SHARDS")
        || request.exclusive_start_shard_id.as_deref() == Some(MERGED_SHARD_ID)
    {
        return Vec::new();
    }
    vec![Shard {
        shard_id: MERGED_SHARD_ID.to_string(),
        sequence_number_range: SequenceNumberRange {
            starting_sequence_number: "0".to_string(),
            ending_sequence_number: None,
        },
        parent_shard_id: None,
    }]
}
