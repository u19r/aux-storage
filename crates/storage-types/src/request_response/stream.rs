use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    AttributeValue, KeySchemaElement, StreamRecord, StreamViewType, TableName,
    TimestampSecondsFractional,
};

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetStreamRecordsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<TableName>,

    #[serde(default)]
    pub system_stream: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 8192)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetStreamRecordsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<TableName>,
    pub records: Vec<StreamRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamsRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<TableName>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_stream_arn: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 100)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ListStreamsResponse {
    pub streams: Vec<StreamDescriptor>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_stream_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamDescriptor {
    pub stream_arn: String,
    pub table_name: TableName,
    pub stream_label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeStreamRequest {
    pub stream_arn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exclusive_start_shard_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 100)]
    pub limit: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_filter: Option<ShardFilter>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct ShardFilter {
    #[serde(rename = "Type")]
    pub filter_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct DescribeStreamResponse {
    pub stream_description: StreamDescription,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamDescription {
    pub stream_arn: String,
    pub stream_label: String,
    pub stream_status: String,
    pub stream_view_type: StreamViewType,
    pub creation_request_date_time: TimestampSecondsFractional,
    pub table_name: TableName,
    pub key_schema: Vec<KeySchemaElement>,
    pub shards: Vec<Shard>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_evaluated_shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct Shard {
    pub shard_id: String,
    pub sequence_number_range: SequenceNumberRange,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_shard_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct SequenceNumberRange {
    pub starting_sequence_number: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ending_sequence_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetShardIteratorRequest {
    pub stream_arn: String,
    pub shard_id: String,
    pub shard_iterator_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence_number: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetShardIteratorResponse {
    pub shard_iterator: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetRecordsRequest {
    pub shard_iterator: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(default = 100, minimum = 1, maximum = 1000)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct GetRecordsResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_shard_iterator: Option<String>,
    pub records: Vec<DynamoDbStreamsRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DynamoDbStreamsRecord {
    #[serde(rename = "eventID")]
    pub event_id: String,
    pub event_name: String,
    pub event_version: String,
    pub event_source: String,
    #[serde(rename = "awsRegion")]
    pub aws_region: String,
    pub dynamodb: StreamRecordDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "PascalCase")]
pub struct StreamRecordDetails {
    pub keys: HashMap<String, AttributeValue>,
    pub sequence_number: String,
    pub stream_view_type: StreamViewType,
    pub size_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_image: Option<HashMap<String, AttributeValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_image: Option<HashMap<String, AttributeValue>>,
}
