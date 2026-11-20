use storage_types::{
    AttributeMap, DynamoDbStreamsRecord, KeyAttributes, StreamRecord, StreamRecordDetails,
    StreamSpecification, StreamUserIdentity, StreamViewType, TableName, TimestampMillis,
    TimestampSecondsFractional,
};

/// Image pairs belong exclusively to the standard DynamoDB Streams adapter.
pub(crate) struct DynamoStreamRecord {
    pub size_bytes: u64,
    pub change: stream_provider::StreamChange,
    pub cursor: Option<String>,
    pub source_table_name: Option<TableName>,
    pub keys: KeyAttributes,
    pub sequence_number: String,
    pub created_at: TimestampMillis,
    pub user_identity: Option<StreamUserIdentity>,
    pub old_image: Option<AttributeMap>,
    pub new_image: Option<AttributeMap>,
}

impl From<DynamoStreamRecord> for StreamRecord {
    fn from(record: DynamoStreamRecord) -> Self {
        Self {
            cursor: record.cursor,
            source_table_name: record.source_table_name,
            keys: record.keys,
            sequence_number: record.sequence_number,
            item: record.new_image,
            system_position: None,
        }
    }
}

pub(super) fn dynamodb_stream_record(
    record: DynamoStreamRecord,
    stream_specification: &StreamSpecification,
    stream_arn: &str,
) -> DynamoDbStreamsRecord {
    let event_name = match record.change {
        stream_provider::StreamChange::Insert => "INSERT",
        stream_provider::StreamChange::Modify => "MODIFY",
        stream_provider::StreamChange::Remove => "REMOVE",
    }
    .to_string();
    let view = stream_specification
        .stream_view_type
        .clone()
        .unwrap_or(StreamViewType::NewAndOldImages);
    let sequence_number = record.sequence_number;
    DynamoDbStreamsRecord {
        event_id: sequence_number.clone(),
        event_name,
        event_version: "1.0".to_string(),
        event_source: "aws:dynamodb".to_string(),
        aws_region: "us-east-1".to_string(),
        event_source_arn: stream_arn.to_string(),
        user_identity: record.user_identity,
        dynamodb: StreamRecordDetails {
            // AWS rounds the creation time down to the second.
            approximate_creation_date_time: Some(TimestampSecondsFractional::from_seconds(
                TimestampSecondsFractional::from(record.created_at)
                    .as_seconds()
                    .floor(),
            )),
            keys: record.keys,
            sequence_number,
            stream_view_type: view,
            size_bytes: record.size_bytes,
            old_image: record.old_image,
            new_image: record.new_image,
        },
    }
}
