use storage_types::{
    AttributeMap, DynamoDbStreamsRecord, KeyAttributes, StreamRecord, StreamRecordDetails,
    StreamSpecification, StreamUserIdentity, StreamViewType, TableName, TimestampMillis,
    TimestampSecondsFractional,
};

/// Image pairs belong exclusively to the standard DynamoDB Streams adapter.
pub(crate) struct DynamoStreamRecord {
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
    mut record: DynamoStreamRecord,
    stream_specification: &StreamSpecification,
    stream_arn: &str,
) -> DynamoDbStreamsRecord {
    let event_name = match (&record.old_image, &record.new_image) {
        (None, Some(_)) => "INSERT",
        (Some(_), None) => "REMOVE",
        _ => "MODIFY",
    }
    .to_string();
    let view = stream_specification
        .stream_view_type
        .clone()
        .unwrap_or(StreamViewType::NewAndOldImages);
    match view {
        StreamViewType::KeysOnly => {
            record.old_image = None;
            record.new_image = None;
        }
        StreamViewType::NewImage => record.old_image = None,
        StreamViewType::OldImage => record.new_image = None,
        StreamViewType::NewAndOldImages => {}
    }
    let size_bytes = stream_record_size_bytes(&record);
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
            approximate_creation_date_time: Some(TimestampSecondsFractional::from(
                record.created_at,
            )),
            keys: record.keys,
            sequence_number,
            stream_view_type: view,
            size_bytes,
            old_image: record.old_image,
            new_image: record.new_image,
        },
    }
}

fn stream_record_size_bytes(record: &DynamoStreamRecord) -> u64 {
    serde_json::to_vec(&(&record.keys, &record.old_image, &record.new_image))
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}
