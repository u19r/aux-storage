use storage_types::{
    DynamoDbStreamsRecord, StreamRecord, StreamRecordDetails, StreamSpecification, StreamViewType,
};

pub(super) fn dynamodb_stream_record(
    record: StreamRecord,
    stream_specification: &StreamSpecification,
) -> DynamoDbStreamsRecord {
    let event_name = match (&record.old_image, &record.new_image) {
        (None, Some(_)) => "INSERT",
        (Some(_), None) => "REMOVE",
        _ => "MODIFY",
    }
    .to_string();
    let size_bytes = stream_record_size_bytes(&record);
    let sequence_number = record.sequence_number;
    DynamoDbStreamsRecord {
        event_id: sequence_number.clone(),
        event_name,
        event_version: "1.0".to_string(),
        event_source: "aws:dynamodb".to_string(),
        aws_region: "us-east-1".to_string(),
        dynamodb: StreamRecordDetails {
            keys: record.keys,
            sequence_number,
            stream_view_type: stream_specification
                .stream_view_type
                .clone()
                .unwrap_or(StreamViewType::NewAndOldImages),
            size_bytes,
            old_image: record.old_image,
            new_image: record.new_image,
        },
    }
}

fn stream_record_size_bytes(record: &StreamRecord) -> u64 {
    serde_json::to_vec(&(&record.keys, &record.old_image, &record.new_image))
        .map(|bytes| bytes.len() as u64)
        .unwrap_or(0)
}
