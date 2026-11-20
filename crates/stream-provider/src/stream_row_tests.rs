use storage_types::{StreamItemId, StreamName, TimestampMillis};

use crate::{StreamDataType, StreamRowValue, decode_stream_item_row};

#[test]
fn given_opaque_tags_when_user_rows_round_trip_then_payloads_are_never_interpreted_as_history() {
    for tag in [
        StreamDataType::Binary,
        StreamDataType::Json,
        StreamDataType::Text,
        StreamDataType::DynamoDbJson,
        StreamDataType::DeleteMarker,
        StreamDataType::StreamPointer,
    ] {
        let name = StreamName::new(b"opaque");
        let payload = b"arbitrary bytes, not a history record".to_vec();
        let created_at = TimestampMillis::from_timestamp(1_800_000_000_000);
        let row = StreamRowValue::new(name.clone(), payload.clone(), tag, created_at);
        let id = StreamItemId::random();
        let item = decode_stream_item_row(id, &row.to_bytes().unwrap()).unwrap();
        assert_eq!(item.id, id);
        assert_eq!(item.stream_name, Some(name));
        assert_eq!(item.data, payload);
        assert_eq!(item.data_type, tag);
        assert_eq!(item.created_at, created_at);
    }
}
