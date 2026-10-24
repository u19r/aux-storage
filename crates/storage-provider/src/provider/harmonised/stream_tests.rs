use super::models::{StreamAppend, StreamReadInput, StreamWriteInput};

fn stream_name() -> storage_types::StreamName {
    storage_types::StreamName::new(b"orders")
}

fn append(ordinal: usize) -> StreamAppend {
    StreamAppend::try_new(ordinal, vec![1, 2, 3], Some("pk".to_owned())).expect("append")
}

#[test]
fn given_empty_appends_when_construct_then_error() {
    assert!(
        StreamWriteInput::try_new(stream_name(), Vec::new()).is_err(),
        "empty appends reject"
    );
}

#[test]
fn given_empty_data_when_construct_then_error() {
    assert!(
        StreamAppend::try_new(0, Vec::new(), None).is_err(),
        "empty data rejects"
    );
}

#[test]
fn given_empty_partition_key_when_construct_then_error() {
    assert!(
        StreamAppend::try_new(0, vec![1], Some(String::new())).is_err(),
        "empty partition key rejects"
    );
}

#[test]
fn given_data_when_construct_then_ok() {
    let input = StreamWriteInput::try_new(stream_name(), vec![append(2)]).expect("data constructs");
    assert_eq!(input.appends.len(), 1);
    assert_eq!(input.appends.as_slice()[0].ordinal, 2);
}

#[test]
fn given_zero_limit_when_construct_then_error() {
    assert!(
        StreamReadInput::try_new(stream_name(), None, 0, true).is_err(),
        "zero limit rejects"
    );
}

#[test]
fn given_over_max_limit_when_construct_then_error() {
    assert!(
        StreamReadInput::try_new(
            stream_name(),
            None,
            storage_types::DYNAMODB_STREAM_RECORDS_LIMIT_MAX + 1,
            true
        )
        .is_err(),
        "over-max limit rejects"
    );
}

#[test]
fn given_valid_limit_when_construct_then_ok() {
    let input = StreamReadInput::try_new(stream_name(), None, 10, true).expect("limit constructs");
    assert_eq!(input.limit.get(), 10);
}
