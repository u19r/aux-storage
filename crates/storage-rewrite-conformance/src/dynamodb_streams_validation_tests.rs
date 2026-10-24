//! DynamoDB Streams request rejections from the authoritative AWS conformance
//! suite.
//!
//! The fixture snapshot is from ../dynamo-conformance at
//! 2158288d7883de4be6c17da00226d8398faa723a. These requests fail before
//! provider access.

use storage_observability_test_support::observation::observe;
use storage_types::{
    DescribeStreamRequest, GetRecordsRequest, GetShardIteratorRequest, ListStreamsRequest,
};

use super::test_support::ConformanceFixtures;

static FIXTURES: ConformanceFixtures = ConformanceFixtures::new();

// Validation and negative rules: a missing stream ARN is rejected before a
// provider call.
#[tokio::test]
async fn given_missing_stream_arn_when_describe_stream_is_parsed_then_aws_validation_shape_is_returned()
 {
    let case = FIXTURES.case(
        include_str!("../fixtures/dynamodb_streams_validation_cases.jsonl"),
        "400/describe_stream_missing_stream_arn_validation.json",
    );
    let observed =
        observe(async { DescribeStreamRequest::try_from(case["request"].clone()) }).await;
    observed.assert_no_warning_or_error();
    observed.assert_database_calls_exact(&[]);
    let message = observed.result.expect_err("missing StreamArn must fail");
    assert!(message.contains("StreamArn") || message.contains("stream_arn"));
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::dynamodb_protocol_error("ValidationException", message, 400)
            .into();
    assert_eq!(
        status.as_u16(),
        case["expected"]["status"].as_u64().unwrap() as u16
    );
    assert!(
        !body.message.is_empty(),
        "AWS fixture omits the exact message"
    );
    assert_eq!(case["ignores"], serde_json::json!(["message"]));
    let mut actual = serde_json::to_value(body).expect("serialize validation error");
    let mut expected = case["expected"]["body"].clone();
    actual.as_object_mut().unwrap().remove("message");
    expected.as_object_mut().unwrap().remove("message");
    assert_eq!(actual, expected);
}

// Validation and negative rules: GetRecords rejects a limit above the service
// maximum.
#[tokio::test]
async fn given_limit_above_one_thousand_when_get_records_is_parsed_then_aws_validation_shape_is_returned()
 {
    let case = FIXTURES.case(
        include_str!("../fixtures/dynamodb_streams_validation_cases.jsonl"),
        "400/get_records_limit_too_large_validation.json",
    );
    let observed = observe(async { GetRecordsRequest::try_from(case["request"].clone()) }).await;
    observed.assert_no_warning_or_error();
    observed.assert_database_calls_exact(&[]);
    let message = observed.result.expect_err("GetRecords limit must fail");
    assert_eq!(
        message,
        storage_types::DYNAMODB_STREAM_RECORDS_LIMIT_MESSAGE
    );
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::dynamodb_protocol_error("ValidationException", message, 400)
            .into();
    assert_eq!(
        status.as_u16(),
        case["expected"]["status"].as_u64().unwrap() as u16
    );
    assert!(
        !body.message.is_empty(),
        "AWS fixture omits the exact message"
    );
    assert_eq!(case["ignores"], serde_json::json!(["message"]));
    let mut actual = serde_json::to_value(body).expect("serialize validation error");
    let mut expected = case["expected"]["body"].clone();
    actual.as_object_mut().unwrap().remove("message");
    expected.as_object_mut().unwrap().remove("message");
    assert_eq!(actual, expected);
}

// Validation and negative rules: an unknown iterator kind is rejected before a
// provider call.
#[tokio::test]
async fn given_unknown_iterator_kind_when_get_shard_iterator_is_parsed_then_aws_validation_shape_is_returned()
 {
    let case = FIXTURES.case(
        include_str!("../fixtures/dynamodb_streams_validation_cases.jsonl"),
        "400/get_shard_iterator_invalid_type_validation.json",
    );
    let observed =
        observe(async { GetShardIteratorRequest::try_from(case["request"].clone()) }).await;
    observed.assert_no_warning_or_error();
    observed.assert_database_calls_exact(&[]);
    let message = observed
        .result
        .expect_err("unknown iterator kind must fail");
    assert!(message.contains("FROM_NOWHERE"));
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::dynamodb_protocol_error("ValidationException", message, 400)
            .into();
    assert_eq!(
        status.as_u16(),
        case["expected"]["status"].as_u64().unwrap() as u16
    );
    assert!(
        !body.message.is_empty(),
        "AWS fixture omits the exact message"
    );
    assert_eq!(case["ignores"], serde_json::json!(["message"]));
    let mut actual = serde_json::to_value(body).expect("serialize validation error");
    let mut expected = case["expected"]["body"].clone();
    actual.as_object_mut().unwrap().remove("message");
    expected.as_object_mut().unwrap().remove("message");
    assert_eq!(actual, expected);
}

// Validation and negative rules: ListStreams rejects zero before a provider
// call.
#[tokio::test]
async fn given_zero_limit_when_list_streams_is_parsed_then_aws_validation_shape_is_returned() {
    let case = FIXTURES.case(
        include_str!("../fixtures/dynamodb_streams_validation_cases.jsonl"),
        "400/list_streams_limit_zero_validation.json",
    );
    let observed = observe(async { ListStreamsRequest::try_from(case["request"].clone()) }).await;
    observed.assert_no_warning_or_error();
    observed.assert_database_calls_exact(&[]);
    let message = observed
        .result
        .expect_err("ListStreams zero limit must fail");
    assert_eq!(message, "Limit must be between 1 and 100");
    let (status, axum::Json(body)): (_, axum::Json<http_error::ErrorResponse>) =
        http_error::HttpApiError::dynamodb_protocol_error("ValidationException", message, 400)
            .into();
    assert_eq!(
        status.as_u16(),
        case["expected"]["status"].as_u64().unwrap() as u16
    );
    assert!(
        !body.message.is_empty(),
        "AWS fixture omits the exact message"
    );
    assert_eq!(case["ignores"], serde_json::json!(["message"]));
    let mut actual = serde_json::to_value(body).expect("serialize validation error");
    let mut expected = case["expected"]["body"].clone();
    actual.as_object_mut().unwrap().remove("message");
    expected.as_object_mut().unwrap().remove("message");
    assert_eq!(actual, expected);
}
