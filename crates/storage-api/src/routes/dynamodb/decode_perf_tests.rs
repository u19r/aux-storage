use std::{hint::black_box, process::Command};

use alloc_counter::AllocationGuard;
use axum::body::Bytes;
use storage_types::{DynamoRequestValidate, GetItemRequest, PutItemRequest};

use crate::routes::dynamodb::{parse_json_request_format, parse_try_into_request};

const ITERATIONS: usize = 1_000;
const GET_BODY: &[u8] = br#"{"TableName":"table","Key":{"pk":{"S":"value"}}}"#;
const PUT_BODY: &[u8] =
    br#"{"TableName":"table","Item":{"pk":{"S":"value"},"data":{"S":"payload"}}}"#;

fn direct<T>(body: &[u8]) -> T
where T: serde::de::DeserializeOwned + DynamoRequestValidate {
    let request: T = serde_json::from_slice(body).expect("direct request decode");
    request
        .validate_for_dynamodb()
        .expect("direct request validation");
    request
}

fn measure<T>(
    label: &'static str,
    decode: impl Fn() -> T,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "dynamodb_wire_decode_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..ITERATIONS {
        black_box(decode());
    }
    guard.finish()
}

#[test]
fn given_small_requests_when_decoding_directly_then_allocations_are_reduced() {
    const ISOLATED_ENV: &str = "AUX_STORAGE_API_DYNAMODB_DECODE_ALLOCATION_ISOLATED";
    if std::env::var_os(ISOLATED_ENV).is_none() {
        let status = Command::new(
            std::env::current_exe()
                .expect("DynamoDB decode allocation test executable should be available"),
        )
        .arg("--exact")
        .arg(
            "routes::dynamodb::decode_perf_tests::given_small_requests_when_decoding_directly_then_allocations_are_reduced",
        )
        .arg("--nocapture")
        .env(ISOLATED_ENV, "1")
        .status()
        .expect("isolated DynamoDB decode allocation test child should start");
        assert!(
            status.success(),
            "isolated DynamoDB decode allocation test failed"
        );
        return;
    }

    let legacy_get = measure("get_value_then_typed", || {
        parse_try_into_request::<GetItemRequest>(&Bytes::from_static(GET_BODY))
            .expect("legacy GetItem decode")
    });
    let direct_get = measure("get_direct_typed", || direct::<GetItemRequest>(GET_BODY));
    let legacy_put = measure("put_value_then_typed", || {
        parse_try_into_request::<PutItemRequest>(&Bytes::from_static(PUT_BODY))
            .expect("legacy PutItem decode")
    });
    let direct_put = measure("put_direct_typed", || direct::<PutItemRequest>(PUT_BODY));

    alloc_counter::emit_report(&legacy_get);
    alloc_counter::emit_report(&direct_get);
    alloc_counter::emit_report(&legacy_put);
    alloc_counter::emit_report(&direct_put);
    assert!(direct_get.allocation_count < legacy_get.allocation_count);
    assert!(direct_get.allocated_bytes < legacy_get.allocated_bytes);
    assert!(direct_put.allocation_count < legacy_put.allocation_count);
    assert!(direct_put.allocated_bytes < legacy_put.allocated_bytes);
}

#[test]
fn given_duplicate_fields_when_decoding_directly_then_request_is_rejected() {
    let error = parse_json_request_format::<GetItemRequest>(&Bytes::from_static(
        br#"{"TableName":"one","TableName":"two","Key":{"pk":{"S":"value"}}}"#,
    ))
    .expect_err("duplicate field must fail");

    assert!(error.body.0.message.contains("duplicate field `TableName`"));
}
