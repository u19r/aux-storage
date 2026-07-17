use std::{
    hint::black_box,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};

use alloc_counter::AllocationGuard;

use crate::{
    DeleteMessageRequest, IntoValidatedQueueRequest, QueueAction, QueueRequest,
    QueueRequestValidation, QueueResult, ReceiveMessageRequest, SendMessageRequest,
    ValidatedQueueRequest, decode_json_request,
};

const ITERATIONS: usize = 1_000;
const SEND_JSON: &[u8] =
    br#"{"QueueUrl":"https://queue.example/jobs","MessageBody":"hello","DelaySeconds":1}"#;

fn legacy_json_send() -> SendMessageRequest {
    let value = serde_json::from_slice(SEND_JSON).expect("JSON value");
    SendMessageRequest::from_json(value).expect("legacy send request")
}

fn typed_json_send() -> SendMessageRequest {
    let QueueRequest::SendMessage(request) =
        decode_json_request(QueueAction::SendMessage, SEND_JSON).expect("typed send request")
    else {
        panic!("expected send request");
    };
    request.into_inner()
}

fn measure(
    label: &'static str,
    decode: impl Fn() -> SendMessageRequest,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "queue_wire_decode_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    let mut bytes = 0usize;
    for _ in 0..ITERATIONS {
        bytes = bytes.saturating_add(decode().message_body.len());
    }
    black_box(bytes);
    guard.finish()
}

#[test]
fn typed_json_wire_decode_reduces_small_request_allocations() {
    let legacy_json = measure("queue_json_value_then_request", legacy_json_send);
    let typed_json = measure("queue_json_direct_typed", typed_json_send);

    alloc_counter::emit_report(&legacy_json);
    alloc_counter::emit_report(&typed_json);
    assert!(typed_json.allocation_count < legacy_json.allocation_count);
    assert!(typed_json.allocated_bytes < legacy_json.allocated_bytes);
}

#[test]
fn typed_json_decode_rejects_duplicate_fields() {
    let error = decode_json_request(
        QueueAction::SendMessage,
        br#"{"QueueUrl":"one","QueueUrl":"two","MessageBody":"hello"}"#,
    )
    .expect_err("duplicate field must fail");

    assert!(error.message.contains("duplicate field `QueueUrl`"));
}

#[test]
fn typed_json_decode_preserves_unknown_field_error() {
    let error = decode_json_request(
        QueueAction::SendMessage,
        br#"{"QueueUrl":"one","MessageBody":"hello","Extra":true}"#,
    )
    .expect_err("unknown field must fail");

    assert_eq!(error.message, "Unknown field: Extra");
}

#[test]
fn typed_json_decode_preserves_validation_error_messages() {
    for body in [
        br#"{"QueueUrl":"one","MessageBody":"hello","DelaySeconds":"bad"}"#.as_slice(),
        br#"{"QueueUrl":"one","MessageBody":"hello","DelaySeconds":901}"#.as_slice(),
        br#"{"QueueUrl":"one","MessageBody":""}"#.as_slice(),
    ] {
        let legacy_value = serde_json::from_slice(body).expect("legacy JSON value");
        let legacy = SendMessageRequest::from_json(legacy_value)
            .expect_err("legacy invalid request")
            .message;
        let typed = decode_json_request(QueueAction::SendMessage, body)
            .expect_err("typed invalid request")
            .message;
        assert_eq!(typed, legacy);
    }
}

struct CountingRequest<'a>(&'a AtomicUsize);

impl QueueRequestValidation for CountingRequest<'_> {
    fn validate_request(&self) -> QueueResult<()> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[test]
fn validated_request_conversion_skips_revalidation_but_raw_conversion_validates() {
    let validation_count = AtomicUsize::new(0);
    let validated =
        ValidatedQueueRequest::new(CountingRequest(&validation_count)).expect("initial validation");
    let _ = validated.into_validated().expect("validated conversion");
    assert_eq!(validation_count.load(Ordering::Relaxed), 1);

    let _ = CountingRequest(&validation_count)
        .into_validated()
        .expect("raw conversion");
    assert_eq!(validation_count.load(Ordering::Relaxed), 2);
}

fn common_request_validation_cpu(passes: usize) -> Duration {
    let send = SendMessageRequest {
        queue_url: "https://queue.example/jobs".to_string(),
        message_body: "hello".to_string(),
        delay_seconds: Some(1),
        message_attributes: None,
    };
    let receive = ReceiveMessageRequest {
        queue_url: "https://queue.example/jobs".to_string(),
        max_number_of_messages: Some(1),
        visibility_timeout: Some(30),
        wait_time_seconds: Some(0),
        attribute_names: None,
        message_attribute_names: None,
    };
    let delete = DeleteMessageRequest {
        queue_url: "https://queue.example/jobs".to_string(),
        receipt_handle: "receipt".into(),
    };
    let started = Instant::now();
    for _ in 0..100_000 {
        for _ in 0..passes {
            send.validate_request().expect("send");
            black_box(());
            receive.validate_request().expect("receive");
            black_box(());
            delete.validate_request().expect("delete");
            black_box(());
        }
    }
    started.elapsed()
}

#[test]
fn single_owner_validation_halves_common_request_validation_work() {
    let single_owner = common_request_validation_cpu(1);
    let duplicate = common_request_validation_cpu(2);

    eprintln!("common SQS validation: single_owner={single_owner:?} duplicate={duplicate:?}");
    assert!(duplicate > single_owner);
}
