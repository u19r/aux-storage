//! AWS emulator behavioural suite ports.
//!
//! SQS request validation, batches, sizes, attributes, handles, concurrency
//! and scale, driven through the JSON wire decode, `QueueManager::dispatch`
//! and the JSON encode, as `queue_api_compat_tests.rs` does. Expected values
//! are AWS's unless a test names a documented difference.

use std::{cell::RefCell, collections::BTreeSet, time::Duration};

use base64::Engine;
use futures::future::join_all;
use http_error::HttpApiError;
use queue::{QueueEndpoint, QueueManager};
use queue_provider::wire::{
    QueueProtocol, WireRequestParts, decode_wire_request, encode_error, encode_success,
};
use serde_json::{Value, json};

use super::test_support::TestStore;

const BASE_URL: &str = "http://aux.test/queue";
const BOUND: Duration = Duration::from_secs(10);
const INVALID_ENTRY_ID: &str = "A batch entry id can only contain alphanumeric characters, \
                                hyphens and underscores. It can be at most 80 letters long.";
const INVALID_HANDLE: &str = "The input receipt handle is invalid.";
const MISSING_QUEUE: &str = "The specified queue does not exist.";
const EXISTS: &str = "A queue already exists with the same name and a different value for \
                      attribute VisibilityTimeout";

/// The manager stamps sends with the system clock, so the store uses it too.
fn manager(store: &TestStore) -> QueueManager {
    store.use_system_time();
    QueueManager::new(store.queue_operations.clone()).with_endpoint(QueueEndpoint {
        public_base_url: BASE_URL.to_owned(),
    })
}

async fn sqs(manager: &QueueManager, action: &str, body: Value) -> (u16, Value) {
    let target = format!("AmazonSQS.{action}");
    let body = body.to_string();
    let response = match decode_wire_request(WireRequestParts {
        content_type: Some("application/x-amz-json-1.0"),
        target: Some(&target),
        body: body.as_bytes(),
        queue_url: None,
    }) {
        Ok(wire) => match manager.dispatch(wire.request, || {}).await {
            Ok(response) => encode_success(QueueProtocol::Json, "test", &response),
            Err(error) => encode_error(QueueProtocol::Json, "test", &HttpApiError::from(error)),
        },
        Err(rejection) => encode_error(rejection.protocol, "test", &rejection.error),
    };
    let value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    (response.status, value)
}

async fn ok(manager: &QueueManager, action: &str, body: Value) -> Value {
    let (status, value) = sqs(manager, action, body).await;
    assert_eq!(status, 200, "{action}: {value}");
    value
}

/// A 400 whose JSON `__type` ends in `#<kind>` and whose message is exact.
fn assert_error((status, body): &(u16, Value), kind: &str, message: &str) {
    // Probe 4: a malformed receipt handle is a 404 on the wire.
    let expected = if kind == "ReceiptHandleIsInvalid" {
        404
    } else {
        400
    };
    assert_eq!(*status, expected, "{body}");
    let found = body["__type"].as_str().unwrap_or_default();
    assert!(found.ends_with(&format!("#{kind}")), "{kind} vs {body}");
    assert_eq!(body["message"], message, "{body}");
}

fn unique(label: &str) -> String {
    format!("{label}{}", uuid::Uuid::new_v4().simple())
}

async fn create(manager: &QueueManager, label: &str, extra: Value) -> String {
    let mut body = json!({ "QueueName": unique(label) });
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().cloned().unwrap_or_default());
    ok(manager, "CreateQueue", body).await["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn send(manager: &QueueManager, url: &str, body: &str) {
    ok(
        manager,
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": body}),
    )
    .await;
}

/// The `Messages` of one receive with every attribute named.
async fn receive(manager: &QueueManager, url: &str, max: u32, visibility: u32) -> Vec<Value> {
    let request = json!({"QueueUrl": url, "MaxNumberOfMessages": max,
        "VisibilityTimeout": visibility, "WaitTimeSeconds": 0,
        "AttributeNames": ["All"], "MessageAttributeNames": ["All"]});
    ok(manager, "ReceiveMessage", request).await["Messages"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

fn handle(message: &Value) -> &str {
    message["ReceiptHandle"].as_str().unwrap()
}

fn body(message: &Value) -> &str {
    message["Body"].as_str().unwrap()
}

fn bodies(messages: &[Value]) -> Vec<&str> {
    let mut bodies = messages.iter().map(body).collect::<Vec<_>>();
    bodies.sort_unstable();
    bodies
}

fn ids(entries: &Value) -> Vec<String> {
    let mut ids: Vec<String> = entries
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|e| e["Id"].as_str().unwrap().to_owned())
                .collect()
        })
        .unwrap_or_default();
    ids.sort();
    ids
}

/// `(visible, not visible, delayed)`.
async fn counters(manager: &QueueManager, url: &str) -> (u64, u64, u64) {
    let read = ok(
        manager,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": ["All"]}),
    )
    .await;
    let number = |name: &str| {
        read["Attributes"][name]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
    };
    (
        number("ApproximateNumberOfMessages"),
        number("ApproximateNumberOfMessagesNotVisible"),
        number("ApproximateNumberOfMessagesDelayed"),
    )
}

async fn attribute_of(manager: &QueueManager, url: &str, name: &str) -> String {
    let read = ok(
        manager,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": [name]}),
    )
    .await;
    read["Attributes"][name].as_str().unwrap().to_owned()
}

fn text_attribute(name: &str, value: &str) -> Value {
    json!({name: {"DataType": "String", "StringValue": value}})
}

/// AWS behavioural: check_batch_entry_id_character_and_length_rules_rejected.py
#[tokio::test]
async fn given_bad_batch_entry_ids_when_any_batch_is_sent_then_the_whole_request_is_refused() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "entry_ids", json!({})).await;
    let long = "x".repeat(81);
    for bad in ["a.b", "a b", "", long.as_str()] {
        let requests = [
            (
                "SendMessageBatch",
                json!({"Id": "ok", "MessageBody": "x"}),
                json!({"Id": bad, "MessageBody": "y"}),
            ),
            (
                "DeleteMessageBatch",
                json!({"Id": "ok", "ReceiptHandle": "AQEB-x"}),
                json!({"Id": bad, "ReceiptHandle": "AQEB-y"}),
            ),
            (
                "ChangeMessageVisibilityBatch",
                json!({"Id": "ok", "ReceiptHandle": "AQEB-x", "VisibilityTimeout": 5}),
                json!({"Id": bad, "ReceiptHandle": "AQEB-y", "VisibilityTimeout": 5}),
            ),
        ];
        for (action, good, wrong) in requests {
            let response = sqs(
                &manager,
                action,
                json!({"QueueUrl": url, "Entries": [good, wrong]}),
            )
            .await;
            assert_error(&response, "InvalidBatchEntryId", INVALID_ENTRY_ID);
        }
    }
    assert_eq!(
        counters(&manager, &url).await,
        (0, 0, 0),
        "no partial effect"
    );
    let accepted =
        json!({"QueueUrl": url, "Entries": [{"Id": "x".repeat(80), "MessageBody": "fits"}]});
    let sent = ok(&manager, "SendMessageBatch", accepted).await;
    assert_eq!(sent["Successful"].as_array().unwrap().len(), 1);
    store.finish().await;
}

/// AWS behavioural: check_bad_receipt_handle.py,
/// check_delete_message_invalid_receipt_handle_rejected.py
#[tokio::test]
async fn given_bogus_receipt_handle_when_deleted_then_refused_and_the_real_handle_still_deletes() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "bogus_handle", json!({})).await;
    for bogus in ["sim2real-bogus", "totally-bogus-receipt-handle"] {
        send(&manager, &url, "stays").await;
        let held = receive(&manager, &url, 1, 600).await;
        let refused = sqs(
            &manager,
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": bogus}),
        )
        .await;
        assert_error(&refused, "ReceiptHandleIsInvalid", INVALID_HANDLE);
        assert_eq!(
            counters(&manager, &url).await,
            (0, 1, 0),
            "the message stays in flight"
        );
        ok(
            &manager,
            "DeleteMessage",
            json!({"QueueUrl": url, "ReceiptHandle": handle(&held[0])}),
        )
        .await;
        assert_eq!(counters(&manager, &url).await, (0, 0, 0));
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_sqsbatch_delete_duplicate_receipt_handle_deleted_once.py
#[tokio::test]
async fn given_duplicate_and_bogus_handles_when_batch_deleted_then_only_the_bogus_entry_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "dup_handle", json!({})).await;
    send(&manager, &url, "dup-target").await;
    send(&manager, &url, "bystander-never-named").await;
    let held = receive(&manager, &url, 10, 600).await;
    let target = held.iter().find(|m| body(m) == "dup-target").unwrap();
    let bystander = held.iter().find(|m| body(m) != "dup-target").unwrap();
    let entries = json!([
        {"Id": "dup-a", "ReceiptHandle": handle(target)},
        {"Id": "dup-b", "ReceiptHandle": handle(target)},
        {"Id": "bogus", "ReceiptHandle": "not-a-real-handle"},
    ]);
    let result = ok(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    assert_eq!(ids(&result["Successful"]), ["dup-a", "dup-b"]);
    assert_eq!(ids(&result["Failed"]), ["bogus"]);
    let failed = &result["Failed"][0];
    assert_eq!(failed["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(failed["SenderFault"], true);
    assert_eq!(failed["Message"], INVALID_HANDLE);

    assert_eq!(counters(&manager, &url).await, (0, 1, 0));
    let release =
        json!({"QueueUrl": url, "ReceiptHandle": handle(bystander), "VisibilityTimeout": 0});
    ok(&manager, "ChangeMessageVisibility", release).await;
    assert_eq!(
        bodies(&receive(&manager, &url, 10, 600).await),
        ["bystander-never-named"]
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_sqsbatch_delete_full_width_one_bogus_handle_rejected.py
#[tokio::test]
async fn given_ten_entry_delete_batch_with_one_bogus_handle_when_sent_then_only_that_entry_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "full_width", json!({})).await;
    for index in 0..10 {
        send(&manager, &url, &format!("round2-{index}")).await;
    }
    let held = receive(&manager, &url, 10, 600).await;
    assert_eq!(held.len(), 10);
    let entries = (0..10)
        .map(|index| {
            let message = held
                .iter()
                .find(|m| body(m) == format!("round2-{index}"))
                .unwrap();
            let receipt = if index == 7 {
                "bogus-handle"
            } else {
                handle(message)
            };
            json!({"Id": format!("d{index}"), "ReceiptHandle": receipt})
        })
        .collect::<Vec<_>>();
    let result = ok(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    let mut expected = (0..10)
        .filter(|i| *i != 7)
        .map(|i| format!("d{i}"))
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(ids(&result["Successful"]), expected);
    assert_eq!(ids(&result["Failed"]), ["d7"]);
    assert_eq!(result["Failed"][0]["Code"], "ReceiptHandleIsInvalid");
    assert_eq!(result["Failed"][0]["SenderFault"], true);
    assert_eq!(result["Failed"][0]["Message"], INVALID_HANDLE);

    let seven = held.iter().find(|m| body(m) == "round2-7").unwrap();
    let release = json!({"QueueUrl": url, "ReceiptHandle": handle(seven), "VisibilityTimeout": 0});
    ok(&manager, "ChangeMessageVisibility", release).await;
    assert_eq!(
        bodies(&receive(&manager, &url, 10, 600).await),
        ["round2-7"]
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_delete_and_visibility_batches_receipt_handle_from_another_queue_fails_alone.
/// py, check_delete_message_batch_partial_failure.py
#[tokio::test]
async fn given_handle_of_another_queue_when_batch_deleted_or_changed_then_only_that_entry_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let queue_a = create(&manager, "cross_a", json!({})).await;
    let queue_b = create(&manager, "cross_b", json!({})).await;
    send(&manager, &queue_a, "foreign").await;
    let foreign = receive(&manager, &queue_a, 1, 600).await;
    let foreign_handle = handle(&foreign[0]).to_owned();
    let message = format!("The receipt handle \"{foreign_handle}\" is not valid for this queue.");
    for action in ["DeleteMessageBatch", "ChangeMessageVisibilityBatch"] {
        send(&manager, &queue_b, "local").await;
        let local = receive(&manager, &queue_b, 1, 600).await;
        let mut entries = json!([
            {"Id": "local", "ReceiptHandle": handle(&local[0])},
            {"Id": "foreign", "ReceiptHandle": foreign_handle},
        ]);
        if action == "ChangeMessageVisibilityBatch" {
            for entry in entries.as_array_mut().unwrap() {
                entry["VisibilityTimeout"] = json!(60);
            }
        }
        let result = ok(
            &manager,
            action,
            json!({"QueueUrl": queue_b, "Entries": entries}),
        )
        .await;
        assert_eq!(ids(&result["Successful"]), ["local"], "{action}");
        assert_eq!(ids(&result["Failed"]), ["foreign"], "{action}");
        let failed = &result["Failed"][0];
        assert_eq!(failed["Code"], "ReceiptHandleIsInvalid");
        assert_eq!(failed["SenderFault"], true);
        assert_eq!(failed["Message"], message);
        assert_eq!(
            counters(&manager, &queue_a).await,
            (0, 1, 0),
            "still in flight"
        );
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_delete_and_visibility_batches_duplicate_ids_rejected.py,
/// check_sqsbatch_send_request_level_error_preempts_entry_failures_rejected.py
///
/// The repaired-batch `EmptyValue` split is not ported: aux rejects a batch
/// with an invalid body as a whole (writing/sqs_parity.md).
#[tokio::test]
async fn given_duplicate_entry_ids_when_batches_are_sent_then_each_is_refused_whole() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "dup_ids", json!({})).await;
    send(&manager, &url, "first").await;
    send(&manager, &url, "second").await;
    let held = receive(&manager, &url, 2, 600).await;
    for (action, extra) in [
        ("DeleteMessageBatch", json!({})),
        (
            "ChangeMessageVisibilityBatch",
            json!({"VisibilityTimeout": 0}),
        ),
    ] {
        let entries = held
            .iter()
            .map(|message| {
                let mut entry = json!({"Id": "dup", "ReceiptHandle": handle(message)});
                entry
                    .as_object_mut()
                    .unwrap()
                    .extend(extra.as_object().cloned().unwrap());
                entry
            })
            .collect::<Vec<_>>();
        let refused = sqs(
            &manager,
            action,
            json!({"QueueUrl": url, "Entries": entries}),
        )
        .await;
        assert_error(&refused, "BatchEntryIdsNotDistinct", "Id dup repeated.");
        assert_eq!(
            counters(&manager, &url).await,
            (0, 2, 0),
            "{action} touched nothing"
        );
    }
    let distinct = held
        .iter()
        .enumerate()
        .map(
            |(index, message)| json!({"Id": format!("d{index}"), "ReceiptHandle": handle(message)}),
        )
        .collect::<Vec<_>>();
    let result = ok(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": distinct}),
    )
    .await;
    assert_eq!(ids(&result["Successful"]), ["d0", "d1"]);

    let entries = [("e0", "a"), ("e2", "b"), ("e4", ""), ("e2", "c")]
        .map(|(id, text)| json!({"Id": id, "MessageBody": text}));
    let refused = sqs(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    assert_error(&refused, "BatchEntryIdsNotDistinct", "Id e2 repeated.");
    assert_eq!(counters(&manager, &url).await, (0, 0, 0), "nothing landed");
    store.finish().await;
}

/// AWS behavioural:
/// check_sqsbatch_send_full_width_one_invalid_entry_rejected.py
#[tokio::test]
async fn given_ten_entry_send_batch_with_one_empty_body_when_sent_then_only_that_entry_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "empty_body", json!({})).await;
    let entries = (0..10)
        .map(|index| {
            let id = if index == 4 {
                "Entry_04-eCHO".to_owned()
            } else {
                format!("Entry_{index:02}")
            };
            let text = if index == 4 {
                String::new()
            } else {
                format!("good-{index}")
            };
            json!({"Id": id, "MessageBody": text})
        })
        .collect::<Vec<_>>();
    let (status, response) = sqs(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    assert_eq!(status, 200, "{response}");
    let failed = response["Failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "{response}");
    assert_eq!(failed[0]["Id"], "Entry_04-eCHO");
    assert_eq!(failed[0]["Code"], "EmptyValue");
    assert_eq!(failed[0]["SenderFault"], true);
    assert_eq!(
        failed[0]["Message"],
        "No Value Found for SendMessageBatchRequestEntry.5.MessageBody."
    );
    assert_eq!(response["Successful"].as_array().unwrap().len(), 9);
    assert_eq!(
        counters(&manager, &url).await,
        (9, 0, 0),
        "the nine valid entries are enqueued"
    );

    let valid =
        [("a", "hello"), ("b", "world")].map(|(id, text)| json!({"Id": id, "MessageBody": text}));
    let sent = ok(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": valid}),
    )
    .await;
    let first = sent["Successful"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["Id"] == "a")
        .unwrap();
    assert_eq!(
        first["MD5OfMessageBody"],
        "5d41402abc4b2a76b9719d911017c592"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_batch_entry_over_queue_max_message_size_fails_alone.py
#[tokio::test]
async fn given_queue_limit_1024_when_batch_has_oversize_entry_then_it_fails_alone() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "batch_size",
        json!({"Attributes": {"MaximumMessageSize": "1024"}}),
    )
    .await;
    let entries = [("fits", 1024), ("over", 1025)]
        .map(|(id, size)| json!({"Id": id, "MessageBody": "a".repeat(size)}));
    let result = ok(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    assert_eq!(ids(&result["Successful"]), ["fits"]);
    assert_eq!(ids(&result["Failed"]), ["over"]);
    let failed = &result["Failed"][0];
    assert_eq!(failed["Code"], "InvalidParameterValue");
    assert_eq!(failed["SenderFault"], true);
    assert_eq!(
        failed["Message"],
        "One or more parameters cannot be validated. Reason: Message must be shorter than 1024 \
         bytes."
    );
    let pair = [("x", 1000), ("y", 1000)]
        .map(|(id, size)| json!({"Id": id, "MessageBody": "b".repeat(size)}));
    let accepted = ok(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": pair}),
    )
    .await;
    assert_eq!(
        ids(&accepted["Successful"]),
        ["x", "y"],
        "the total is not limited"
    );
    assert_eq!(
        attribute_of(&manager, &url, "MaximumMessageSize").await,
        "1024"
    );
    store.finish().await;
}

/// AWS behavioural: check_op_send_message_oversize_rejected.py
#[tokio::test]
async fn given_queue_limit_1024_when_2048_byte_body_is_sent_then_refused_and_not_enqueued() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "oversize",
        json!({"Attributes": {"MaximumMessageSize": "1024"}}),
    )
    .await;
    send(&manager, &url, &"c".repeat(100)).await;
    let refused = sqs(
        &manager,
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "d".repeat(2048)}),
    )
    .await;
    assert_error(
        &refused,
        "InvalidParameterValueException",
        "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes.",
    );
    let received = receive(&manager, &url, 10, 30).await;
    assert_eq!(received.len(), 1, "only the control message");
    assert_eq!(body(&received[0]).len(), 100);
    store.finish().await;
}

/// AWS behavioural:
/// check_maximum_message_size_is_a_byte_boundary_not_a_character_count.py
#[tokio::test]
async fn given_queue_limit_1024_when_bodies_straddle_it_in_bytes_then_the_limit_counts_utf8_bytes()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "byte_limit",
        json!({"Attributes": {"MaximumMessageSize": "1024"}}),
    )
    .await;
    let message =
        "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes.";
    let two_byte = "\u{e9}";
    for fits in ["a".repeat(1024), two_byte.repeat(512)] {
        send(&manager, &url, &fits).await;
    }
    for over in ["a".repeat(1025), two_byte.repeat(513)] {
        let refused = sqs(
            &manager,
            "SendMessage",
            json!({"QueueUrl": url, "MessageBody": over}),
        )
        .await;
        assert_error(&refused, "InvalidParameterValueException", message);
    }
    assert_eq!(
        counters(&manager, &url).await,
        (2, 0, 0),
        "only the fitting bodies"
    );
    ok(
        &manager,
        "SetQueueAttributes",
        json!({"QueueUrl": url, "Attributes": {"MaximumMessageSize": "4096"}}),
    )
    .await;
    send(&manager, &url, &two_byte.repeat(513)).await;
    let all = receive(&manager, &url, 10, 30).await;
    assert!(
        all.iter().any(|m| body(m) == two_byte.repeat(513)),
        "returned byte-exact"
    );
    store.finish().await;
}

/// AWS behavioural: check_message_size_counts_attribute_name_type_and_value.py
#[tokio::test]
async fn given_queue_limit_1024_when_attributes_are_sent_then_name_type_value_count_but_trace_header_does_not()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "attr_size",
        json!({"Attributes": {"MaximumMessageSize": "1024"}}),
    )
    .await;
    // Name "n" (1) + DataType "String.ab" (9) + value (10) = 20 counted bytes.
    let attributes = json!({"n": {"DataType": "String.ab", "StringValue": "v".repeat(10)}});
    let request = |size: usize| json!({"QueueUrl": url, "MessageBody": "b".repeat(size), "MessageAttributes": attributes});
    ok(&manager, "SendMessage", request(1004)).await;
    let refused = sqs(&manager, "SendMessage", request(1005)).await;
    assert_error(
        &refused,
        "InvalidParameterValueException",
        "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes.",
    );
    let mut traced = request(1004);
    traced["MessageSystemAttributes"] = json!({"AWSTraceHeader": {"DataType": "String", "StringValue": "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1"}});
    ok(&manager, "SendMessage", traced).await;
    assert_eq!(
        counters(&manager, &url).await,
        (2, 0, 0),
        "only accepted messages are enqueued"
    );
    store.finish().await;
}

/// AWS behavioural: check_max_message_size.py
///
/// aux's route accepts bodies up to 5 MiB, so a 2 MiB body reaches SQS
/// validation instead of AWS's transport-level 413; the validation answer is
/// asserted. The 262144-byte leg is covered by queue_contract_tests.rs.
#[tokio::test]
async fn given_two_mebibyte_body_when_sent_then_refused_by_validation_and_not_delivered() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "huge_body", json!({})).await;
    let refused = sqs(
        &manager,
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "z".repeat(2 * 1024 * 1024)}),
    )
    .await;
    assert_error(
        &refused,
        "InvalidParameterValueException",
        "One or more parameters are invalid. Reason: Message must be shorter than 1048576 bytes.",
    );
    assert_eq!(counters(&manager, &url).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural: check_binary_message_attribute.py,
/// check_message_attribute_names_differing_only_in_case_are_distinct.py,
/// check_numeric_message_attribute_precision_preserved.py
#[tokio::test]
async fn given_binary_number_and_case_distinct_attributes_when_received_then_each_value_is_exact() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "attribute_values", json!({})).await;
    let bytes = (0..=255u8).collect::<Vec<_>>();
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    let mut attributes = json!({
        "blob": {"DataType": "Binary", "BinaryValue": encoded},
        "pi": {"DataType": "Number", "StringValue": "3.14159265358979323846"},
    });
    for (name, value) in [("Order", "upper"), ("order", "lower"), ("ORDER", "allcaps")] {
        attributes[name] = text_attribute(name, value)[name].clone();
    }
    let request =
        json!({"QueueUrl": url, "MessageBody": "attributes", "MessageAttributes": attributes});
    ok(&manager, "SendMessage", request).await;
    let received = receive(&manager, &url, 1, 30).await;
    let got = &received[0]["MessageAttributes"];
    assert_eq!(got["blob"]["DataType"], "Binary");
    assert_eq!(got["blob"]["BinaryValue"], encoded.as_str());
    assert_eq!(got["pi"]["DataType"], "Number");
    assert_eq!(got["pi"]["StringValue"], "3.14159265358979323846");
    for (name, value) in [("Order", "upper"), ("order", "lower"), ("ORDER", "allcaps")] {
        assert_eq!(got[name]["StringValue"], value, "{name}");
    }
    store.finish().await;
}

/// AWS behavioural: check_empty_vs_absent_message_attribute_preserved.py
///
/// writing/sqs_parity.md says aux accepts an empty StringValue; this fails
/// until empty values are rejected.
#[tokio::test]
async fn given_empty_string_attribute_when_sent_then_refused_and_an_absent_one_is_preserved() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "empty_attribute", json!({})).await;
    let rejected = json!({"QueueUrl": url, "MessageBody": "rejected-body", "MessageAttributes": text_attribute("Coupon", "")});
    let refused = sqs(&manager, "SendMessage", rejected).await;
    assert_error(
        &refused,
        "InvalidParameterValueException",
        "Message (user) attribute 'Coupon' must contain a non-empty value of type 'String'.",
    );
    send(&manager, &url, "without-coupon").await;
    let received = receive(&manager, &url, 10, 30).await;
    assert_eq!(bodies(&received), ["without-coupon"]);
    assert!(received[0]["MessageAttributes"].get("Coupon").is_none());
    store.finish().await;
}

/// AWS behavioural: check_op_delay_seconds_out_of_range_rejected.py
#[tokio::test]
async fn given_delay_seconds_at_and_past_the_limit_when_sent_then_900_is_kept_and_901_is_refused() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "delay_range", json!({})).await;
    let at_limit = json!({"QueueUrl": url, "MessageBody": "late", "DelaySeconds": 900});
    ok(&manager, "SendMessage", at_limit).await;
    let past = json!({"QueueUrl": url, "MessageBody": "refused", "DelaySeconds": 901});
    assert_error(
        &sqs(&manager, "SendMessage", past).await,
        "InvalidParameterValueException",
        "Value 901 for parameter DelaySeconds is invalid. Reason: DelaySeconds must be >= 0 and \
         <= 900.",
    );
    assert!(receive(&manager, &url, 10, 30).await.is_empty());
    assert_eq!(
        counters(&manager, &url).await,
        (0, 0, 1),
        "only the 900 s message exists"
    );
    store.finish().await;
}

/// AWS behavioural: check_queue_name_exists.py,
/// check_create_queue_name_conflict_different_attributes_rejected.py
#[tokio::test]
async fn given_existing_queue_when_created_with_other_timeout_then_refused_and_unchanged() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("conflict");
    let first = ok(
        &manager,
        "CreateQueue",
        json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "30"}}),
    )
    .await;
    let url = first["QueueUrl"].as_str().unwrap().to_owned();
    let other = json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "60"}});
    assert_error(
        &sqs(&manager, "CreateQueue", other).await,
        "QueueNameExists",
        EXISTS,
    );
    assert_eq!(
        attribute_of(&manager, &url, "VisibilityTimeout").await,
        "30"
    );
    let same = ok(
        &manager,
        "CreateQueue",
        json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "30"}}),
    )
    .await;
    assert_eq!(same["QueueUrl"], url.as_str());
    let resolved = ok(&manager, "GetQueueUrl", json!({"QueueName": name})).await;
    assert_eq!(resolved["QueueUrl"], url.as_str());
    store.finish().await;
}

/// AWS behavioural:
/// check_create_queue_after_set_queue_attributes_compares_current_values.py
#[tokio::test]
async fn given_timeout_changed_after_create_when_created_again_then_the_current_value_is_compared()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("current_values");
    let url = ok(&manager, "CreateQueue", json!({"QueueName": name})).await["QueueUrl"]
        .as_str()
        .unwrap()
        .to_owned();
    ok(
        &manager,
        "SetQueueAttributes",
        json!({"QueueUrl": url, "Attributes": {"VisibilityTimeout": "90"}}),
    )
    .await;
    for request in [
        json!({"QueueName": name}),
        json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "90"}}),
    ] {
        assert_eq!(
            ok(&manager, "CreateQueue", request).await["QueueUrl"],
            url.as_str()
        );
    }
    let stale = json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "45"}});
    assert_error(
        &sqs(&manager, "CreateQueue", stale).await,
        "QueueNameExists",
        EXISTS,
    );
    assert_eq!(
        attribute_of(&manager, &url, "VisibilityTimeout").await,
        "90"
    );
    store.finish().await;
}

/// AWS behavioural: check_cli_sqs_create_send_receive_delete.py,
/// check_queue_moves_a_batch_of_exact_messages_and_the_deleted_queue_refuses_batch_calls.py
#[tokio::test]
async fn given_batch_of_exact_messages_when_queue_is_deleted_then_every_call_on_its_url_is_refused()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "batch_exact", json!({})).await;
    let texts = [
        ("m0", "{\"source\":\"cli\"}".to_owned()),
        ("m1", "x".repeat(1000)),
        ("m2", "hello".to_owned()),
    ];
    let entries = texts
        .clone()
        .map(|(id, text)| json!({"Id": id, "MessageBody": text}));
    let sent = ok(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await;
    assert!(sent.get("Failed").is_none_or(|failed| failed == &json!([])));
    assert_eq!(ids(&sent["Successful"]), ["m0", "m1", "m2"]);
    let hello = sent["Successful"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["Id"] == "m2")
        .unwrap();
    assert_eq!(
        hello["MD5OfMessageBody"],
        "5d41402abc4b2a76b9719d911017c592"
    );

    let received = receive(&manager, &url, 10, 600).await;
    let mut expected = texts
        .iter()
        .map(|(_, text)| text.as_str())
        .collect::<Vec<_>>();
    expected.sort_unstable();
    assert_eq!(bodies(&received), expected);
    let deletes = received
        .iter()
        .enumerate()
        .map(
            |(index, message)| json!({"Id": format!("d{index}"), "ReceiptHandle": handle(message)}),
        )
        .collect::<Vec<_>>();
    let deleted = ok(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": deletes}),
    )
    .await;
    assert!(
        deleted
            .get("Failed")
            .is_none_or(|failed| failed == &json!([]))
    );

    ok(&manager, "DeleteQueue", json!({"QueueUrl": url})).await;
    let attributes = sqs(
        &manager,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": ["All"]}),
    )
    .await;
    assert_error(&attributes, "QueueDoesNotExist", MISSING_QUEUE);
    let one = sqs(
        &manager,
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "late"}),
    )
    .await;
    assert_error(&one, "QueueDoesNotExist", MISSING_QUEUE);
    let batch = json!({"QueueUrl": url, "Entries": [{"Id": "a", "MessageBody": "late"}]});
    assert_error(
        &sqs(&manager, "SendMessageBatch", batch).await,
        "QueueDoesNotExist",
        MISSING_QUEUE,
    );
    let deletes = json!({"QueueUrl": url, "Entries": [{"Id": "a", "ReceiptHandle": "AQEB-a"}]});
    assert_error(
        &sqs(&manager, "DeleteMessageBatch", deletes).await,
        "QueueDoesNotExist",
        MISSING_QUEUE,
    );
    let listed = ok(&manager, "ListQueues", json!({})).await;
    assert!(
        !listed.to_string().contains(&url),
        "the URL is absent from ListQueues"
    );
    store.finish().await;
}

/// AWS behavioural: check_e2e_work_queue_pattern.py
#[tokio::test]
async fn given_five_messages_when_received_in_batches_and_deleted_then_the_queue_reads_empty() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "work_queue", json!({})).await;
    for index in 0..5 {
        send(&manager, &url, &format!("job-{index}")).await;
    }
    let mut done = BTreeSet::new();
    for max in [3, 3] {
        for message in receive(&manager, &url, max, 600).await {
            ok(
                &manager,
                "DeleteMessage",
                json!({"QueueUrl": url, "ReceiptHandle": handle(&message)}),
            )
            .await;
            done.insert(body(&message).to_owned());
        }
    }
    assert_eq!(done.len(), 5);
    assert!(receive(&manager, &url, 10, 600).await.is_empty());
    assert_eq!(counters(&manager, &url).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural: check_receive_max_messages.py,
/// check_receive_message_respects_max_number_of_messages_cap.py
#[tokio::test]
async fn given_fifteen_messages_when_received_with_cap_five_then_no_receive_exceeds_it() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "cap", json!({})).await;
    let mut sent = BTreeSet::new();
    for index in 0..15 {
        send(&manager, &url, &format!("cap-{index}")).await;
        sent.insert(format!("cap-{index}"));
    }
    let mut seen = BTreeSet::new();
    for _ in 0..8 {
        let batch = receive(&manager, &url, 5, 600).await;
        assert!(batch.len() <= 5, "a receive returned {}", batch.len());
        seen.extend(batch.iter().map(|m| body(m).to_owned()));
    }
    assert_eq!(seen, sent);
    store.finish().await;
}

/// AWS behavioural: check_queue_level_wait_time_wakes_on_arrival.py
#[tokio::test]
async fn given_queue_level_wait_when_receive_names_none_then_it_waits_and_returns_a_later_send() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "queue_wait",
        json!({"Attributes": {"ReceiveMessageWaitTimeSeconds": "20"}}),
    )
    .await;
    let waiter = async {
        let request = json!({"QueueUrl": url, "MaxNumberOfMessages": 1});
        ok(&manager, "ReceiveMessage", request).await
    };
    let sender = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        send(&manager, &url, "wakes-queue-level-poller").await;
    };
    let (received, ()) = tokio::time::timeout(BOUND, async { futures::join!(waiter, sender) })
        .await
        .expect("the queue-level wait never woke within 10 s");
    let message = &received["Messages"][0];
    assert_eq!(body(message), "wakes-queue-level-poller");
    ok(
        &manager,
        "DeleteMessage",
        json!({"QueueUrl": url, "ReceiptHandle": handle(message)}),
    )
    .await;
    let after = json!({"QueueUrl": url, "WaitTimeSeconds": 0});
    assert!(
        ok(&manager, "ReceiveMessage", after)
            .await
            .get("Messages")
            .is_none_or(|m| m == &json!([]))
    );
    store.finish().await;
}

/// AWS behavioural: check_receive_wait_time_zero_overrides_queue_default.py
#[tokio::test]
async fn given_queue_level_wait_when_receive_names_zero_then_it_returns_at_once_without_a_later_send()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        "wait_zero",
        json!({"Attributes": {"ReceiveMessageWaitTimeSeconds": "20"}}),
    )
    .await;
    let short = json!({"QueueUrl": url, "WaitTimeSeconds": 0});
    let first = tokio::time::timeout(BOUND, ok(&manager, "ReceiveMessage", short))
        .await
        .expect("a zero wait stayed open for 10 s");
    assert!(first.get("Messages").is_none_or(|m| m == &json!([])));
    send(&manager, &url, "sent-after-the-short-poll").await;
    let later = json!({"QueueUrl": url, "WaitTimeSeconds": 2});
    let second = ok(&manager, "ReceiveMessage", later).await;
    assert_eq!(second["Messages"][0]["Body"], "sent-after-the-short-poll");
    store.finish().await;
}

/// AWS behavioural:
/// check_concurrent_create_queue_same_name_converges_to_one_queue.py
#[tokio::test]
async fn given_ten_identical_creates_at_once_when_all_finish_then_one_queue_with_the_requested_values()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("converge");
    let request = json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "41"}, "tags": {"team": "x"}});
    let results = join_all((0..10).map(|_| sqs(&manager, "CreateQueue", request.clone()))).await;
    let urls = results
        .iter()
        .map(|(status, body)| {
            assert_eq!(*status, 200, "{body}");
            body["QueueUrl"].as_str().unwrap().to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(urls.len(), 1, "one QueueUrl");
    let url = urls.into_iter().next().unwrap();
    assert!(url.ends_with(&name));
    let listed = ok(&manager, "ListQueues", json!({"QueueNamePrefix": name})).await;
    assert_eq!(listed["QueueUrls"].as_array().unwrap().len(), 1);
    assert_eq!(
        attribute_of(&manager, &url, "VisibilityTimeout").await,
        "41"
    );
    assert_eq!(attribute_of(&manager, &url, "DelaySeconds").await, "0");
    let tags = ok(&manager, "ListQueueTags", json!({"QueueUrl": url})).await;
    assert_eq!(tags["Tags"], json!({"team": "x"}));
    store.finish().await;
}

/// AWS behavioural: check_concurrent_set_queue_attributes_last_writer_wins.py
#[tokio::test]
async fn given_five_concurrent_timeout_sets_when_all_finish_then_the_value_is_one_submitted() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "last_writer", json!({})).await;
    let values = ["11", "22", "33", "44", "55"];
    let results = join_all(values.iter().map(|value| {
        sqs(
            &manager,
            "SetQueueAttributes",
            json!({"QueueUrl": url, "Attributes": {"VisibilityTimeout": value}}),
        )
    }))
    .await;
    assert!(
        results.iter().all(|(status, _)| *status == 200),
        "{results:?}"
    );
    let last = attribute_of(&manager, &url, "VisibilityTimeout").await;
    assert!(values.contains(&last.as_str()), "got {last}");
    store.finish().await;
}

/// AWS behavioural:
/// check_concurrent_set_queue_attributes_on_distinct_attributes_lose_no_update.
/// py
#[tokio::test]
async fn given_concurrent_sets_of_distinct_attributes_when_all_finish_then_no_write_is_lost() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "distinct_sets", json!({})).await;
    let rounds = [
        [
            ("VisibilityTimeout", "77"),
            ("DelaySeconds", "7"),
            ("MaximumMessageSize", "200000"),
            ("MessageRetentionPeriod", "7777"),
            ("ReceiveMessageWaitTimeSeconds", "7"),
        ],
        [
            ("VisibilityTimeout", "88"),
            ("DelaySeconds", "8"),
            ("MaximumMessageSize", "300000"),
            ("MessageRetentionPeriod", "8888"),
            ("ReceiveMessageWaitTimeSeconds", "8"),
        ],
    ];
    for round in rounds {
        let results = join_all(round.iter().map(|(name, value)| {
            sqs(
                &manager,
                "SetQueueAttributes",
                json!({"QueueUrl": url, "Attributes": {*name: *value}}),
            )
        }))
        .await;
        assert!(
            results.iter().all(|(status, _)| *status == 200),
            "{results:?}"
        );
        for (name, value) in round {
            assert_eq!(attribute_of(&manager, &url, name).await, value, "{name}");
        }
    }
    store.finish().await;
}

/// AWS behavioural: check_concurrent_tag_queue_no_lost_updates.py
#[tokio::test]
async fn given_eight_concurrent_tag_calls_when_all_finish_then_exactly_those_eight_tags_exist() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "racing_tags", json!({})).await;
    // Eight writers contend on one catalog row, so the bounded native retry can
    // run out and answer 503. An SDK retries 503, so each caller does too.
    let results = join_all((0..8).map(|index| {
        let (manager, url) = (&manager, &url);
        async move {
            let tags = json!({format!("racer-{index}"): format!("value-{index}")});
            let mut result = (503, Value::Null);
            for _ in 0..10 {
                result = sqs(manager, "TagQueue", json!({"QueueUrl": url, "Tags": tags})).await;
                if result.0 != 503 {
                    break;
                }
            }
            result
        }
    }))
    .await;
    assert!(
        results.iter().all(|(status, _)| *status == 200),
        "{results:?}"
    );
    let expected = (0..8)
        .map(|index| (format!("racer-{index}"), json!(format!("value-{index}"))))
        .collect::<serde_json::Map<_, _>>();
    let listed = ok(&manager, "ListQueueTags", json!({"QueueUrl": url})).await;
    assert_eq!(listed["Tags"], Value::Object(expected));
    store.finish().await;
}

/// AWS behavioural:
/// check_standard_queue_concurrent_producers_and_consumers_lose_no_message.py
#[tokio::test]
async fn given_three_producers_and_six_consumers_when_all_run_then_every_sent_message_is_received_once()
 {
    const TOTAL: usize = 150;
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "producers_consumers", json!({})).await;
    let received = RefCell::new(Vec::<String>::new());
    let failures = RefCell::new(0usize);
    let deadline = tokio::time::Instant::now() + BOUND;

    let producer = |producer: usize| {
        let (manager, url, failures) = (&manager, &url, &failures);
        async move {
            let mut acknowledged = Vec::new();
            for batch in 0..5 {
                let entries = (0..10)
                    .map(|entry| json!({"Id": format!("e{entry}"), "MessageBody": format!("p{producer}-b{batch}-e{entry}")}))
                    .collect::<Vec<_>>();
                let sent = ok(
                    manager,
                    "SendMessageBatch",
                    json!({"QueueUrl": url, "Entries": entries}),
                )
                .await;
                *failures.borrow_mut() += sent["Failed"].as_array().map_or(0, Vec::len);
                acknowledged.extend((0..10).map(|entry| format!("p{producer}-b{batch}-e{entry}")));
            }
            acknowledged
        }
    };
    let consumer = || {
        let (manager, url, received, failures) = (&manager, &url, &received, &failures);
        async move {
            while received.borrow().len() < TOTAL && tokio::time::Instant::now() < deadline {
                let messages = receive(manager, url, 10, 600).await;
                if messages.is_empty() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    continue;
                }
                let entries = messages
                    .iter()
                    .enumerate()
                    .map(
                        |(index, m)| json!({"Id": format!("d{index}"), "ReceiptHandle": handle(m)}),
                    )
                    .collect::<Vec<_>>();
                let deleted = ok(
                    manager,
                    "DeleteMessageBatch",
                    json!({"QueueUrl": url, "Entries": entries}),
                )
                .await;
                *failures.borrow_mut() += deleted["Failed"].as_array().map_or(0, Vec::len);
                received
                    .borrow_mut()
                    .extend(messages.iter().map(|m| body(m).to_owned()));
            }
        }
    };
    let (acknowledged, _) = futures::join!(
        join_all((0..3).map(producer)),
        join_all((0..6).map(|_| consumer()))
    );
    let sent = acknowledged.into_iter().flatten().collect::<BTreeSet<_>>();
    assert_eq!(sent.len(), TOTAL);
    let mut got = received.into_inner();
    got.sort();
    assert_eq!(got.len(), TOTAL, "each message is received exactly once");
    assert_eq!(
        got.into_iter().collect::<BTreeSet<_>>(),
        sent,
        "no phantom bodies"
    );
    assert_eq!(failures.into_inner(), 0, "no Failed entries");
    assert_eq!(counters(&manager, &url).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural:
/// check_standard_queue_holds_more_than_one_thousand_messages.py
#[tokio::test]
async fn given_1001_messages_in_batches_of_ten_when_received_then_all_distinct_ids_and_bodies_arrive()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, "thousand", json!({})).await;
    let mut message_ids = BTreeSet::new();
    let mut sent = BTreeSet::new();
    for first in (0..1001).step_by(10) {
        let entries = (first..(first + 10).min(1001))
            .map(|index| json!({"Id": format!("i{index}"), "MessageBody": format!("body-{index}")}))
            .collect::<Vec<_>>();
        let result = ok(
            &manager,
            "SendMessageBatch",
            json!({"QueueUrl": url, "Entries": entries}),
        )
        .await;
        assert_eq!(
            result["Successful"].as_array().unwrap().len(),
            entries.len()
        );
        for entry in result["Successful"].as_array().unwrap() {
            message_ids.insert(entry["MessageId"].as_str().unwrap().to_owned());
        }
        sent.extend((first..(first + 10).min(1001)).map(|index| format!("body-{index}")));
    }
    assert_eq!(message_ids.len(), 1001, "1001 distinct MessageIds");
    assert_eq!(counters(&manager, &url).await, (1001, 0, 0));
    let mut seen = BTreeSet::new();
    loop {
        let batch = receive(&manager, &url, 10, 600).await;
        if batch.is_empty() {
            break;
        }
        seen.extend(batch.iter().map(|m| body(m).to_owned()));
    }
    assert_eq!(seen, sent);
    store.finish().await;
}
