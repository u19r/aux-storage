//! AWS emulator behavioural suite ports.
//!
//! The expected codes and messages are AWS's, copied from the suite scripts.
//! A code is the legacy `x-amzn-query-error` code, which is what an AWS SDK
//! reports. Each test names its source script in a doc line. Delays and
//! visibility timeouts use the system clock (the manager stamps sends with
//! it), so those tests use one or two second timeouts and a bounded poll.

use std::time::{Duration, Instant};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use http_error::HttpApiError;
use queue::{QueueEndpoint, QueueManager};
use queue_provider::wire::{
    QueueProtocol, WireRequestParts, decode_wire_request, encode_error, encode_success,
};
use serde_json::{Value, json};

use super::test_support::TestStore;

const BASE_URL: &str = "http://aux.test/queue";
const POLL_BOUND: Duration = Duration::from_secs(10);
const NO_QUEUE: &str = "AWS.SimpleQueueService.NonExistentQueue";
const NO_QUEUE_MESSAGE: &str = "The specified queue does not exist.";
const INVALID: &str = "InvalidParameterValue";
const BODY_CHARACTERS: &str = "the set of allowed characters is #x9 | #xA | #xD | [#x20-#xD7FF] | \
                               [#xE000-#xFFFD] | [#x10000-#x10FFFF].";

/// The manager stamps sends with the system clock, so the store uses it too.
fn manager(store: &TestStore) -> QueueManager {
    store.use_system_time();
    QueueManager::new(store.queue_operations.clone()).with_endpoint(QueueEndpoint {
        public_base_url: BASE_URL.to_owned(),
    })
}

struct Reply {
    status: u16,
    /// The AWS error code (the `x-amzn-query-error` header), empty on success.
    code: String,
    message: String,
    body: Value,
}

impl Reply {
    fn succeeded(self) -> Value {
        assert_eq!(self.status, 200, "{}", self.body);
        self.body
    }

    fn assert_refused(&self, code: &str, message: &str) {
        assert_eq!(
            (self.status, self.code.as_str(), self.message.as_str()),
            (400, code, message),
            "{}",
            self.body
        );
    }
}

/// One SQS JSON request through decode, dispatch, and encode.
async fn call(manager: &QueueManager, action: &str, body: Value) -> Reply {
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
    let value: Value = serde_json::from_slice(&response.body).unwrap_or(Value::Null);
    let code = response
        .headers
        .iter()
        .find(|(name, _)| *name == "x-amzn-query-error")
        .and_then(|(_, value)| value.split(';').next())
        .unwrap_or_default()
        .to_owned();
    let message = value
        .get("message")
        .or_else(|| value.get("Message"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    Reply {
        status: response.status,
        code,
        message,
        body: value,
    }
}

fn merge(base: Value, extra: Value) -> Value {
    let mut base = base;
    if let (Some(base), Some(extra)) = (base.as_object_mut(), extra.as_object()) {
        base.extend(extra.clone());
    }
    base
}

fn unique(label: &str) -> String {
    format!("{label}{}", uuid::Uuid::new_v4().simple())
}

async fn create(manager: &QueueManager, name: &str, extra: Value) -> String {
    let body = merge(json!({ "QueueName": name }), extra);
    let created = call(manager, "CreateQueue", body).await.succeeded();
    created["QueueUrl"].as_str().unwrap().to_owned()
}

async fn send(manager: &QueueManager, url: &str, body: &str, extra: Value) -> Reply {
    let request = merge(json!({"QueueUrl": url, "MessageBody": body}), extra);
    call(manager, "SendMessage", request).await
}

async fn send_ok(manager: &QueueManager, url: &str, body: &str) -> Value {
    send(manager, url, body, json!({})).await.succeeded()
}

fn messages(body: &Value) -> Vec<Value> {
    body["Messages"].as_array().cloned().unwrap_or_default()
}

async fn receive(manager: &QueueManager, url: &str, extra: Value) -> Vec<Value> {
    let request = merge(json!({"QueueUrl": url, "MaxNumberOfMessages": 10}), extra);
    messages(&call(manager, "ReceiveMessage", request).await.succeeded())
}

/// Polls until a receive returns messages; fails after `POLL_BOUND`.
async fn receive_within_bound(
    manager: &QueueManager,
    url: &str,
    extra: Value,
    waiting_for: &str,
) -> Vec<Value> {
    let start = Instant::now();
    loop {
        let got = receive(manager, url, extra.clone()).await;
        if !got.is_empty() {
            return got;
        }
        assert!(
            start.elapsed() < POLL_BOUND,
            "no message arrived within {POLL_BOUND:?} while waiting for {waiting_for}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Every message that is visible now, received until a receive is empty.
async fn drain(manager: &QueueManager, url: &str, extra: Value) -> Vec<Value> {
    let mut all = Vec::new();
    for _ in 0..100 {
        let got = receive(manager, url, extra.clone()).await;
        if got.is_empty() {
            break;
        }
        all.extend(got);
    }
    all
}

fn sorted_bodies(received: &[Value]) -> Vec<String> {
    let mut bodies = received
        .iter()
        .map(|message| message["Body"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    bodies.sort();
    bodies
}

async fn attributes(manager: &QueueManager, url: &str, names: &[&str]) -> Value {
    let request = json!({"QueueUrl": url, "AttributeNames": names});
    call(manager, "GetQueueAttributes", request)
        .await
        .succeeded()["Attributes"]
        .clone()
}

async fn set_attributes(manager: &QueueManager, url: &str, attributes: Value) -> Reply {
    let request = json!({"QueueUrl": url, "Attributes": attributes});
    call(manager, "SetQueueAttributes", request).await
}

async fn tags(manager: &QueueManager, url: &str) -> Value {
    call(manager, "ListQueueTags", json!({"QueueUrl": url}))
        .await
        .succeeded()["Tags"]
        .clone()
}

fn ghost_url() -> String {
    format!("{BASE_URL}/000000000000/{}", unique("ghost"))
}

fn string_attribute(value: &str) -> Value {
    json!({"DataType": "String", "StringValue": value})
}

fn binary_attribute(value: &[u8]) -> Value {
    json!({"DataType": "Binary", "BinaryValue": STANDARD.encode(value)})
}

/// Pseudo-random letters (splitmix64), so that a large value does not compress
/// away.
fn random_text(length: usize, mut state: u64) -> String {
    (0..length)
        .map(|_| {
            state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut mixed = state;
            mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            mixed ^= mixed >> 31;
            char::from(b'a' + u8::try_from(mixed % 26).unwrap())
        })
        .collect()
}

fn attribute_map(count: usize) -> Value {
    Value::Object(
        (0..count)
            .map(|index| {
                (
                    format!("attr{index:02}"),
                    string_attribute(&format!("value-{index}")),
                )
            })
            .collect(),
    )
}

/// AWS behavioural: check_batch_request_validation_rejected.py,
/// check_delete_and_visibility_batches_over_ten_entries_rejected.py
#[tokio::test]
async fn given_empty_or_oversized_batches_when_sent_then_aws_codes_apply_and_nothing_acts() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("batches"), json!({})).await;

    call(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": []}),
    )
    .await
    .assert_refused(
        "AWS.SimpleQueueService.EmptyBatchRequest",
        "There should be at least one SendMessageBatchRequestEntry in the request.",
    );
    let two = json!({"QueueUrl": url, "Entries": [
        {"Id": "a", "MessageBody": "one"}, {"Id": "b", "MessageBody": "two"}]});
    let sent = call(&manager, "SendMessageBatch", two).await.succeeded();
    assert_eq!(sent["Successful"].as_array().unwrap().len(), 2, "{sent}");
    send_ok(&manager, &url, "three").await;

    let hold = json!({"VisibilityTimeout": 600});
    let received = receive(&manager, &url, hold.clone()).await;
    assert_eq!(received.len(), 3);
    let handles = received
        .iter()
        .map(|message| message["ReceiptHandle"].clone())
        .collect::<Vec<_>>();
    let eleven = |with_timeout: bool| {
        (0..11)
            .map(|index| {
                let mut entry = json!({"Id": format!("e{index}"),
                    "ReceiptHandle": handles[index % 3]});
                if with_timeout {
                    entry["VisibilityTimeout"] = json!(0);
                }
                entry
            })
            .collect::<Vec<_>>()
    };
    let too_many = (
        "AWS.SimpleQueueService.TooManyEntriesInBatchRequest",
        "Maximum number of entries per request are 10. You have sent 11.",
    );
    call(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": eleven(false)}),
    )
    .await
    .assert_refused(too_many.0, too_many.1);
    call(
        &manager,
        "ChangeMessageVisibilityBatch",
        json!({"QueueUrl": url, "Entries": eleven(true)}),
    )
    .await
    .assert_refused(too_many.0, too_many.1);
    call(
        &manager,
        "ChangeMessageVisibilityBatch",
        json!({"QueueUrl": url, "Entries": []}),
    )
    .await
    .assert_refused(
        "AWS.SimpleQueueService.EmptyBatchRequest",
        "There should be at least one ChangeMessageVisibilityBatchRequestEntry in the request.",
    );
    // The refused calls acted on nothing: the three messages are still held.
    assert!(receive(&manager, &url, json!({})).await.is_empty());

    // A batch of at most ten entries releases them, then deletes them.
    let release = (0..3)
        .map(|index| {
            json!({"Id": format!("r{index}"), "ReceiptHandle": handles[index],
            "VisibilityTimeout": 0})
        })
        .collect::<Vec<_>>();
    let released = call(
        &manager,
        "ChangeMessageVisibilityBatch",
        json!({"QueueUrl": url, "Entries": release}),
    )
    .await
    .succeeded();
    assert_eq!(
        released["Successful"].as_array().unwrap().len(),
        3,
        "{released}"
    );
    let again = receive(&manager, &url, hold).await;
    assert_eq!(sorted_bodies(&again), ["one", "three", "two"]);
    let delete = again
        .iter()
        .enumerate()
        .map(|(index, message)| {
            json!({"Id": format!("d{index}"),
            "ReceiptHandle": message["ReceiptHandle"]})
        })
        .collect::<Vec<_>>();
    let deleted = call(
        &manager,
        "DeleteMessageBatch",
        json!({"QueueUrl": url, "Entries": delete}),
    )
    .await
    .succeeded();
    assert_eq!(
        deleted["Successful"].as_array().unwrap().len(),
        3,
        "{deleted}"
    );
    call(
        &manager,
        "SendMessage",
        json!({"QueueUrl": url, "MessageBody": "last"}),
    )
    .await
    .succeeded();
    assert_eq!(
        sorted_bodies(&receive(&manager, &url, json!({})).await),
        ["last"]
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_batch_total_size_counts_message_attributes_not_only_bodies.py
#[tokio::test]
async fn given_batch_whose_attributes_exceed_the_limit_when_sent_then_batch_request_too_long() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        &unique("batchsize"),
        json!({"Attributes": {"MaximumMessageSize": "1048576"}}),
    )
    .await;
    // Each entry is 1 + 4 + 6 + 524280 bytes: body, attribute name, type,
    // value.
    let entry = |id: &str, seed: u64| {
        json!({"Id": id, "MessageBody": "a",
            "MessageAttributes": {"blob": string_attribute(&random_text(524_280, seed))}})
    };
    let batch = json!({"QueueUrl": url, "Entries": [entry("a", 1), entry("b", 2)]});
    call(&manager, "SendMessageBatch", batch)
        .await
        .assert_refused(
            "AWS.SimpleQueueService.BatchRequestTooLong",
            "Batch requests cannot be longer than 1048576 bytes. You have sent 1048582 bytes.",
        );
    assert!(receive(&manager, &url, json!({})).await.is_empty());

    for seed in [3, 4] {
        let alone = json!({"MessageAttributes": {
            "blob": string_attribute(&random_text(524_280, seed))}});
        send(&manager, &url, "a", alone).await.succeeded();
    }
    let delivered = receive(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(delivered.len(), 2);
    store.finish().await;
}

/// AWS behavioural: check_cli_sqs_nonexistent_queue_error.py,
/// check_nonexistent_queue.py, check_tag_nonexistent_queue.py,
/// check_operations_fail_after_delete.py,
/// check_tag_operations_on_a_deleted_queue_rejected.py
#[tokio::test]
async fn given_missing_or_deleted_queue_when_called_then_non_existent_queue_for_each_action() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let real = create(&manager, &unique("real"), json!({})).await;

    let ghost = ghost_url();
    for (action, body) in [
        (
            "SendMessage",
            json!({"QueueUrl": ghost, "MessageBody": "x"}),
        ),
        ("ReceiveMessage", json!({"QueueUrl": ghost})),
        ("TagQueue", json!({"QueueUrl": ghost, "Tags": {"a": "b"}})),
        ("UntagQueue", json!({"QueueUrl": ghost, "TagKeys": ["a"]})),
        ("ListQueueTags", json!({"QueueUrl": ghost})),
    ] {
        let reply = call(&manager, action, body).await;
        assert_eq!(reply.status, 400, "{action}: {}", reply.body);
        assert_eq!(
            (reply.code.as_str(), reply.message.as_str()),
            (NO_QUEUE, NO_QUEUE_MESSAGE),
            "{action}"
        );
    }
    // Positive controls on a real queue.
    send_ok(&manager, &real, "control").await;
    assert_eq!(
        sorted_bodies(&receive(&manager, &real, json!({})).await),
        ["control"]
    );
    assert!(receive(&manager, &real, json!({})).await.is_empty());
    let tag = json!({"QueueUrl": real, "Tags": {"a": "b"}});
    call(&manager, "TagQueue", tag).await.succeeded();
    assert_eq!(tags(&manager, &real).await, json!({"a": "b"}));

    // A deleted queue refuses every action.
    let name = unique("deleted");
    let deleted = create(
        &manager,
        &name,
        json!({"tags": {"sim2real": "x", "late": "y"}}),
    )
    .await;
    let sibling = create(
        &manager,
        &unique("sibling"),
        json!({"tags": {"sim2real": "x"}}),
    )
    .await;
    call(&manager, "DeleteQueue", json!({"QueueUrl": deleted}))
        .await
        .succeeded();
    for (action, body) in [
        (
            "SendMessage",
            json!({"QueueUrl": deleted, "MessageBody": "x"}),
        ),
        ("ReceiveMessage", json!({"QueueUrl": deleted})),
        (
            "GetQueueAttributes",
            json!({"QueueUrl": deleted, "AttributeNames": ["All"]}),
        ),
        ("ListQueueTags", json!({"QueueUrl": deleted})),
        (
            "TagQueue",
            json!({"QueueUrl": deleted, "Tags": {"late": "x"}}),
        ),
        (
            "UntagQueue",
            json!({"QueueUrl": deleted, "TagKeys": ["late"]}),
        ),
        ("DeleteQueue", json!({"QueueUrl": deleted})),
    ] {
        let reply = call(&manager, action, body).await;
        assert_eq!(reply.status, 400, "{action}: {}", reply.body);
        assert_eq!(
            (reply.code.as_str(), reply.message.as_str()),
            (NO_QUEUE, NO_QUEUE_MESSAGE),
            "{action}"
        );
    }
    let listed = call(&manager, "ListQueues", json!({"QueueNamePrefix": name}))
        .await
        .succeeded();
    assert!(
        listed
            .get("QueueUrls")
            .is_none_or(|urls| urls == &json!([])),
        "{listed}"
    );
    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": sibling, "Tags": {"late": "x"}}),
    )
    .await
    .succeeded();
    assert_eq!(
        tags(&manager, &sibling).await,
        json!({"sim2real": "x", "late": "x"})
    );
    call(
        &manager,
        "UntagQueue",
        json!({"QueueUrl": sibling, "TagKeys": ["late"]}),
    )
    .await
    .succeeded();
    assert_eq!(tags(&manager, &sibling).await, json!({"sim2real": "x"}));
    store.finish().await;
}

/// AWS behavioural:
/// check_queue_moves_an_exact_message_and_the_deleted_queue_is_gone.py
#[tokio::test]
async fn given_sent_message_when_received_and_queue_deleted_then_message_was_exact_and_queue_is_gone()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("exact");
    let url = create(&manager, &name, json!({})).await;
    let sent = send_ok(&manager, &url, "digest-subject-abcd1234").await;

    let received = receive_within_bound(&manager, &url, json!({}), "the sent message").await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["Body"], "digest-subject-abcd1234");
    // md5 of the body, computed outside aux.
    assert_eq!(received[0]["MD5OfBody"], "d4689d63f86eb9a12670cf4e4463a6c5");
    assert_eq!(received[0]["MessageId"], sent["MessageId"]);
    let delete = json!({"QueueUrl": url, "ReceiptHandle": received[0]["ReceiptHandle"]});
    call(&manager, "DeleteMessage", delete).await.succeeded();
    assert!(drain(&manager, &url, json!({})).await.is_empty());

    call(&manager, "DeleteQueue", json!({"QueueUrl": url}))
        .await
        .succeeded();
    call(
        &manager,
        "GetQueueAttributes",
        json!({"QueueUrl": url, "AttributeNames": ["All"]}),
    )
    .await
    .assert_refused(NO_QUEUE, NO_QUEUE_MESSAGE);
    send(&manager, &url, "late", json!({}))
        .await
        .assert_refused(NO_QUEUE, NO_QUEUE_MESSAGE);
    let listed = call(&manager, "ListQueues", json!({"QueueNamePrefix": name}))
        .await
        .succeeded();
    assert!(
        listed
            .get("QueueUrls")
            .is_none_or(|urls| urls == &json!([])),
        "{listed}"
    );
    store.finish().await;
}

fn policy(sid: &str, action: &str, resource: &str, principal: &str) -> String {
    json!({"Version": "2012-10-17", "Statement": [{"Sid": sid, "Effect": "Allow",
        "Principal": {"AWS": principal}, "Action": action, "Resource": resource}]})
    .to_string()
}

/// AWS behavioural:
/// check_create_queue_existing_name_different_policy_not_conflicting.py
#[tokio::test]
async fn given_existing_queue_when_created_again_with_policy_or_timeout_changes_then_only_real_differences_conflict()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("policy");
    let resource = format!("arn:aws:sqs:us-east-1:000000000000:{name}");
    let original = policy(
        "sim2real-original",
        "sqs:SendMessage",
        &resource,
        "000000000000",
    );
    let url = create(&manager, &name, json!({"Attributes": {"Policy": original}})).await;

    let sid_only = policy(
        "sim2real-changed",
        "sqs:SendMessage",
        &resource,
        "000000000000",
    );
    let again = create(&manager, &name, json!({"Attributes": {"Policy": sid_only}})).await;
    assert_eq!(again, url);
    let stored = attributes(&manager, &url, &["Policy"]).await;
    let stored: Value = serde_json::from_str(stored["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(stored["Statement"][0]["Sid"], "sim2real-original");

    for changed in [
        policy(
            "sim2real-original",
            "sqs:ReceiveMessage",
            &resource,
            "000000000000",
        ),
        policy(
            "sim2real-original",
            "sqs:SendMessage",
            &format!("{resource}-other"),
            "000000000000",
        ),
        policy(
            "sim2real-original",
            "sqs:SendMessage",
            &resource,
            "111111111111",
        ),
    ] {
        let request = json!({"QueueName": name, "Attributes": {"Policy": changed}});
        call(&manager, "CreateQueue", request).await.assert_refused(
            "QueueAlreadyExists",
            "A queue already exists with the same name and a different value for attribute Policy",
        );
    }
    let request = json!({"QueueName": name, "Attributes": {"VisibilityTimeout": "31"}});
    call(&manager, "CreateQueue", request).await.assert_refused(
        "QueueAlreadyExists",
        "A queue already exists with the same name and a different value for attribute \
         VisibilityTimeout",
    );
    let stored = attributes(&manager, &url, &["Policy"]).await;
    let stored: Value = serde_json::from_str(stored["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(stored["Statement"][0]["Sid"], "sim2real-original");
    assert_eq!(stored["Statement"][0]["Action"], "sqs:SendMessage");
    store.finish().await;
}

/// AWS behavioural: check_create_queue_existing_name_different_tags_rejected.py
///
/// `writing/sqs_parity.md` (CreateQueue row) says a repeated create keeps the
/// existing tags and was not probed; the suite probe shows AWS refusing, so
/// AWS's value is asserted.
#[tokio::test]
async fn given_existing_queue_when_created_again_with_other_tags_then_queue_already_exists() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("tagclash");
    let url = create(&manager, &name, json!({"tags": {"sim2real": "one"}})).await;
    assert_eq!(
        create(&manager, &name, json!({"tags": {"sim2real": "one"}})).await,
        url
    );

    let request = json!({"QueueName": name, "tags": {"sim2real": "two"}});
    call(&manager, "CreateQueue", request).await.assert_refused(
        "QueueAlreadyExists",
        "A queue already exists with the same name and a different value for tags ",
    );
    assert_eq!(tags(&manager, &url).await, json!({"sim2real": "one"}));
    store.finish().await;
}

/// AWS behavioural:
/// check_create_queue_invalid_attribute_creates_nothing_rejected.py,
/// check_queue_delay_seconds_attribute_bounds_rejected.py (CreateQueue leg)
#[tokio::test]
async fn given_out_of_range_attribute_when_queue_is_created_then_it_is_refused_and_no_queue_exists()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    for (attribute, bad, good) in [
        ("VisibilityTimeout", "43201", Some("43200")),
        ("DelaySeconds", "901", None),
    ] {
        let name = unique("createbad");
        let request = json!({"QueueName": name, "Attributes": {attribute: bad}});
        call(&manager, "CreateQueue", request).await.assert_refused(
            "InvalidAttributeValue",
            &format!("Invalid value for the parameter {attribute}."),
        );
        call(&manager, "GetQueueUrl", json!({"QueueName": name}))
            .await
            .assert_refused(NO_QUEUE, NO_QUEUE_MESSAGE);
        let accepted = good.unwrap_or("0");
        let url = create(
            &manager,
            &name,
            json!({"Attributes": {attribute: accepted}}),
        )
        .await;
        assert_eq!(
            attributes(&manager, &url, &[attribute]).await[attribute],
            accepted
        );
    }
    store.finish().await;
}

/// AWS behavioural: check_queue_attribute_range_bounds_rejected.py,
/// check_queue_delay_seconds_attribute_bounds_rejected.py,
/// check_visibility_timeout_out_of_range_rejected.py
#[tokio::test]
async fn given_out_of_range_attribute_when_set_then_invalid_attribute_value_and_value_is_unchanged()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("ranges"), json!({})).await;
    let cases: [(&str, &[&str], &[&str], &str); 5] = [
        (
            "MessageRetentionPeriod",
            &["59", "1209601"],
            &["60", "1209600"],
            "345600",
        ),
        (
            "MaximumMessageSize",
            &["1023", "1048577"],
            &["1024", "1048576"],
            // Recorded AWS (docs/sqs-sns-aws-probes.md): the default is 1 MiB.
            "1048576",
        ),
        ("ReceiveMessageWaitTimeSeconds", &["21"], &["0", "20"], "0"),
        ("DelaySeconds", &["-1", "901", "abc"], &["0", "900"], "0"),
        ("VisibilityTimeout", &["43201"], &["45", "43200"], "30"),
    ];
    for (name, bad, _, default) in cases {
        for value in bad {
            set_attributes(&manager, &url, json!({ name: value }))
                .await
                .assert_refused(
                    "InvalidAttributeValue",
                    &format!("Invalid value for the parameter {name}."),
                );
            assert_eq!(
                attributes(&manager, &url, &[name]).await[name],
                default,
                "{name}={value}"
            );
        }
    }
    for (name, _, good, _) in cases {
        for value in good {
            set_attributes(&manager, &url, json!({ name: value }))
                .await
                .succeeded();
            assert_eq!(attributes(&manager, &url, &[name]).await[name], *value);
        }
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_set_queue_attributes_one_invalid_value_applies_none_rejected.py
#[tokio::test]
async fn given_one_invalid_value_among_several_when_set_then_none_is_applied() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("atomic"), json!({})).await;
    let names = [
        "VisibilityTimeout",
        "DelaySeconds",
        "MessageRetentionPeriod",
    ];

    let invalid = json!({"VisibilityTimeout": "45", "DelaySeconds": "5",
        "MessageRetentionPeriod": "10"});
    set_attributes(&manager, &url, invalid)
        .await
        .assert_refused(
            "InvalidAttributeValue",
            "Invalid value for the parameter MessageRetentionPeriod.",
        );
    assert_eq!(
        attributes(&manager, &url, &names).await,
        json!({"VisibilityTimeout": "30", "DelaySeconds": "0", "MessageRetentionPeriod": "345600"})
    );
    let corrected = json!({"VisibilityTimeout": "45", "DelaySeconds": "5",
        "MessageRetentionPeriod": "120"});
    set_attributes(&manager, &url, corrected).await.succeeded();
    assert_eq!(
        attributes(&manager, &url, &names).await,
        json!({"VisibilityTimeout": "45", "DelaySeconds": "5", "MessageRetentionPeriod": "120"})
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_empty_collection_parameters_validated_inconsistently.py
#[tokio::test]
async fn given_empty_collections_when_sent_then_missing_parameter_and_nothing_changes() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("empty"), json!({"Attributes": {}})).await;
    assert_eq!(
        attributes(&manager, &url, &["VisibilityTimeout"]).await["VisibilityTimeout"],
        "30"
    );

    call(
        &manager,
        "SetQueueAttributes",
        json!({"QueueUrl": url, "Attributes": {}}),
    )
    .await
    .assert_refused(
        "MissingParameter",
        "The request must contain the parameter Attribute.Name.",
    );
    call(&manager, "TagQueue", json!({"QueueUrl": url, "Tags": {}}))
        .await
        .assert_refused(
            "MissingParameter",
            "The request must contain the parameter Tags.",
        );
    call(
        &manager,
        "UntagQueue",
        json!({"QueueUrl": url, "TagKeys": []}),
    )
    .await
    .assert_refused(
        "MissingParameter",
        "The request must contain the parameter TagKeys.",
    );
    assert_eq!(tags(&manager, &url).await, Value::Null);
    store.finish().await;
}

/// AWS behavioural: check_get_queue_url_does_not_format_validate_the_name.py
#[tokio::test]
async fn given_malformed_name_when_looked_up_then_not_found_while_create_is_invalid() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let malformed = format!("{}!bad name", unique("fmt"));

    call(&manager, "GetQueueUrl", json!({"QueueName": malformed}))
        .await
        .assert_refused(NO_QUEUE, NO_QUEUE_MESSAGE);
    call(&manager, "CreateQueue", json!({"QueueName": malformed}))
        .await
        .assert_refused(
            INVALID,
            "Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length",
        );
    call(
        &manager,
        "GetQueueUrl",
        json!({"QueueName": unique("wellformedabsent")}),
    )
    .await
    .assert_refused(NO_QUEUE, NO_QUEUE_MESSAGE);
    let name = unique("present");
    let url = create(&manager, &name, json!({})).await;
    let found = call(&manager, "GetQueueUrl", json!({"QueueName": name}))
        .await
        .succeeded();
    assert_eq!(found["QueueUrl"], url.as_str());
    store.finish().await;
}

/// AWS behavioural: check_immutable_queue_arn_mutation_rejected.py,
/// check_set_queue_attributes_readonly_attribute_rejected.py
#[tokio::test]
async fn given_read_only_attribute_when_set_then_unknown_attribute_and_value_is_unchanged() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("readonly"), json!({})).await;
    let arn = attributes(&manager, &url, &["QueueArn"]).await["QueueArn"].clone();

    for (name, value) in [
        ("QueueArn", "arn:aws:sqs:us-east-1:000000000000:other"),
        ("ApproximateNumberOfMessages", "1234"),
    ] {
        set_attributes(&manager, &url, json!({ name: value }))
            .await
            .assert_refused(
                "InvalidAttributeName",
                &format!("Unknown Attribute {name}."),
            );
    }
    assert_eq!(
        attributes(&manager, &url, &["QueueArn"]).await["QueueArn"],
        arn
    );
    assert_eq!(
        attributes(&manager, &url, &["ApproximateNumberOfMessages"]).await
            ["ApproximateNumberOfMessages"],
        "0"
    );
    set_attributes(&manager, &url, json!({"VisibilityTimeout": "45"}))
        .await
        .succeeded();
    assert_eq!(
        attributes(&manager, &url, &["VisibilityTimeout"]).await["VisibilityTimeout"],
        "45"
    );
    store.finish().await;
}

/// AWS behavioural: check_list_queues_prefix_matches_only_at_start.py
#[tokio::test]
async fn given_queue_names_when_listed_by_prefix_then_only_names_starting_with_it_match() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let token = unique("pfx");
    let starts = create(&manager, &format!("{token}-starts"), json!({})).await;
    create(&manager, &format!("other-{token}"), json!({})).await;

    let listed = call(&manager, "ListQueues", json!({"QueueNamePrefix": token}))
        .await
        .succeeded();
    assert_eq!(listed["QueueUrls"], json!([starts]));
    store.finish().await;
}

/// AWS behavioural:
/// check_long_poll_returns_immediately_when_message_already_available.py,
/// check_short_poll_returns_immediately.py (as a bounded wait, not a timing
/// assertion); check_long_poll_blocks_on_empty.py is covered by
/// `queue/src/manager/receive_tests.
/// rs::given_empty_queue_when_long_poll_expires_then_empty_response_does_not_return_early`
#[tokio::test]
async fn given_waiting_receive_when_message_is_available_or_wait_is_zero_then_it_returns_without_waiting_out()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("poll"), json!({})).await;

    let short = tokio::time::timeout(
        POLL_BOUND,
        call(
            &manager,
            "ReceiveMessage",
            json!({"QueueUrl": url, "WaitTimeSeconds": 0}),
        ),
    )
    .await
    .expect("a receive with WaitTimeSeconds 0 returned within the bound")
    .succeeded();
    assert!(messages(&short).is_empty(), "{short}");

    let sent = send_ok(&manager, &url, "ready").await;
    let long = tokio::time::timeout(
        POLL_BOUND,
        call(
            &manager,
            "ReceiveMessage",
            json!({"QueueUrl": url, "WaitTimeSeconds": 18}),
        ),
    )
    .await
    .expect("a long poll on a queue with a message returned within the bound")
    .succeeded();
    let got = messages(&long);
    assert_eq!(got.len(), 1, "{long}");
    assert_eq!(got[0]["MessageId"], sent["MessageId"]);
    store.finish().await;
}

/// AWS behavioural: check_e2e_delay_queue_then_consume.py
#[tokio::test]
async fn given_delay_queue_when_message_is_sent_then_it_is_invisible_until_due_and_received_once() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        &unique("delayq"),
        json!({"Attributes": {"DelaySeconds": "2"}}),
    )
    .await;
    send_ok(&manager, &url, "delayed-body").await;

    assert!(
        receive(&manager, &url, json!({})).await.is_empty(),
        "visible before the delay"
    );
    let got = receive_within_bound(&manager, &url, json!({}), "the delayed message").await;
    assert_eq!(sorted_bodies(&got), ["delayed-body"]);
    let delete = json!({"QueueUrl": url, "ReceiptHandle": got[0]["ReceiptHandle"]});
    call(&manager, "DeleteMessage", delete).await.succeeded();
    assert!(
        receive(&manager, &url, json!({})).await.is_empty(),
        "redelivered after delete"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_send_message_batch_delay_seconds_applies_to_each_entry_alone.py
#[tokio::test]
async fn given_batch_with_one_delayed_entry_when_received_then_the_other_entry_comes_first() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("batchdelay"), json!({})).await;
    let batch = json!({"QueueUrl": url, "Entries": [
        {"Id": "now", "MessageBody": "sim2real-now",
         "MessageAttributes": {"slot": string_attribute("immediate")}},
        {"Id": "later", "MessageBody": "sim2real-later", "DelaySeconds": 2,
         "MessageAttributes": {"slot": string_attribute("delayed")}}]});
    let sent = call(&manager, "SendMessageBatch", batch).await.succeeded();
    let mut ids = sent["Successful"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["Id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    ids.sort();
    assert_eq!(ids, ["later", "now"]);
    assert!(
        sent.get("Failed").is_none_or(|failed| failed == &json!([])),
        "{sent}"
    );

    let attributes = json!({"MessageAttributeNames": ["All"]});
    let first = receive(&manager, &url, attributes.clone()).await;
    assert_eq!(sorted_bodies(&first), ["sim2real-now"]);
    assert_eq!(
        first[0]["MessageAttributes"]["slot"]["StringValue"],
        "immediate"
    );
    let second = receive_within_bound(&manager, &url, attributes, "the delayed entry").await;
    assert_eq!(sorted_bodies(&second), ["sim2real-later"]);
    assert_eq!(
        second[0]["MessageAttributes"]["slot"]["StringValue"],
        "delayed"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_max_message_size_lowered_spares_already_enqueued_messages.py
#[tokio::test]
async fn given_lowered_maximum_message_size_when_sending_then_oversize_is_refused_and_old_message_stays()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("maxsize"), json!({})).await;
    let old = random_text(4096, 7);
    send_ok(&manager, &url, &old).await;

    set_attributes(&manager, &url, json!({"MaximumMessageSize": "1024"}))
        .await
        .succeeded();
    send(&manager, &url, &random_text(1025, 8), json!({}))
        .await
        .assert_refused(
            INVALID,
            "One or more parameters are invalid. Reason: Message must be shorter than 1024 bytes.",
        );
    send_ok(&manager, &url, &random_text(1024, 9)).await;

    let got = drain(&manager, &url, json!({})).await;
    let bodies = got
        .iter()
        .map(|m| m["Body"].as_str().unwrap().len())
        .collect::<Vec<_>>();
    assert!(
        got.iter().any(|m| m["Body"] == old.as_str()),
        "the 4096-byte message was damaged: {bodies:?}"
    );
    assert_eq!(got.len(), 2, "{bodies:?}");
    store.finish().await;
}

/// AWS behavioural: check_message_attribute_name_rules_rejected.py
#[tokio::test]
async fn given_message_attribute_names_when_sent_then_rule_violations_are_refused_and_lookalikes_accepted()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("attrname"), json!({})).await;
    let reserved = "You can't use message attribute names beginning with 'AWS.' or 'Amazon'. \
                    These strings are reserved for internal use.";
    let refused = [
        ("AWS.Trace".to_owned(), reserved.to_owned()),
        ("aws.trace".to_owned(), reserved.to_owned()),
        ("Amazon.Route".to_owned(), reserved.to_owned()),
        (
            ".leading".to_owned(),
            "Message (user) attribute name must not begin with character '.'.".to_owned(),
        ),
        (
            "trailing.".to_owned(),
            "Message (user) attribute name must not end with character '.'.".to_owned(),
        ),
        (
            "two..dots".to_owned(),
            "Message (user) attribute name must not have successive '.' characters.".to_owned(),
        ),
        (
            "has space".to_owned(),
            "Message (user) attribute name contains invalid non-alphanumeric character '#x20'. \
             You can use only alphanumeric characters, hyphens (-), underscores (_), or periods \
             (.)."
                .to_owned(),
        ),
        (
            "n".repeat(257),
            "Message (user) attribute name must be shorter than 256 bytes.".to_owned(),
        ),
    ];
    let accepted = [
        "Corp.Trace-Id_v2".to_owned(),
        "AWSRegionHint".to_owned(),
        "amazonroute".to_owned(),
        "n".repeat(256),
    ];
    for (index, name) in accepted.iter().enumerate() {
        let extra = json!({"MessageAttributes": {name: string_attribute("v")}});
        send(&manager, &url, &format!("accepted-{index}"), extra)
            .await
            .succeeded();
    }
    for (name, message) in &refused {
        let extra = json!({"MessageAttributes": {name: string_attribute("v")}});
        send(&manager, &url, "refused", extra)
            .await
            .assert_refused(INVALID, message);
    }
    let got = drain(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(got.len(), accepted.len(), "{:?}", sorted_bodies(&got));
    for message in &got {
        let index: usize = message["Body"]
            .as_str()
            .unwrap()
            .strip_prefix("accepted-")
            .unwrap()
            .parse()
            .unwrap();
        let names = message["MessageAttributes"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(names, [accepted[index].clone()]);
    }
    store.finish().await;
}

/// AWS behavioural: check_message_attribute_type_and_number_value_validated.py,
/// check_message_attribute_type_length_bounds_rejected.py
#[tokio::test]
async fn given_message_attribute_types_and_numbers_when_sent_then_invalid_ones_are_refused_and_valid_ones_round_trip()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("attrtype"), json!({})).await;
    let not_a_number = "Can't cast the value of message (user) attribute 'N' to a number.";
    let bad_type = "The type of message (user) attribute 'N' is invalid. You must use only the \
                    following supported type prefixes: Binary, Number, String.";
    let too_precise = "4".repeat(39);
    let too_precise_message = format!(
        "Message (user) attribute 'N' number value [{too_precise}] must contain fewer than 38 \
         precision digits."
    );
    let refused = [
        (
            json!({"DataType": "Number", "StringValue": "abc"}),
            not_a_number.to_owned(),
        ),
        (
            json!({"DataType": "Number", "StringValue": " 1"}),
            not_a_number.to_owned(),
        ),
        (
            json!({"DataType": "Number", "StringValue": too_precise}),
            too_precise_message,
        ),
        (
            json!({"DataType": "Strin", "StringValue": "1"}),
            bad_type.to_owned(),
        ),
        (
            json!({"DataType": "Number.", "StringValue": "1"}),
            bad_type.to_owned(),
        ),
        (
            json!({"DataType": format!("String.{}", "s".repeat(250)), "StringValue": "v"}),
            "Message (user) attribute type must be shorter than 256 bytes.".to_owned(),
        ),
    ];
    let long_type = format!("String.{}", "s".repeat(249));
    let accepted = [
        (
            "negative-decimal",
            json!({"DataType": "Number", "StringValue": "-12.5"}),
            json!({"DataType": "Number", "StringValue": "-12.5"}),
        ),
        // An exponent reads back in plain notation.
        (
            "exponent",
            json!({"DataType": "Number", "StringValue": "1e3"}),
            json!({"DataType": "Number", "StringValue": "1000"}),
        ),
        (
            "custom-number",
            json!({"DataType": "Number.float", "StringValue": "0.000001"}),
            json!({"DataType": "Number.float", "StringValue": "0.000001"}),
        ),
        (
            "custom-binary",
            json!({"DataType": "Binary.png", "BinaryValue": STANDARD.encode(b"\x89PNG")}),
            json!({"DataType": "Binary.png", "BinaryValue": STANDARD.encode(b"\x89PNG")}),
        ),
        (
            "long-type",
            json!({"DataType": long_type, "StringValue": "v"}),
            json!({"DataType": long_type, "StringValue": "v"}),
        ),
    ];
    for (body, attribute, _) in &accepted {
        let extra = json!({"MessageAttributes": {"N": attribute}});
        send(&manager, &url, body, extra).await.succeeded();
    }
    for (attribute, message) in &refused {
        let extra = json!({"MessageAttributes": {"N": attribute}});
        send(&manager, &url, "refused", extra)
            .await
            .assert_refused(INVALID, message);
    }
    let got = drain(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(got.len(), accepted.len(), "{:?}", sorted_bodies(&got));
    for message in &got {
        let body = message["Body"].as_str().unwrap();
        let (_, _, want) = accepted.iter().find(|(name, _, _)| *name == body).unwrap();
        let back = &message["MessageAttributes"]["N"];
        for field in ["DataType", "StringValue", "BinaryValue"] {
            assert_eq!(back.get(field), want.get(field), "{body}: {field}");
        }
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_message_attribute_value_in_the_wrong_field_for_its_data_type_rejected.
/// py
#[tokio::test]
async fn given_attribute_value_in_the_wrong_field_when_sent_then_refused_and_batch_entry_fails_alone()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("wrongfield"), json!({})).await;
    let cases = [
        (
            "blob",
            json!({"DataType": "Binary", "StringValue": "aGk="}),
            "Message (user) attribute 'blob' with type 'Binary' must use field 'Binary'.",
        ),
        (
            "label",
            json!({"DataType": "String", "BinaryValue": STANDARD.encode(b"hi")}),
            "Message (user) attribute 'label' with type 'String' must use field 'String'.",
        ),
        (
            "count",
            json!({"DataType": "Number", "BinaryValue": STANDARD.encode(b"1")}),
            "Message (user) attribute 'count' with type 'Number' must use field 'String'.",
        ),
        (
            "label",
            json!({"DataType": "String"}),
            "Message (user) attribute 'label' must contain a non-empty value of type 'String'.",
        ),
    ];
    for (index, (name, value, message)) in cases.iter().enumerate() {
        let extra = json!({"MessageAttributes": {*name: value}});
        send(&manager, &url, &format!("sim2real-refused-{index}"), extra)
            .await
            .assert_refused(INVALID, message);
    }
    let mixed = json!({"QueueUrl": url, "Entries": [
        {"Id": "good", "MessageBody": "sim2real-accepted",
         "MessageAttributes": {"blob": binary_attribute(b"\x00\xff")}},
        {"Id": "bad", "MessageBody": "sim2real-refused-mixed",
         "MessageAttributes": {"blob": cases[0].1.clone()}}]});
    let reply = call(&manager, "SendMessageBatch", mixed).await.succeeded();
    let successful = reply["Successful"].as_array().unwrap();
    assert_eq!(successful.len(), 1, "{reply}");
    assert_eq!(successful[0]["Id"], "good");
    let failed = reply["Failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "{reply}");
    assert_eq!(
        (
            failed[0]["Id"].as_str(),
            failed[0]["Code"].as_str(),
            failed[0]["Message"].as_str(),
            failed[0]["SenderFault"].as_bool()
        ),
        (Some("bad"), Some(INVALID), Some(cases[0].2), Some(true))
    );
    let all_bad = json!({"QueueUrl": url, "Entries": (0..2).map(|index| json!({
        "Id": format!("bad{index}"), "MessageBody": format!("sim2real-refused-all-{index}"),
        "MessageAttributes": {cases[index].0: cases[index].1.clone()}})).collect::<Vec<_>>()});
    let reply = call(&manager, "SendMessageBatch", all_bad)
        .await
        .succeeded();
    assert!(
        reply.get("Successful").is_none_or(|ok| ok == &json!([])),
        "{reply}"
    );
    assert_eq!(reply["Failed"].as_array().unwrap().len(), 2, "{reply}");

    let got = drain(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(sorted_bodies(&got), ["sim2real-accepted"]);
    assert_eq!(
        got[0]["MessageAttributes"]["blob"],
        json!({"DataType": "Binary", "BinaryValue": STANDARD.encode(b"\x00\xff")})
    );
    store.finish().await;
}

/// AWS behavioural: check_message_attributes_round_trip_types.py,
/// check_message_attributes_round_trip_with_md5.py,
/// check_message_digests_recomputed_from_the_documented_canonical_encoding.py
///
/// The digests are pinned values computed outside aux from the documented
/// encoding (length-prefixed name and type, a transport byte, the value).
/// The reversed insertion order leg is not portable: a JSON object has no
/// order.
#[tokio::test]
async fn given_string_number_and_binary_attributes_when_sent_then_digests_equal_the_documented_encoding()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("digests"), json!({})).await;
    let names = json!({"MessageAttributeNames": ["All"],
        "MessageSystemAttributeNames": ["All"]});

    let attributes = json!({"StringAttr": string_attribute("hello-world"),
        "NumberAttr": {"DataType": "Number", "StringValue": "42"},
        "BinaryAttr": binary_attribute(b"\x00\x01\xff\xfe")});
    let sent = send(
        &manager,
        &url,
        "attrs-md5-round-trip",
        json!({"MessageAttributes": attributes}),
    )
    .await
    .succeeded();
    assert_eq!(
        sent["MD5OfMessageAttributes"],
        "449eb38d855e790ba68995ae92338d6c"
    );
    let got = receive_within_bound(&manager, &url, names.clone(), "the attribute message").await;
    assert_eq!(got[0]["MessageAttributes"], attributes);
    assert_eq!(
        got[0]["MD5OfMessageAttributes"],
        "449eb38d855e790ba68995ae92338d6c"
    );
    let delete = json!({"QueueUrl": url, "ReceiptHandle": got[0]["ReceiptHandle"]});
    call(&manager, "DeleteMessage", delete).await.succeeded();

    let trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    let custom = json!({
        "zeta": {"DataType": "String.urn", "StringValue": "urn:sim2real:abcd1234"},
        "alpha": {"DataType": "Number", "StringValue": "7"},
        "mid": {"DataType": "Binary.gzip", "BinaryValue": STANDARD.encode((0..8u8).collect::<Vec<_>>())}});
    let sent = send(
        &manager,
        &url,
        "digest-subject-abcd1234",
        json!({
        "MessageAttributes": custom,
        "MessageSystemAttributes": {"AWSTraceHeader": string_attribute(trace)}}),
    )
    .await
    .succeeded();
    assert_eq!(sent["MD5OfMessageBody"], "d4689d63f86eb9a12670cf4e4463a6c5");
    assert_eq!(
        sent["MD5OfMessageAttributes"],
        "12445a378afb68fb523386c07947b18b"
    );
    assert_eq!(
        sent["MD5OfMessageSystemAttributes"],
        "5ae4d5d7636402d80f4eb6d213245a88"
    );
    let got = receive_within_bound(&manager, &url, names.clone(), "the digest message").await;
    assert_eq!(got[0]["MD5OfBody"], "d4689d63f86eb9a12670cf4e4463a6c5");
    assert_eq!(
        got[0]["MD5OfMessageAttributes"],
        "12445a378afb68fb523386c07947b18b"
    );
    assert_eq!(got[0]["MessageAttributes"], custom);

    // One byte of one value changes the digest.
    let mut changed = custom.clone();
    changed["mid"]["BinaryValue"] = json!(STANDARD.encode([255u8, 1, 2, 3, 4, 5, 6, 7]));
    let sent = send(
        &manager,
        &url,
        "digest-sensitivity",
        json!({"MessageAttributes": changed}),
    )
    .await
    .succeeded();
    assert_eq!(
        sent["MD5OfMessageAttributes"],
        "379ed46da798b5ff4324aaecf4661e44"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_sse_sqs_managed_encryption_round_trips_body_byte_exact.py
///
/// Aux does not encrypt (`writing/sqs_parity.md`, server-side encryption
/// row); the premise is dropped and the byte-exact round trip is kept.
#[tokio::test]
async fn given_sse_queue_when_multibyte_body_is_sent_then_it_round_trips_with_plaintext_digests() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let parts = [
        "ascii-plain",
        "\t tab-and-newline \n and-carriage-return \r",
        "2-byte: \u{e9}\u{fc}\u{f1} \u{416}\u{438}",
        "3-byte: \u{65e5}\u{672c}\u{8a9e} \u{20ac} \u{fffd}",
        "4-byte: \u{1f600}\u{1f9ea}\u{10fffd}",
        "boundary: \u{d7ff} \u{e000}",
    ];
    let body = format!("sse-abcd1234|{}", parts.join("|"));
    let attribute_text = format!("attr-{}", parts[2..5].concat());
    let binary = (0..=255u8).collect::<Vec<_>>();
    let attributes =
        json!({"s": string_attribute(&attribute_text), "b": binary_attribute(&binary)});

    let on = create(
        &manager,
        &unique("sseon"),
        json!({"Attributes": {"SqsManagedSseEnabled": "true"}}),
    )
    .await;
    assert_eq!(
        attributes_of(&manager, &on).await["SqsManagedSseEnabled"],
        "true"
    );
    let off = create(
        &manager,
        &unique("sseoff"),
        json!({"Attributes": {"SqsManagedSseEnabled": "false"}}),
    )
    .await;
    assert_eq!(
        attributes_of(&manager, &off).await["SqsManagedSseEnabled"],
        "false"
    );

    for url in [&on, &off] {
        let sent = send(
            &manager,
            url,
            &body,
            json!({"MessageAttributes": attributes}),
        )
        .await
        .succeeded();
        assert_eq!(sent["MD5OfMessageBody"], "188221e9a8c7a32bdaac76638cd58396");
        assert_eq!(
            sent["MD5OfMessageAttributes"],
            "66431a3e45deeac62c54146b0a574550"
        );
        let got = receive_within_bound(
            &manager,
            url,
            json!({"MessageAttributeNames": ["All"]}),
            "the multibyte message",
        )
        .await;
        assert_eq!(got[0]["Body"], body.as_str());
        assert_eq!(got[0]["MD5OfBody"], "188221e9a8c7a32bdaac76638cd58396");
        assert_eq!(
            got[0]["MD5OfMessageAttributes"],
            "66431a3e45deeac62c54146b0a574550"
        );
        assert_eq!(got[0]["MessageAttributes"], attributes);
    }
    store.finish().await;
}

async fn attributes_of(manager: &QueueManager, url: &str) -> Value {
    attributes(manager, url, &["All"]).await
}

/// AWS behavioural:
/// check_message_system_attribute_other_than_aws_trace_header_rejected.py,
/// check_send_message_malformed_awstraceheader_accepted_then_dropped.py
#[tokio::test]
async fn given_system_attributes_when_sent_then_only_a_valid_trace_header_is_kept() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("sysattr"), json!({})).await;
    let trace = "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1";
    let invalid = "Message system attribute name 'traceparent' is invalid.";

    let other = json!({"MessageSystemAttributes": {"traceparent": string_attribute("00-abc")}});
    send(&manager, &url, "refused-single", other.clone())
        .await
        .assert_refused(INVALID, invalid);
    send(
        &manager,
        &url,
        "accepted-single",
        json!({
        "MessageSystemAttributes": {"AWSTraceHeader": string_attribute(trace)}}),
    )
    .await
    .succeeded();
    let batch = json!({"QueueUrl": url, "Entries": [
        {"Id": "good", "MessageBody": "accepted-batch",
         "MessageSystemAttributes": {"AWSTraceHeader": string_attribute(trace)}},
        {"Id": "bad", "MessageBody": "refused-batch",
         "MessageSystemAttributes": {"traceparent": string_attribute("00-abc")}}]});
    let reply = call(&manager, "SendMessageBatch", batch).await.succeeded();
    assert_eq!(reply["Successful"].as_array().unwrap().len(), 1, "{reply}");
    let failed = reply["Failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "{reply}");
    assert_eq!(
        (
            failed[0]["Id"].as_str(),
            failed[0]["Code"].as_str(),
            failed[0]["Message"].as_str(),
            failed[0]["SenderFault"].as_bool()
        ),
        (Some("bad"), Some(INVALID), Some(invalid), Some(true))
    );

    send(
        &manager,
        &url,
        "empty-header",
        json!({
        "MessageSystemAttributes": {"AWSTraceHeader": string_attribute("")}}),
    )
    .await
    .assert_refused(
        INVALID,
        "Message system attribute 'AWSTraceHeader' must contain a non-empty value of type \
         'String'.",
    );
    for (body, header) in [
        ("garbage", string_attribute("not a trace header")),
        (
            "number-type",
            json!({"DataType": "Number", "StringValue": "1"}),
        ),
        ("binary-type", binary_attribute(b"x")),
    ] {
        send(
            &manager,
            &url,
            body,
            json!({"MessageSystemAttributes": {"AWSTraceHeader": header}}),
        )
        .await
        .succeeded();
    }

    let got = drain(
        &manager,
        &url,
        json!({"MessageSystemAttributeNames": ["AWSTraceHeader"]}),
    )
    .await;
    assert_eq!(
        sorted_bodies(&got),
        [
            "accepted-batch",
            "accepted-single",
            "binary-type",
            "garbage",
            "number-type"
        ]
    );
    for message in &got {
        let body = message["Body"].as_str().unwrap();
        let header = message["Attributes"]
            .get("AWSTraceHeader")
            .and_then(Value::as_str);
        if body.starts_with("accepted") {
            assert_eq!(header, Some(trace), "{body}");
        } else {
            assert_eq!(header, None, "{body}");
        }
    }
    store.finish().await;
}

/// AWS behavioural: check_more_than_ten_message_attributes_rejected.py
#[tokio::test]
async fn given_eleven_message_attributes_when_sent_then_refused_and_batch_sibling_lands() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("tenattrs"), json!({})).await;
    let message = "Number of message attributes [11] exceeds the allowed maximum [10].";

    send(
        &manager,
        &url,
        "ten-attributes",
        json!({"MessageAttributes": attribute_map(10)}),
    )
    .await
    .succeeded();
    send(
        &manager,
        &url,
        "eleven-single",
        json!({"MessageAttributes": attribute_map(11)}),
    )
    .await
    .assert_refused(INVALID, message);
    let batch = json!({"QueueUrl": url, "Entries": [
        {"Id": "ok", "MessageBody": "batch-sibling"},
        {"Id": "bad", "MessageBody": "eleven-batch", "MessageAttributes": attribute_map(11)}]});
    let reply = call(&manager, "SendMessageBatch", batch).await.succeeded();
    assert_eq!(reply["Successful"].as_array().unwrap().len(), 1, "{reply}");
    assert_eq!(reply["Successful"][0]["Id"], "ok");
    let failed = &reply["Failed"][0];
    assert_eq!(
        (
            failed["Id"].as_str(),
            failed["Code"].as_str(),
            failed["SenderFault"].as_bool(),
            failed["Message"].as_str()
        ),
        (Some("bad"), Some(INVALID), Some(true), Some(message))
    );

    let got = drain(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(sorted_bodies(&got), ["batch-sibling", "ten-attributes"]);
    let ten = got.iter().find(|m| m["Body"] == "ten-attributes").unwrap();
    assert_eq!(ten["MessageAttributes"], attribute_map(10));
    store.finish().await;
}

/// AWS behavioural: check_op_receive_message_param_bounds_rejected.py
#[tokio::test]
async fn given_receive_parameters_out_of_range_when_received_then_invalid_parameter_value() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("recvbounds"), json!({})).await;
    assert!(receive(&manager, &url, json!({})).await.is_empty());

    call(
        &manager,
        "ReceiveMessage",
        json!({"QueueUrl": url, "MaxNumberOfMessages": 11}),
    )
    .await
    .assert_refused(
        INVALID,
        "Value 11 for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10, \
         if provided.",
    );
    call(
        &manager,
        "ReceiveMessage",
        json!({"QueueUrl": url, "WaitTimeSeconds": 21}),
    )
    .await
    .assert_refused(
        INVALID,
        "Value 21 for parameter WaitTimeSeconds is invalid. Reason: Must be >= 0 and <= 20, if \
         provided.",
    );
    assert!(receive(&manager, &url, json!({})).await.is_empty());
    store.finish().await;
}

/// AWS behavioural: check_op_visibility_timeout_out_of_range_rejected.py
#[tokio::test]
async fn given_visibility_timeout_out_of_range_when_changed_then_refused_and_zero_releases_the_message()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("visrange"), json!({})).await;
    let sent = send_ok(&manager, &url, "held").await;
    let got = receive(&manager, &url, json!({})).await;
    let handle = got[0]["ReceiptHandle"].clone();

    for timeout in [43201, -1] {
        let request =
            json!({"QueueUrl": url, "ReceiptHandle": handle, "VisibilityTimeout": timeout});
        call(&manager, "ChangeMessageVisibility", request)
            .await
            .assert_refused(
                INVALID,
                &format!(
                    "Value {timeout} for parameter VisibilityTimeout is invalid. Reason: \
                     VisibilityTimeout must be an integer between 0 and 43200."
                ),
            );
    }
    let release = json!({"QueueUrl": url, "ReceiptHandle": handle, "VisibilityTimeout": 0});
    call(&manager, "ChangeMessageVisibility", release)
        .await
        .succeeded();
    let again = receive_within_bound(&manager, &url, json!({}), "the released message").await;
    assert_eq!(again[0]["MessageId"], sent["MessageId"]);
    store.finish().await;
}

/// AWS behavioural: check_queue_arn_and_get_queue_url_consistency.py,
/// check_queue_arn_stable_round_trip.py
#[tokio::test]
async fn given_queue_when_arn_and_url_are_read_then_they_are_stable_and_consistent() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("arn");
    let url = create(&manager, &name, json!({})).await;

    let first = attributes(&manager, &url, &["QueueArn"]).await["QueueArn"].clone();
    let second = attributes(&manager, &url, &["QueueArn"]).await["QueueArn"].clone();
    assert_eq!(first, second);
    let arn = first.as_str().unwrap();
    let parts = arn.split(':').collect::<Vec<_>>();
    assert_eq!(parts.len(), 6, "{arn}");
    assert_eq!(
        (parts[0], parts[1], parts[2]),
        ("arn", "aws", "sqs"),
        "{arn}"
    );
    assert!(!parts[3].is_empty(), "{arn}");
    assert_eq!(
        (parts[4], parts[5]),
        ("000000000000", name.as_str()),
        "{arn}"
    );
    let found = call(&manager, "GetQueueUrl", json!({"QueueName": name}))
        .await
        .succeeded();
    assert_eq!(found["QueueUrl"], url.as_str());
    assert!(url.ends_with(&format!("/{name}")), "{url}");
    store.finish().await;
}

/// AWS behavioural:
/// check_redrive_policy_max_receive_count_out_of_range_rejected.py,
/// check_redrive_policy_missing_target_or_count_rejected.py (the missing
/// maxReceiveCount leg is covered by
/// `storage-api/src/routes/queue/protocol_tests.
/// rs::given_large_values_or_bad_redrive_policies_when_set_then_aws_errors`)
#[tokio::test]
async fn given_invalid_redrive_policy_when_set_then_refused_and_nothing_stored() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let base = unique("redrive");
    let dlq = create(&manager, &format!("{base}-dlq"), json!({})).await;
    let source = create(&manager, &format!("{base}-src"), json!({})).await;
    let dlq_arn = attributes(&manager, &dlq, &["QueueArn"]).await["QueueArn"]
        .as_str()
        .unwrap()
        .to_owned();
    let stored = || async { attributes(&manager, &source, &["RedrivePolicy"]).await };

    for bad in [0, 1001] {
        let document = json!({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": bad}).to_string();
        set_attributes(&manager, &source, json!({"RedrivePolicy": document}))
            .await
            .assert_refused(
                INVALID,
                &format!(
                    "Value {document} for parameter RedrivePolicy is invalid. Reason: Invalid \
                     value for maxReceiveCount: {bad}, valid values are from 1 to 1000 both \
                     inclusive."
                ),
            );
        assert!(
            stored().await["RedrivePolicy"].is_null(),
            "maxReceiveCount {bad} was stored"
        );
    }
    let ghost = json!({"deadLetterTargetArn": format!("{dlq_arn}-never-created"),
        "maxReceiveCount": 3})
    .to_string();
    set_attributes(&manager, &source, json!({"RedrivePolicy": ghost}))
        .await
        .assert_refused(
            INVALID,
            &format!(
                "Value {ghost} for parameter RedrivePolicy is invalid. Reason: Dead letter target \
                 does not exist."
            ),
        );
    assert!(
        stored().await["RedrivePolicy"].is_null(),
        "the ghost target was stored"
    );

    let boundary = json!({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": 1000}).to_string();
    set_attributes(&manager, &source, json!({"RedrivePolicy": boundary}))
        .await
        .succeeded();
    let policy: Value =
        serde_json::from_str(stored().await["RedrivePolicy"].as_str().unwrap()).unwrap();
    assert_eq!(policy["maxReceiveCount"], 1000);
    assert_eq!(policy["deadLetterTargetArn"], dlq_arn.as_str());
    let sources = call(
        &manager,
        "ListDeadLetterSourceQueues",
        json!({"QueueUrl": dlq}),
    )
    .await
    .succeeded();
    assert_eq!(sources["queueUrls"], json!([source]));
    store.finish().await;
}

/// AWS behavioural: check_send_message_batch_partial_failure.py
#[tokio::test]
async fn given_batch_with_an_invalid_body_character_when_sent_then_only_that_entry_fails() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("badchar"), json!({})).await;
    let batch = json!({"QueueUrl": url, "Entries": [
        {"Id": "good", "MessageBody": "good-body"},
        {"Id": "bad", "MessageBody": "bad\u{1}body"}]});
    let reply = call(&manager, "SendMessageBatch", batch).await.succeeded();
    assert_eq!(reply["Successful"].as_array().unwrap().len(), 1, "{reply}");
    assert_eq!(reply["Successful"][0]["Id"], "good");
    let failed = &reply["Failed"][0];
    assert_eq!(
        (
            failed["Id"].as_str(),
            failed["Code"].as_str(),
            failed["SenderFault"].as_bool(),
            failed["Message"].as_str()
        ),
        (
            Some("bad"),
            Some("InvalidMessageContents"),
            Some(true),
            Some(
                format!(
                    "Invalid binary character '#x1' was found in the message body, \
                     {BODY_CHARACTERS}"
                )
                .as_str()
            )
        )
    );
    assert_eq!(
        sorted_bodies(&drain(&manager, &url, json!({})).await),
        ["good-body"]
    );
    store.finish().await;
}

/// AWS behavioural: check_send_message_invalid_body_character_rejected.py
#[tokio::test]
async fn given_body_characters_when_sent_then_illegal_ones_are_refused_and_legal_ones_round_trip() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("bodychars"), json!({})).await;
    let refused = [
        ('\u{0}', "#x0"),
        ('\u{1}', "#x1"),
        ('\u{1f}', "#x1F"),
        ('\u{fffe}', "#xFFFE"),
        ('\u{ffff}', "#xFFFF"),
    ];
    let accepted = [
        '\t',
        '\r',
        ' ',
        '\u{d7ff}',
        '\u{e000}',
        '\u{fffd}',
        '\u{10000}',
        '\u{10ffff}',
    ];

    for (character, shown) in refused {
        send(&manager, &url, &format!("a{character}b"), json!({}))
            .await
            .assert_refused(
                "InvalidMessageContents",
                &format!(
                    "Invalid binary character '{shown}' was found in the message body, \
                     {BODY_CHARACTERS}"
                ),
            );
    }
    for character in accepted {
        send_ok(&manager, &url, &format!("a{character}b")).await;
    }
    let mut want = accepted
        .iter()
        .map(|c| format!("a{c}b"))
        .collect::<Vec<_>>();
    want.sort();
    assert_eq!(sorted_bodies(&drain(&manager, &url, json!({})).await), want);
    store.finish().await;
}

/// AWS behavioural:
/// check_sender_id_and_sent_timestamp_derive_from_the_caller_and_the_live_clock.
/// py (SenderId legs dropped: aux never produces SenderId,
/// `writing/sqs_parity.md` behavioural difference 13)
#[tokio::test]
async fn given_two_sends_when_received_then_sent_timestamps_are_current_and_ordered() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("sentts"), json!({})).await;
    let millis = || {
        i64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    };
    let before = millis();
    send_ok(&manager, &url, "first").await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    send_ok(&manager, &url, "second").await;
    let after = millis();

    let got = drain(&manager, &url, json!({"AttributeNames": ["SentTimestamp"]})).await;
    let stamp = |body: &str| -> i64 {
        got.iter().find(|m| m["Body"] == body).unwrap()["Attributes"]["SentTimestamp"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    };
    let (first, second) = (stamp("first"), stamp("second"));
    for value in [first, second] {
        assert!(
            value >= before - 300_000 && value <= after + 300_000,
            "{value} outside [{before}, {after}]"
        );
    }
    assert!(second >= first, "{first} then {second}");
    store.finish().await;
}

/// AWS behavioural: check_set_queue_attributes_invalid_policy_json_rejected.py
#[tokio::test]
async fn given_policy_that_is_not_json_when_set_then_refused_and_a_valid_policy_persists() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("badpolicy"), json!({})).await;

    set_attributes(&manager, &url, json!({"Policy": "{this is not json"}))
        .await
        .assert_refused(
            "InvalidAttributeValue",
            "Invalid value for the parameter Policy.",
        );
    assert!(attributes(&manager, &url, &["Policy"]).await["Policy"].is_null());

    let valid = policy("AllowOwnerSend", "sqs:SendMessage", "*", "000000000000");
    set_attributes(&manager, &url, json!({"Policy": valid}))
        .await
        .succeeded();
    let stored = attributes(&manager, &url, &["Policy"]).await;
    let stored: Value = serde_json::from_str(stored["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(stored["Statement"][0]["Sid"], "AllowOwnerSend");
    store.finish().await;
}

/// AWS behavioural:
/// check_sqsbatch_send_partial_failure_preserves_survivor_attributes.py
#[tokio::test]
async fn given_ten_entry_batch_with_one_empty_body_when_sent_then_survivors_keep_their_attributes()
{
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("survivors"), json!({})).await;
    let entries = (0..10u8)
        .map(|index| {
            json!({"Id": format!("e{index}"),
                "MessageBody": if index == 4 { String::new() } else { format!("body-{index}") },
                "MessageAttributes": {
                    "idx": {"DataType": "Number", "StringValue": index.to_string()},
                    "tag": string_attribute(&format!("tag-{index}")),
                    "blob": binary_attribute(&[index, index.wrapping_mul(7), 255])}})
        })
        .collect::<Vec<_>>();
    let reply = call(
        &manager,
        "SendMessageBatch",
        json!({"QueueUrl": url, "Entries": entries}),
    )
    .await
    .succeeded();

    let successful = reply["Successful"].as_array().unwrap();
    assert_eq!(successful.len(), 9, "{reply}");
    assert!(successful.iter().all(|entry| entry["Id"] != "e4"));
    let digests = successful
        .iter()
        .map(|entry| entry["MD5OfMessageAttributes"].as_str().unwrap().to_owned())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(digests.len(), 9);
    assert!(digests.iter().all(|digest| digest.len() == 32));
    let failed = reply["Failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1, "{reply}");
    assert_eq!(
        (
            failed[0]["Id"].as_str(),
            failed[0]["Code"].as_str(),
            failed[0]["Message"].as_str()
        ),
        (
            Some("e4"),
            Some("EmptyValue"),
            Some("No Value Found for SendMessageBatchRequestEntry.5.MessageBody.")
        )
    );

    let got = drain(&manager, &url, json!({"MessageAttributeNames": ["All"]})).await;
    assert_eq!(got.len(), 9);
    for message in &got {
        let index: u8 = message["Body"]
            .as_str()
            .unwrap()
            .strip_prefix("body-")
            .unwrap()
            .parse()
            .unwrap();
        assert_ne!(index, 4);
        let attributes = &message["MessageAttributes"];
        assert_eq!(attributes["idx"]["StringValue"], index.to_string());
        assert_eq!(attributes["tag"]["StringValue"], format!("tag-{index}"));
        assert_eq!(
            attributes["blob"],
            binary_attribute(&[index, index.wrapping_mul(7), 255])
        );
    }
    store.finish().await;
}

/// AWS behavioural: check_tag_queue_invalid_tag_key_rejected.py,
/// check_tag_queue_system_prefix_and_length_boundaries_rejected.py (the
/// UntagQueue 129-character leg is dropped: aux checks only added keys)
#[tokio::test]
async fn given_invalid_tags_when_tagged_then_refused_with_aws_messages_and_boundaries_are_stored() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(&manager, &unique("tagrules"), json!({})).await;
    let key_message = "Tag keys must be between 1 and 128 characters in length.";
    let refused = [
        ("k".repeat(300), "v".to_owned(), key_message),
        (String::new(), "v".to_owned(), key_message),
        (
            "aws:reserved".to_owned(),
            "v".to_owned(),
            "Caller is an end user and not allowed to mutate system tags",
        ),
        (
            "toolong".to_owned(),
            "v".repeat(257),
            "Tag values must be between 0 and 256 characters in length.",
        ),
    ];
    for (key, value, message) in &refused {
        call(
            &manager,
            "TagQueue",
            json!({"QueueUrl": url, "Tags": {key: value}}),
        )
        .await
        .assert_refused(INVALID, message);
        assert_eq!(tags(&manager, &url).await, Value::Null, "{message}");
    }
    call(
        &manager,
        "UntagQueue",
        json!({"QueueUrl": url, "TagKeys": ["k".repeat(129)]}),
    )
    .await
    .assert_refused(INVALID, key_message);
    let accepted = json!({
        "k".repeat(128): "v",
        "longvalue": "v".repeat(256),
        "empty": "",
        "Environment": "sim2real-test"});
    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": url, "Tags": accepted}),
    )
    .await
    .succeeded();
    assert_eq!(tags(&manager, &url).await, accepted);
    store.finish().await;
}

/// AWS behavioural: check_tag_queue_over_fifty_tags_rejected.py
#[tokio::test]
async fn given_fifty_tags_when_one_more_is_added_then_refused_until_a_key_is_removed() {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let name = unique("fifty");
    let url = create(&manager, &name, json!({})).await;
    let message = format!("Too many tags added for queue {name}.");
    let many = |count: usize| {
        Value::Object(
            (0..count)
                .map(|index| (format!("k{index}"), json!("v")))
                .collect(),
        )
    };

    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": url, "Tags": many(51)}),
    )
    .await
    .assert_refused(INVALID, &message);
    assert_eq!(tags(&manager, &url).await, Value::Null);
    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": url, "Tags": many(50)}),
    )
    .await
    .succeeded();
    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": url, "Tags": {"extra": "x"}}),
    )
    .await
    .assert_refused(INVALID, &message);
    assert_eq!(tags(&manager, &url).await, many(50));

    call(
        &manager,
        "UntagQueue",
        json!({"QueueUrl": url, "TagKeys": ["absent"]}),
    )
    .await
    .succeeded();
    assert_eq!(tags(&manager, &url).await, many(50));
    call(
        &manager,
        "UntagQueue",
        json!({"QueueUrl": url, "TagKeys": ["k1"]}),
    )
    .await
    .succeeded();
    call(
        &manager,
        "TagQueue",
        json!({"QueueUrl": url, "Tags": {"extra": "x"}}),
    )
    .await
    .succeeded();
    let mut want = many(50);
    want.as_object_mut().unwrap().remove("k1");
    want["extra"] = json!("x");
    assert_eq!(tags(&manager, &url).await, want);
    store.finish().await;
}

/// AWS behavioural: check_visibility_timeout_shorten_and_receive_override.py
#[tokio::test]
async fn given_receive_override_when_visibility_is_shortened_then_message_returns_after_the_new_timeout()
 {
    let store = TestStore::new().await;
    let manager = manager(&store);
    let url = create(
        &manager,
        &unique("visshorten"),
        json!({"Attributes": {"VisibilityTimeout": "1"}}),
    )
    .await;
    send_ok(&manager, &url, "hidden").await;

    let got = receive(&manager, &url, json!({"VisibilityTimeout": 30})).await;
    assert_eq!(sorted_bodies(&got), ["hidden"]);
    assert!(receive(&manager, &url, json!({})).await.is_empty());
    let counts = attributes(&manager, &url, &["ApproximateNumberOfMessagesNotVisible"]).await;
    let in_flight: u64 = counts["ApproximateNumberOfMessagesNotVisible"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!(in_flight >= 1, "{counts}");

    let shorten =
        json!({"QueueUrl": url, "ReceiptHandle": got[0]["ReceiptHandle"], "VisibilityTimeout": 2});
    call(&manager, "ChangeMessageVisibility", shorten)
        .await
        .succeeded();
    let again = receive_within_bound(&manager, &url, json!({}), "the shortened message").await;
    assert_eq!(sorted_bodies(&again), ["hidden"]);
    // A receive without an override uses the 1 second queue default.
    let default =
        receive_within_bound(&manager, &url, json!({}), "the default-timeout message").await;
    assert_eq!(sorted_bodies(&default), ["hidden"]);
    store.finish().await;
}
