//! AWS emulator behavioural suite ports.
//!
//! Multi-step SQS flows through the replacement store: visibility, receive
//! counts, delays, dead-letter redrive, purge, deletion. Every wait is an
//! advance of the store's test clock. Expected values are AWS's.

use std::{collections::HashMap, num::NonZeroU32, time::Duration};

use queue_provider::{
    MessageAttributeValue, MessageResponse, Queue, QueueArn, QueueError, ReceiptHandle,
    SendMessageRequest, queue_message_for_send,
};
use storage_types::DurationSeconds;

use super::test_support::TestStore;

const ACCOUNT_URL: &str = "http://localhost/123456789012";

fn unique(label: &str) -> String {
    format!("{label}_{}", uuid::Uuid::new_v4().simple())
}

async fn create(store: &TestStore, label: &str, attributes: &[(&str, String)]) -> String {
    create_named(store, &unique(label), attributes).await
}

async fn create_named(store: &TestStore, name: &str, attributes: &[(&str, String)]) -> String {
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("{ACCOUNT_URL}/{name}"),
            queue_name: name.to_owned(),
            attributes: attributes
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .expect("create queue")
        .queue_url
}

async fn send_with(
    store: &TestStore,
    url: &str,
    body: &str,
    delay_seconds: Option<i64>,
    message_attributes: Option<HashMap<String, MessageAttributeValue>>,
) -> String {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: url.to_owned(),
                message_body: body.to_owned(),
                delay_seconds,
                message_attributes,
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message")
        .to_string()
}

async fn send(store: &TestStore, url: &str, body: &str) -> String {
    send_with(store, url, body, None, None).await
}

async fn receive(store: &TestStore, url: &str, max: u32, visibility: u32) -> Vec<MessageResponse> {
    store
        .queue_operations
        .receive_messages(
            url,
            max,
            Some(DurationSeconds::from(visibility)),
            Some(DurationSeconds::from(0)),
        )
        .await
        .expect("receive")
        .messages
}

fn handle(message: &MessageResponse) -> ReceiptHandle {
    ReceiptHandle::from(message.receipt_handle.as_str())
}

fn attribute<'a>(message: &'a MessageResponse, name: &str) -> Option<&'a str> {
    message.attributes.as_ref()?.get(name).map(String::as_str)
}

fn bodies(messages: &[MessageResponse]) -> Vec<&str> {
    let mut bodies = messages
        .iter()
        .map(|message| message.body.as_str())
        .collect::<Vec<_>>();
    bodies.sort_unstable();
    bodies
}

/// `(visible, not visible, delayed)`, the three approximate counters.
async fn counters(store: &TestStore, url: &str) -> (u64, u64, u64) {
    let (_, counts) = store
        .queue_operations
        .get_queue_with_message_counts(url)
        .await
        .expect("counts")
        .expect("queue exists");
    (counts.visible, counts.not_visible, counts.delayed)
}

async fn set_visibility(store: &TestStore, url: &str, message: &MessageResponse, seconds: u32) {
    store
        .queue_operations
        .change_message_visibility(url, handle(message), DurationSeconds::from(seconds))
        .await
        .expect("change visibility");
}

fn is_receipt_handle_invalid(error: &QueueError) -> bool {
    error.aws_query_error_type() == "ReceiptHandleIsInvalid"
}

fn dlq_policy(target_url: &str, max_receive_count: u32) -> String {
    let identity = storage_types::MessagingIdentity {
        partition: "aws".to_owned(),
        region: "eu-central-1".to_owned(),
        account_id: "123456789012".to_owned(),
    };
    format!(
        "{{\"deadLetterTargetArn\":\"{}\",\"maxReceiveCount\":{max_receive_count}}}",
        QueueArn::of(&identity, target_url.rsplit('/').next().unwrap())
    )
}

/// AWS behavioural:
/// check_approximate_counters_converge_through_receive_release_and_delete.py
#[tokio::test]
async fn given_five_messages_when_received_released_and_deleted_then_counters_follow_each_step() {
    let store = TestStore::new().await;
    let url = create(
        &store,
        "counters",
        &[("VisibilityTimeout", "600".to_owned())],
    )
    .await;
    assert_eq!(counters(&store, &url).await, (0, 0, 0));
    let mut sent = Vec::new();
    for index in 0..5 {
        sent.push(format!("count-{index}"));
        send(&store, &url, &sent[index]).await;
    }
    assert_eq!(counters(&store, &url).await, (5, 0, 0));

    let held = receive(&store, &url, 3, 600).await;
    assert_eq!(held.len(), 3);
    assert_eq!(counters(&store, &url).await, (2, 3, 0));
    set_visibility(&store, &url, &held[0], 0).await;
    assert_eq!(counters(&store, &url).await, (3, 2, 0));
    store
        .queue_operations
        .delete_message(&url, handle(&held[1]))
        .await
        .expect("delete first in flight");
    assert_eq!(counters(&store, &url).await, (3, 1, 0));
    store
        .queue_operations
        .delete_message(&url, handle(&held[2]))
        .await
        .expect("delete second in flight");

    let mut seen = vec![held[1].body.clone(), held[2].body.clone()];
    for message in receive(&store, &url, 10, 600).await {
        store
            .queue_operations
            .delete_message(&url, handle(&message))
            .await
            .expect("delete drained");
        seen.push(message.body);
    }
    seen.sort_unstable();
    assert_eq!(seen, sent, "the drain sees exactly the five bodies");
    assert_eq!(counters(&store, &url).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural: check_approximate_receive_count_increments.py,
/// check_receive_count_and_first_receive_timestamp_invariants_across_redeliveries.py
///
/// `ApproximateFirstReceiveTimestamp` is never produced (writing/sqs_parity.md,
/// behavioural difference 13), so its assertions are left out.
#[tokio::test]
async fn given_message_when_redelivered_after_expiry_then_receive_count_rises_and_sent_timestamp_holds()
 {
    let store = TestStore::new().await;
    let url = create(&store, "count_expiry", &[]).await;
    send(&store, &url, "counted").await;
    let mut sent_timestamps = Vec::new();
    for count in ["1", "2", "3"] {
        let received = receive(&store, &url, 1, 2).await;
        assert_eq!(received.len(), 1, "delivery {count}");
        assert_eq!(
            attribute(&received[0], "ApproximateReceiveCount"),
            Some(count)
        );
        let sent_at = attribute(&received[0], "SentTimestamp").map(str::to_owned);
        assert_eq!(sent_at.as_deref().map(str::len), Some(13), "13 digits");
        sent_timestamps.push(sent_at);
        assert!(receive(&store, &url, 1, 2).await.is_empty(), "hidden");
        store.advance_time(Duration::from_secs(3));
    }
    assert!(sent_timestamps.windows(2).all(|pair| pair[0] == pair[1]));
    store.finish().await;
}

/// AWS behavioural: check_receive_count_increments.py
#[tokio::test]
async fn given_message_when_released_with_visibility_zero_then_receive_count_goes_one_then_two() {
    let store = TestStore::new().await;
    let url = create(&store, "count_release", &[]).await;
    send(&store, &url, "released").await;
    let first = receive(&store, &url, 1, 30).await;
    assert_eq!(attribute(&first[0], "ApproximateReceiveCount"), Some("1"));
    set_visibility(&store, &url, &first[0], 0).await;
    let second = receive(&store, &url, 1, 30).await;
    assert_eq!(second.len(), 1);
    assert_eq!(attribute(&second[0], "ApproximateReceiveCount"), Some("2"));
    assert_ne!(second[0].receipt_handle, first[0].receipt_handle);
    store.finish().await;
}

/// AWS behavioural: check_change_message_visibility_batch.py
#[tokio::test]
async fn given_two_in_flight_messages_when_batch_sets_zero_then_both_are_receivable_at_once() {
    let store = TestStore::new().await;
    let url = create(&store, "batch_zero", &[]).await;
    send(&store, &url, "a").await;
    send(&store, &url, "b").await;
    let held = receive(&store, &url, 2, 600).await;
    assert_eq!(held.len(), 2);
    assert!(receive(&store, &url, 2, 600).await.is_empty());
    let results = store
        .queue_operations
        .change_message_visibilities(
            &url,
            held.iter()
                .map(|message| (handle(message), DurationSeconds::from(0)))
                .collect(),
        )
        .await
        .expect("batch");
    assert!(results.iter().all(Result::is_ok), "no Failed entries");
    assert_eq!(bodies(&receive(&store, &url, 2, 600).await), ["a", "b"]);
    store.finish().await;
}

/// AWS behavioural: check_change_message_visibility_batch_partial.py,
/// check_change_message_visibility_batch_partial_failure.py
///
/// AWS's text for the failed entry is "The input receipt handle is invalid."
/// (aux's differs, see writing/sqs_parity.md); the entry code is asserted.
#[tokio::test]
async fn given_batch_with_one_bogus_handle_when_changed_then_it_fails_alone_and_timeouts_hold() {
    let store = TestStore::new().await;
    let url = create(&store, "batch_partial", &[]).await;
    send(&store, &url, "extended").await;
    send(&store, &url, "plain").await;
    let held = receive(&store, &url, 2, 4).await;
    assert_eq!(held.len(), 2);
    let extended = held.iter().find(|m| m.body == "extended").unwrap();
    let results = store
        .queue_operations
        .change_message_visibilities(
            &url,
            vec![
                (handle(extended), DurationSeconds::from(20)),
                (ReceiptHandle::from("bad"), DurationSeconds::from(20)),
            ],
        )
        .await
        .expect("batch");
    assert!(results[0].is_ok());
    assert!(is_receipt_handle_invalid(results[1].as_ref().unwrap_err()));

    store.advance_time(Duration::from_secs(5));
    assert_eq!(
        bodies(&receive(&store, &url, 10, 600).await),
        ["plain"],
        "the failed entry's message reappears at its original timeout"
    );
    store.advance_time(Duration::from_secs(10));
    assert!(receive(&store, &url, 10, 600).await.is_empty());
    store.advance_time(Duration::from_secs(6));
    assert_eq!(bodies(&receive(&store, &url, 10, 600).await), ["extended"]);
    store.finish().await;
}

/// AWS behavioural:
/// check_change_message_visibility_batch_repeated_receipt_handle_last_entry_wins.
/// py
///
/// The script does not grade which timeout wins; only the absence of failures.
#[tokio::test]
async fn given_batch_entries_sharing_one_handle_when_changed_then_no_entry_fails_in_either_order() {
    let store = TestStore::new().await;
    for timeouts in [[60, 0], [0, 60]] {
        let url = create(&store, "batch_repeated", &[]).await;
        send(&store, &url, "shared").await;
        let held = receive(&store, &url, 1, 600).await;
        let results = store
            .queue_operations
            .change_message_visibilities(
                &url,
                timeouts
                    .iter()
                    .map(|seconds| (handle(&held[0]), DurationSeconds::from(*seconds)))
                    .collect(),
            )
            .await
            .expect("batch");
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(Result::is_ok), "{timeouts:?}");
    }
    store.finish().await;
}

/// AWS behavioural: check_change_message_visibility_extends.py,
/// check_change_message_visibility_extends_timeout.py
#[tokio::test]
async fn given_in_flight_message_when_visibility_is_extended_then_it_reappears_at_the_new_deadline()
{
    let store = TestStore::new().await;
    let url = create(&store, "extend", &[]).await;
    send(&store, &url, "extended").await;
    let held = receive(&store, &url, 1, 2).await;
    set_visibility(&store, &url, &held[0], 10).await;
    store.advance_time(Duration::from_secs(3));
    assert!(receive(&store, &url, 1, 30).await.is_empty(), "past 2 s");
    store.advance_time(Duration::from_secs(5));
    assert!(receive(&store, &url, 1, 30).await.is_empty(), "before 9 s");
    // 11 s: the 10 s deadline plus the clock-skew margin of due rows.
    store.advance_time(Duration::from_secs(3));
    let again = receive(&store, &url, 1, 30).await;
    assert_eq!(bodies(&again), ["extended"], "at or after 9 s");
    assert_eq!(attribute(&again[0], "ApproximateReceiveCount"), Some("2"));
    store.finish().await;
}

/// AWS behavioural:
/// check_delayed_messages_are_counted_separately_from_visible_and_in_flight.py
#[tokio::test]
async fn given_delayed_messages_when_counted_then_delayed_is_separate_and_purge_zeroes_all() {
    let store = TestStore::new().await;
    let url = create(&store, "delayed", &[]).await;
    send(&store, &url, "control").await;
    send_with(&store, &url, "short", Some(15), None).await;
    send_with(&store, &url, "long", Some(300), None).await;
    assert_eq!(bodies(&receive(&store, &url, 10, 600).await), ["control"]);
    assert_eq!(counters(&store, &url).await, (0, 1, 2));

    store.advance_time(Duration::from_secs(16));
    assert_eq!(bodies(&receive(&store, &url, 10, 600).await), ["short"]);
    assert_eq!(
        counters(&store, &url).await,
        (0, 2, 1),
        "only the 300 s message is still delayed"
    );
    store
        .queue_operations
        .purge_queue(&url)
        .await
        .expect("purge");
    assert_eq!(counters(&store, &url).await, (0, 0, 0));
    store.advance_time(Duration::from_secs(400));
    assert!(receive(&store, &url, 10, 600).await.is_empty());
    store.finish().await;
}

/// AWS behavioural: check_delete_then_empty.py
#[tokio::test]
async fn given_two_received_messages_when_one_is_deleted_then_only_the_sibling_reappears() {
    let store = TestStore::new().await;
    let url = create(&store, "delete_then_empty", &[]).await;
    send(&store, &url, "deleted").await;
    send(&store, &url, "sibling").await;
    let held = receive(&store, &url, 2, 3).await;
    assert_eq!(held.len(), 2);
    let deleted = held.iter().find(|m| m.body == "deleted").unwrap();
    store
        .queue_operations
        .delete_message(&url, handle(deleted))
        .await
        .expect("delete");
    for _ in 0..4 {
        store.advance_time(Duration::from_secs(4));
        assert_eq!(bodies(&receive(&store, &url, 10, 3).await), ["sibling"]);
    }
    store.finish().await;
}

/// AWS behavioural:
/// check_dlq_move_withheld_until_the_maxreceivecount_th_delivery.py
#[tokio::test]
async fn given_max_receive_count_three_when_received_then_move_waits_for_the_fourth_receive() {
    let store = TestStore::new().await;
    let dlq = create(&store, "dlq_three", &[]).await;
    let source = create(
        &store,
        "src_three",
        &[("RedrivePolicy", dlq_policy(&dlq, 3))],
    )
    .await;
    send(&store, &source, "poison-body").await;
    for count in ["1", "2", "3"] {
        assert_eq!(counters(&store, &dlq).await, (0, 0, 0), "before {count}");
        let received = receive(&store, &source, 1, 0).await;
        assert_eq!(received.len(), 1);
        assert_eq!(
            attribute(&received[0], "ApproximateReceiveCount"),
            Some(count)
        );
    }
    assert_eq!(counters(&store, &dlq).await, (0, 0, 0), "not moved yet");
    assert!(receive(&store, &source, 1, 0).await.is_empty());
    let moved = receive(&store, &dlq, 10, 30).await;
    assert_eq!(bodies(&moved), ["poison-body"]);
    assert_eq!(counters(&store, &source).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural:
/// check_dlq_moves_only_the_undeleted_message_after_four_deliveries.py
#[tokio::test]
async fn given_max_receive_count_four_when_one_message_is_deleted_then_only_the_other_moves() {
    let store = TestStore::new().await;
    let dlq = create(&store, "dlq_four", &[]).await;
    let source = create(
        &store,
        "src_four",
        &[("RedrivePolicy", dlq_policy(&dlq, 4))],
    )
    .await;
    let attributes = HashMap::from([(
        "kind".to_owned(),
        MessageAttributeValue {
            string_value: Some("order-token".to_owned()),
            binary_value: None,
            data_type: "String".to_owned(),
        },
    )]);
    send(&store, &source, "healthy").await;
    let poison_id = send_with(&store, &source, "poison", None, Some(attributes)).await;
    let mut poison_counts = Vec::new();
    for delivery in 1..=4 {
        let received = receive(&store, &source, 10, 0).await;
        if delivery == 2 {
            let healthy = received.iter().find(|m| m.body == "healthy").unwrap();
            store
                .queue_operations
                .delete_message(&source, handle(healthy))
                .await
                .expect("delete healthy");
        }
        let poison = received.iter().find(|m| m.body == "poison").unwrap();
        poison_counts.push(
            attribute(poison, "ApproximateReceiveCount")
                .unwrap()
                .to_owned(),
        );
    }
    assert_eq!(poison_counts, ["1", "2", "3", "4"]);
    assert!(receive(&store, &source, 10, 0).await.is_empty());
    let moved = receive(&store, &dlq, 10, 30).await;
    assert_eq!(moved.len(), 1, "the DLQ holds only the undeleted message");
    assert_eq!(
        (moved[0].message_id.as_str(), moved[0].body.as_str()),
        (poison_id.as_str(), "poison")
    );
    let kind = moved[0]
        .message_attributes
        .as_ref()
        .unwrap()
        .get("kind")
        .unwrap();
    assert_eq!(kind.data_type, "String");
    assert_eq!(kind.string_value.as_deref(), Some("order-token"));
    store.finish().await;
}

/// AWS behavioural:
/// check_message_retention_period_expires_undeleted_message.py,
/// check_server_side_message_retention_auto_expunge.py
#[tokio::test]
async fn given_short_retention_when_clock_passes_it_then_message_expires_and_control_queue_keeps_it()
 {
    let store = TestStore::new().await;
    let short = create(
        &store,
        "retention_short",
        &[("MessageRetentionPeriod", "60".to_owned())],
    )
    .await;
    let control = create(&store, "retention_control", &[]).await;
    for url in [&short, &control] {
        send(&store, url, "undeleted").await;
        assert_eq!(counters(&store, url).await, (1, 0, 0));
    }
    store.advance_time(Duration::from_secs(61));
    assert_eq!(counters(&store, &short).await, (0, 0, 0));
    assert!(receive(&store, &short, 10, 30).await.is_empty());
    assert_eq!(
        bodies(&receive(&store, &control, 10, 30).await),
        ["undeleted"]
    );
    store.finish().await;
}

/// AWS behavioural: check_retention_expiry_discards_an_in_flight_message.py
#[tokio::test]
async fn given_in_flight_message_when_retention_ends_first_then_it_never_returns() {
    let store = TestStore::new().await;
    let short = create(
        &store,
        "inflight_short",
        &[("MessageRetentionPeriod", "60".to_owned())],
    )
    .await;
    let twin = create(&store, "inflight_twin", &[]).await;
    for url in [&short, &twin] {
        send(&store, url, "in-flight").await;
        assert_eq!(receive(&store, url, 1, 90).await.len(), 1);
        assert_eq!(counters(&store, url).await, (0, 1, 0));
    }
    store.advance_time(Duration::from_secs(91));
    assert!(receive(&store, &short, 10, 30).await.is_empty());
    assert_eq!(counters(&store, &short).await, (0, 0, 0));
    let again = receive(&store, &twin, 10, 30).await;
    assert_eq!(bodies(&again), ["in-flight"]);
    assert_eq!(attribute(&again[0], "ApproximateReceiveCount"), Some("2"));
    store.advance_time(Duration::from_secs(120));
    assert!(
        receive(&store, &short, 10, 30).await.is_empty(),
        "late receives"
    );
    store.finish().await;
}

/// AWS behavioural: check_purge_queue_discards_in_flight_messages.py,
/// check_purge_queue_removes_all_messages.py
#[tokio::test]
async fn given_in_flight_and_visible_messages_when_purged_then_none_return_and_counters_read_zero()
{
    let store = TestStore::new().await;
    let url = create(&store, "purge_flow", &[]).await;
    for index in 0..6 {
        send(&store, &url, &format!("purge-{index}")).await;
    }
    let held = receive(&store, &url, 3, 90).await;
    assert_eq!(held.len(), 3);
    assert_eq!(counters(&store, &url).await, (3, 3, 0));
    store
        .queue_operations
        .purge_queue(&url)
        .await
        .expect("purge");
    assert!(receive(&store, &url, 10, 30).await.is_empty());
    assert_eq!(counters(&store, &url).await, (0, 0, 0));
    store.advance_time(Duration::from_secs(91));
    assert!(
        receive(&store, &url, 10, 30).await.is_empty(),
        "no in-flight body reappears"
    );
    assert_eq!(counters(&store, &url).await, (0, 0, 0));
    store.finish().await;
}

/// AWS behavioural:
/// check_queue_visibility_timeout_change_spares_in_flight_messages.py
#[tokio::test]
async fn given_in_flight_message_when_queue_timeout_is_lowered_then_it_keeps_its_original_deadline()
{
    let store = TestStore::new().await;
    let url = create(
        &store,
        "lower_timeout",
        &[("VisibilityTimeout", "35".to_owned())],
    )
    .await;
    send(&store, &url, "old").await;
    let queue_default = store
        .queue_operations
        .receive_messages(&url, 1, None, Some(DurationSeconds::from(0)))
        .await
        .expect("receive with the queue default")
        .messages;
    assert_eq!(queue_default.len(), 1);
    store
        .queue_operations
        .set_queue_attributes(
            &url,
            HashMap::from([("VisibilityTimeout".to_owned(), "2".to_owned())]),
        )
        .await
        .expect("lower the timeout");
    send(&store, &url, "new").await;
    let new = store
        .queue_operations
        .receive_messages(&url, 10, None, Some(DurationSeconds::from(0)))
        .await
        .expect("receive with the new default")
        .messages;
    assert_eq!(bodies(&new), ["new"]);
    store.advance_time(Duration::from_secs(3));
    assert_eq!(
        bodies(
            &store
                .queue_operations
                .receive_messages(&url, 10, None, Some(DurationSeconds::from(0)))
                .await
                .unwrap()
                .messages
        ),
        ["new"],
        "later receives use 2 s; the old message is still hidden"
    );
    store.advance_time(Duration::from_secs(33));
    let old = receive(&store, &url, 10, 30).await;
    assert!(
        old.iter().any(|m| m.body == "old"),
        "old reappears after its 35 s"
    );
    store.finish().await;
}

/// AWS behavioural:
/// check_receipt_handle_from_a_deleted_queue_incarnation_rejected.py
#[tokio::test]
async fn given_handle_of_deleted_queue_when_same_name_recreated_then_handle_is_refused() {
    let store = TestStore::new().await;
    let name = unique("incarnation");
    let old_url = create_named(&store, &name, &[]).await;
    send(&store, &old_url, "old-generation").await;
    let old = receive(&store, &old_url, 1, 600).await;
    store
        .queue_operations
        .delete_queue(&old_url)
        .await
        .expect("delete queue");

    let new_url = create_named(&store, &name, &[]).await;
    send(&store, &new_url, "new-generation").await;
    let delete = store
        .queue_operations
        .delete_message(&new_url, handle(&old[0]))
        .await
        .unwrap_err();
    assert_eq!(
        delete.aws_query_error_type(),
        "AWS.SimpleQueueService.NonExistentQueue"
    );
    assert_eq!(
        delete.aws_query_message(),
        "The specified queue does not exist."
    );
    let change = store
        .queue_operations
        .change_message_visibility(&new_url, handle(&old[0]), DurationSeconds::from(30))
        .await
        .unwrap_err();
    assert_eq!(
        change.aws_query_error_type(),
        "AWS.SimpleQueueService.NonExistentQueue"
    );
    assert_eq!(
        change.aws_query_message(),
        "The specified queue does not exist."
    );
    assert_eq!(
        bodies(&receive(&store, &new_url, 10, 30).await),
        ["new-generation"]
    );
    store.finish().await;
}

/// AWS behavioural: check_receipt_handle_from_another_queue_rejected.py
#[tokio::test]
async fn given_handle_of_another_queue_when_deleted_then_refused_and_message_redelivers() {
    let store = TestStore::new().await;
    let queue_a = create(&store, "handle_a", &[]).await;
    let queue_b = create(&store, "handle_b", &[]).await;
    let id = send(&store, &queue_a, "queue-a-body").await;
    let held = receive(&store, &queue_a, 1, 2).await;
    let foreign = held[0].receipt_handle.clone();
    let error = store
        .queue_operations
        .delete_message(&queue_b, handle(&held[0]))
        .await
        .unwrap_err();
    assert_eq!(error.aws_query_error_type(), "ReceiptHandleIsInvalid");
    assert_eq!(
        error.aws_query_message(),
        format!("The receipt handle \"{foreign}\" is not valid for this queue.")
    );
    store.advance_time(Duration::from_secs(3));
    let again = receive(&store, &queue_a, 1, 30).await;
    assert_eq!(
        (again[0].message_id.as_str(), again[0].body.as_str()),
        (id.as_str(), "queue-a-body")
    );
    assert_eq!(attribute(&again[0], "ApproximateReceiveCount"), Some("2"));
    store
        .queue_operations
        .delete_message(&queue_a, handle(&again[0]))
        .await
        .expect("delete on A");
    store.advance_time(Duration::from_secs(31));
    assert!(receive(&store, &queue_a, 1, 30).await.is_empty());
    assert_eq!(counters(&store, &queue_b).await, (0, 0, 0));
    store.finish().await;
}

async fn listed(store: &TestStore, dlq: &str) -> Vec<String> {
    store
        .queue_operations
        .list_dead_letter_source_queues(dlq, None, NonZeroU32::new(1_000).unwrap())
        .await
        .unwrap()
        .expect("target exists")
        .queue_urls
}

/// AWS behavioural: check_redrive_policy_cleared_by_empty_string_unlinks_dlq.py
#[tokio::test]
async fn given_two_linked_sources_when_policy_cleared_and_set_again_then_dlq_listing_follows() {
    let store = TestStore::new().await;
    let dlq = create(&store, "unlink_dlq", &[]).await;
    let source = create(
        &store,
        "unlink_a",
        &[("RedrivePolicy", dlq_policy(&dlq, 3))],
    )
    .await;
    let sibling = create(
        &store,
        "unlink_b",
        &[("RedrivePolicy", dlq_policy(&dlq, 3))],
    )
    .await;
    let name = |url: &str| url.rsplit('/').next().unwrap().to_owned();
    let mut both = vec![name(&source), name(&sibling)];
    both.sort();
    assert_eq!(listed(&store, &dlq).await, both);

    store
        .queue_operations
        .set_queue_attributes(
            &source,
            HashMap::from([("RedrivePolicy".to_owned(), String::new())]),
        )
        .await
        .expect("clear");
    let cleared = store
        .queue_operations
        .get_queue(&source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cleared.attributes.get("RedrivePolicy"), None);
    let kept = store
        .queue_operations
        .get_queue(&sibling)
        .await
        .unwrap()
        .unwrap();
    assert!(kept.attributes.contains_key("RedrivePolicy"));
    assert_eq!(listed(&store, &dlq).await, [name(&sibling)]);

    store
        .queue_operations
        .set_queue_attributes(
            &source,
            HashMap::from([("RedrivePolicy".to_owned(), dlq_policy(&dlq, 3))]),
        )
        .await
        .expect("relink");
    assert_eq!(listed(&store, &dlq).await, both);
    store.finish().await;
}

/// AWS behavioural:
/// check_visibility_heartbeat_hides_a_message_from_concurrent_pollers.py
#[tokio::test]
async fn given_heartbeat_every_two_seconds_when_pollers_receive_then_held_message_is_never_delivered()
 {
    let store = TestStore::new().await;
    let url = create(&store, "heartbeat", &[]).await;
    send(&store, &url, "heartbeated").await;
    let held = receive(&store, &url, 1, 5).await;
    assert_eq!(bodies(&held), ["heartbeated"]);
    send(&store, &url, "unheartbeated").await;
    let mut polled = Vec::new();
    for _ in 0..12 {
        store.advance_time(Duration::from_secs(2));
        set_visibility(&store, &url, &held[0], 10).await;
        polled.extend(receive(&store, &url, 10, 1).await);
    }
    assert!(polled.iter().all(|m| m.body == "unheartbeated"));
    assert!(!polled.is_empty(), "the unheartbeated sibling is delivered");
    store
        .queue_operations
        .delete_message(&url, handle(&held[0]))
        .await
        .expect("delete with the original handle");
    store.advance_time(Duration::from_secs(12));
    let late = receive(&store, &url, 10, 30).await;
    assert!(late.iter().all(|m| m.body != "heartbeated"));
    store.finish().await;
}
