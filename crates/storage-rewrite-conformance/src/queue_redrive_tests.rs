//! W12 dead-letter queues (design D10, test row 13) on the provider that
//! `STORAGE_TEST_BACKEND` selects. Receives use a zero visibility timeout, so
//! with the fixture clock fixed each receive sees the message due again.

use std::{
    collections::{HashMap, HashSet},
    num::NonZeroU32,
    sync::Arc,
    time::Duration,
};

use queue_provider::{
    MessageId, MessageResponse, Queue, QueueArn, QueueError, QueueValidationKind,
    SendMessageRequest, queue_message_for_send, queue_name_of,
};
use storage_types::DurationSeconds;

use super::{queue_claim_tests::probed_store, test_support::TestStore};

const ACCOUNT_URL: &str = "http://localhost/123456789012";

/// A non-default deployment identity: queue ARNs, redrive targets, and
/// `DeadLetterQueueSourceArn` must all follow it.
fn identity() -> storage_types::MessagingIdentity {
    storage_types::MessagingIdentity {
        partition: "aws".to_owned(),
        region: "eu-central-1".to_owned(),
        account_id: "123456789012".to_owned(),
    }
}

fn arn(url: &str) -> String {
    QueueArn::of(&identity(), url.rsplit('/').next().unwrap()).to_string()
}

fn policy(target_url: &str, max_receive_count: &str) -> String {
    format!(
        "{{\"deadLetterTargetArn\":\"{}\",\"maxReceiveCount\":{max_receive_count}}}",
        arn(target_url)
    )
}

async fn create(
    store: &TestStore,
    name: &str,
    attributes: &[(&str, String)],
) -> Result<String, QueueError> {
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("{ACCOUNT_URL}/{name}"),
            queue_name: name.to_owned(),
            attributes: attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .map(|queue| queue.queue_url)
}

fn unique(label: &str) -> String {
    format!("{label}_{}", uuid::Uuid::new_v4().simple())
}

async fn send(store: &TestStore, url: &str, body: &str) -> String {
    store
        .queue_operations
        .send_message(queue_message_for_send(
            SendMessageRequest {
                queue_url: url.to_owned(),
                message_body: body.to_owned(),
                delay_seconds: None,
                message_attributes: None,
                ..Default::default()
            },
            store.now(),
        ))
        .await
        .expect("send message")
        .to_string()
}

/// One receive whose lease ends at once (the message is due again).
async fn receive(store: &TestStore, url: &str, max: u32) -> Vec<MessageResponse> {
    store
        .queue_operations
        .receive_messages(url, max, Some(DurationSeconds::from(0)), None)
        .await
        .expect("receive")
        .messages
}

/// Every live row of a queue, from the bounded counts, without a receive.
async fn message_total(store: &TestStore, url: &str) -> u64 {
    let (_, counts) = store
        .queue_operations
        .get_queue_with_message_counts(url)
        .await
        .unwrap()
        .expect("queue exists");
    counts.visible + counts.not_visible + counts.delayed
}

/// A stable-identity publication (`PublishMessage`): the same `MessageId`
/// can be in more than one queue.
async fn publish(store: &TestStore, url: &str, id: MessageId, body: &str) {
    let mut message = queue_message_for_send(
        SendMessageRequest {
            queue_url: url.to_owned(),
            message_body: body.to_owned(),
            delay_seconds: None,
            message_attributes: None,
            ..Default::default()
        },
        store.now(),
    );
    message.message_id = id;
    store
        .queue_operations
        .publish_message(message)
        .await
        .expect("publish message");
}

fn attribute<'a>(message: &'a MessageResponse, name: &str) -> Option<&'a str> {
    message.attributes.as_ref()?.get(name).map(String::as_str)
}

async fn stored_policy(store: &TestStore, url: &str) -> Option<String> {
    store
        .queue_operations
        .get_queue(url)
        .await
        .unwrap()
        .unwrap()
        .attributes
        .get("RedrivePolicy")
        .cloned()
}

/// Row 13: with `maxReceiveCount = 2` the third receive is empty and the
/// dead-letter queue holds the same message with its source ARN and count.
#[tokio::test]
async fn given_max_receive_count_two_when_received_three_times_then_the_message_moves() {
    let store = TestStore::new().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "2"))],
    )
    .await
    .unwrap();
    let mut message = queue_message_for_send(
        SendMessageRequest {
            queue_url: source.clone(),
            message_body: "poison".to_owned(),
            ..Default::default()
        },
        store.now(),
    );
    // W14 item 06: a dead-letter move keeps the trace header.
    message.trace_header = Some("Root=1-moved".to_owned());
    let id = store
        .queue_operations
        .send_message(message)
        .await
        .expect("send message")
        .to_string();
    for count in ["1", "2"] {
        let received = receive(&store, &source, 1).await;
        assert_eq!(received.len(), 1);
        assert_eq!(
            attribute(&received[0], "ApproximateReceiveCount"),
            Some(count)
        );
    }
    let third = store
        .queue_operations
        .receive_messages(&source, 1, Some(DurationSeconds::from(0)), None)
        .await
        .unwrap();
    assert!(
        third.messages.is_empty(),
        "the moved message is not returned"
    );
    assert_eq!(third.moved_to.as_deref(), Some(queue_name_of(&dlq)));
    assert!(receive(&store, &source, 10).await.is_empty());

    let moved = receive(&store, &dlq, 10).await;
    assert_eq!(moved.len(), 1);
    assert_eq!(
        (moved[0].message_id.as_str(), moved[0].body.as_str()),
        (id.as_str(), "poison")
    );
    assert_eq!(
        attribute(&moved[0], "DeadLetterQueueSourceArn"),
        Some(arn(&source).as_str())
    );
    assert_eq!(
        attribute(&moved[0], "ApproximateReceiveCount"),
        Some("3"),
        "SQS keeps the receive count on a move"
    );
    assert_eq!(attribute(&moved[0], "AWSTraceHeader"), Some("Root=1-moved"));
    store.finish().await;
}

/// A missing target leaves the message in the source and its count keeps
/// increasing; a recreated target receives the next move.
#[tokio::test]
async fn given_missing_then_recreated_target_when_received_then_delivery_continues_then_moves() {
    let store = TestStore::new().await;
    let dlq_name = unique("dlq");
    let dlq = create(&store, &dlq_name, &[]).await.unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    send(&store, &source, "kept").await;
    store.queue_operations.delete_queue(&dlq).await.unwrap();
    for count in ["1", "2", "3"] {
        let received = receive(&store, &source, 1).await;
        assert_eq!(received.len(), 1, "a missing target delivers as usual");
        assert_eq!(
            attribute(&received[0], "ApproximateReceiveCount"),
            Some(count)
        );
    }

    let recreated = create(&store, &dlq_name, &[]).await.unwrap();
    assert!(receive(&store, &source, 1).await.is_empty());
    let moved = receive(&store, &recreated, 1).await;
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].body, "kept");
    assert_eq!(attribute(&moved[0], "ApproximateReceiveCount"), Some("4"));
    store.finish().await;
}

/// A queue whose policy names itself delivers as usual (SQS accepts its own
/// ARN). A chain moves each hop with the ARN of the queue it left.
#[tokio::test]
async fn given_own_arn_and_a_chain_when_received_then_self_delivers_and_each_hop_moves() {
    let store = TestStore::new().await;
    let own_name = unique("own");
    let own_url = format!("{ACCOUNT_URL}/{own_name}");
    let own = create(
        &store,
        &own_name,
        &[("RedrivePolicy", policy(&own_url, "1"))],
    )
    .await
    .unwrap();
    send(&store, &own, "loop").await;
    for _ in 0..3 {
        assert_eq!(receive(&store, &own, 1).await.len(), 1);
    }

    let last = create(&store, &unique("c"), &[]).await.unwrap();
    let middle = create(
        &store,
        &unique("b"),
        &[("RedrivePolicy", policy(&last, "2"))],
    )
    .await
    .unwrap();
    let first = create(
        &store,
        &unique("a"),
        &[("RedrivePolicy", policy(&middle, "1"))],
    )
    .await
    .unwrap();
    send(&store, &first, "hop").await;
    assert_eq!(receive(&store, &first, 1).await.len(), 1);
    assert!(
        receive(&store, &first, 1).await.is_empty(),
        "moved to the middle"
    );
    let in_middle = receive(&store, &middle, 1).await;
    assert_eq!(in_middle.len(), 1);
    assert_eq!(
        attribute(&in_middle[0], "ApproximateReceiveCount"),
        Some("2")
    );
    assert_eq!(
        attribute(&in_middle[0], "DeadLetterQueueSourceArn"),
        Some(arn(&first).as_str())
    );
    assert!(
        receive(&store, &middle, 1).await.is_empty(),
        "moved to the last"
    );
    let in_last = receive(&store, &last, 1).await;
    assert_eq!(in_last.len(), 1);
    assert_eq!(attribute(&in_last[0], "ApproximateReceiveCount"), Some("3"));
    assert_eq!(
        attribute(&in_last[0], "DeadLetterQueueSourceArn"),
        Some(arn(&middle).as_str())
    );
    store.finish().await;
}

/// `ListDeadLetterSourceQueues` returns the queues whose policy names the
/// target; a removed policy no longer counts; a missing target is an error.
#[tokio::test]
async fn given_sources_of_a_target_when_listed_then_exactly_those_queues_are_returned() {
    let store = TestStore::new().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let other = create(&store, &unique("other"), &[]).await.unwrap();
    let mut expected = Vec::new();
    for label in ["s1", "s2"] {
        expected.push(
            create(
                &store,
                &unique(label),
                &[("RedrivePolicy", policy(&dlq, "3"))],
            )
            .await
            .unwrap(),
        );
    }
    let removed = create(
        &store,
        &unique("s3"),
        &[("RedrivePolicy", policy(&dlq, "3"))],
    )
    .await
    .unwrap();
    create(
        &store,
        &unique("s4"),
        &[("RedrivePolicy", policy(&other, "3"))],
    )
    .await
    .unwrap();
    store
        .queue_operations
        .set_queue_attributes(
            &removed,
            HashMap::from([("RedrivePolicy".to_owned(), String::new())]),
        )
        .await
        .unwrap();
    assert_eq!(stored_policy(&store, &removed).await, None);

    let listed = store
        .queue_operations
        .list_dead_letter_source_queues(&dlq, None, NonZeroU32::new(1_000).unwrap())
        .await
        .unwrap()
        .expect("target exists");
    expected.sort();
    // The provider reports names; the manager makes URLs.
    let expected = expected
        .iter()
        .map(|url| queue_name_of(url).to_owned())
        .collect::<Vec<_>>();
    assert_eq!(listed.queue_urls, expected, "name order");
    let mut paged = Vec::new();
    let mut token = None;
    for _ in 0..20 {
        let page = store
            .queue_operations
            .list_dead_letter_source_queues(&dlq, token.as_deref(), NonZeroU32::MIN)
            .await
            .unwrap()
            .expect("target exists");
        assert!(page.queue_urls.len() <= 1);
        paged.extend(page.queue_urls);
        token = page.next_token;
        if token.is_none() {
            break;
        }
    }
    assert_eq!(token, None, "paging ends");
    assert_eq!(paged, expected, "pages of one give the same list");
    let missing = format!("{ACCOUNT_URL}/{}", unique("missing"));
    assert!(
        store
            .queue_operations
            .list_dead_letter_source_queues(&missing, None, NonZeroU32::MIN)
            .await
            .unwrap()
            .is_none()
    );
    store.finish().await;
}

/// Validation on create and set: shape, count bounds, a missing target; a
/// numeric string is stored as a number. The partition, account, and region
/// rule is the queue manager's (it owns the messaging identity); W14-I1
/// below proves it.
#[tokio::test]
async fn given_redrive_policies_when_created_or_set_then_invalid_ones_fail_and_valid_ones_normalize()
 {
    let store = TestStore::new().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let source = create(&store, &unique("src"), &[]).await.unwrap();
    let set = |value: String| {
        let store = &store;
        let source = source.clone();
        async move {
            store
                .queue_operations
                .set_queue_attributes(
                    &source,
                    HashMap::from([("RedrivePolicy".to_owned(), value)]),
                )
                .await
        }
    };
    let dlq_arn = arn(&dlq);
    let invalid = [
        "not json".to_owned(),
        "{}".to_owned(),
        policy(&dlq, "0"),
        policy(&dlq, "1001"),
        policy(&dlq, "\"abc\""),
        format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":1,\"x\":1}}"),
        policy(&format!("{ACCOUNT_URL}/{}", unique("absent")), "1"),
    ];
    for value in invalid {
        let error = set(value.clone()).await.expect_err(&value);
        assert!(
            matches!(
                error,
                QueueError::Validation {
                    // Probe 9: a value that is not JSON is
                    // `InvalidAttributeValue`.
                    kind: QueueValidationKind::InvalidParameterValue
                        | QueueValidationKind::InvalidAttributeValue,
                    ..
                }
            ),
            "{value}: {error:?}"
        );
    }
    let missing_on_create = create(
        &store,
        &unique("src"),
        &[(
            "RedrivePolicy",
            policy(&format!("{ACCOUNT_URL}/{}", unique("absent")), "1"),
        )],
    )
    .await;
    assert!(missing_on_create.is_err(), "create checks the target too");

    set(format!(
        "{{\"maxReceiveCount\":\"5\",\"deadLetterTargetArn\":\"{dlq_arn}\"}}"
    ))
    .await
    .unwrap();
    assert_eq!(
        stored_policy(&store, &source).await.as_deref(),
        Some(format!("{{\"deadLetterTargetArn\":\"{dlq_arn}\",\"maxReceiveCount\":5}}").as_str())
    );
    let queue = store
        .queue_operations
        .get_queue(&dlq)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        QueueArn::of(&identity(), &queue.queue_name).to_string(),
        dlq_arn
    );
    store.finish().await;
}

/// Row 13 under load: 16 receivers drain 100 messages with
/// `maxReceiveCount = 1`. Each message is delivered once and then moved once:
/// at the end each ID is in the dead-letter queue exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_16_receivers_when_messages_redrive_then_each_id_moves_exactly_once() {
    let store = Arc::new(TestStore::new().await);
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    let mut sent = HashSet::new();
    for index in 0..100 {
        sent.insert(send(&store, &source, &format!("m{index}")).await);
    }
    let receivers = (0..16)
        .map(|_| {
            let store = Arc::clone(&store);
            let source = source.clone();
            tokio::spawn(async move {
                let mut delivered = Vec::new();
                let mut empty = 0;
                while empty < 3 {
                    let received = receive(&store, &source, 2).await;
                    if received.is_empty() {
                        empty += 1;
                    } else {
                        empty = 0;
                        delivered.extend(received.into_iter().map(|message| message.message_id));
                    }
                }
                delivered
            })
        })
        .collect::<Vec<_>>();
    let mut deliveries = HashMap::<String, usize>::new();
    for receiver in receivers {
        for id in tokio::time::timeout(Duration::from_secs(10), receiver)
            .await
            .expect("receivers exceeded the test budget")
            .unwrap()
        {
            *deliveries.entry(id).or_default() += 1;
        }
    }
    assert!(
        deliveries.values().all(|count| *count == 1),
        "a message was delivered from the source twice"
    );
    // Counts before any further receive: a receive of the source would move
    // a leftover message and hide it.
    assert_eq!(
        message_total(&store, &source).await,
        0,
        "the source is empty"
    );
    assert_eq!(
        message_total(&store, &dlq).await,
        100,
        "the dead-letter queue holds one row for each message"
    );
    let mut moved = HashSet::new();
    loop {
        let received = store
            .queue_operations
            .receive_messages(&dlq, 10, Some(DurationSeconds::from(300)), None)
            .await
            .unwrap()
            .messages;
        if received.is_empty() {
            break;
        }
        moved.extend(received.into_iter().map(|message| message.message_id));
    }
    assert_eq!(
        moved, sent,
        "each ID is in the dead-letter queue exactly once"
    );
    Arc::into_inner(store).unwrap().finish().await;
}

/// Native calls of one receive: on a queue with no policy, with a policy and
/// no move (the target catalog row rides in the due wave), and with a move
/// (one batched read of the target IDs and one delete batch).
#[tokio::test]
async fn given_policy_and_move_when_received_then_native_calls_grow_only_with_the_move() {
    let (store, probe) = probed_store().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let plain = create(&store, &unique("plain"), &[]).await.unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    send(&store, &plain, "plain").await;
    send(&store, &source, "redrive").await;
    let mut calls = Vec::new();
    let mut point_reads = Vec::new();
    for (url, expect_delivered) in [(&plain, true), (&source, true), (&source, false)] {
        probe.reset();
        assert_eq!(!receive(&store, url, 1).await.is_empty(), expect_delivered);
        assert_eq!(probe.claims(), 1, "one claim attempt");
        calls.push(probe.calls());
        point_reads.push(probe.point_reads());
    }
    eprintln!(
        "redrive native calls: backend={} no_policy={} policy_no_move={} move={}",
        TestStore::selected_backend(),
        calls[0],
        calls[1],
        calls[2]
    );
    assert!(calls[1] <= calls[0] + 1, "{calls:?}");
    assert!(calls[2] <= calls[1] + 3, "{calls:?}");
    assert_eq!(
        point_reads[1], point_reads[0],
        "a policy with no move reads no target IDs: {point_reads:?}"
    );
    // A move adds one batched read of the target IDs (`load_held`). On KV
    // the source delete also reads its keys with one more batched get; SQL
    // guards the delete in its statement.
    let delete_read = usize::from(matches!(
        TestStore::selected_backend().as_str(),
        "rocksdb" | "foundationdb"
    ));
    assert_eq!(
        point_reads[2],
        point_reads[1] + 1 + delete_read,
        "a move reads the target IDs in one batch: {point_reads:?}"
    );
    store.finish().await;
}

/// Review A1: two queues that are each other's dead-letter queue with
/// `maxReceiveCount = 1`. Each hop delivers once before the next move, so the
/// message is delivered on alternate receives, and a receive that delivers
/// signals no target (no wake loop between the two queues' waiters).
#[tokio::test]
async fn given_a_redrive_cycle_when_received_alternately_then_each_hop_delivers_once() {
    let store = TestStore::new().await;
    let first = create(&store, &unique("cycle_a"), &[]).await.unwrap();
    let second = create(
        &store,
        &unique("cycle_b"),
        &[("RedrivePolicy", policy(&first, "1"))],
    )
    .await
    .unwrap();
    store
        .queue_operations
        .set_queue_attributes(
            &first,
            HashMap::from([("RedrivePolicy".to_owned(), policy(&second, "1"))]),
        )
        .await
        .unwrap();
    let id = send(&store, &first, "cycle").await;
    let mut queue = first.clone();
    for hop in 0..4_u32 {
        let delivered = store
            .queue_operations
            .receive_messages(&queue, 1, Some(DurationSeconds::from(0)), None)
            .await
            .unwrap();
        assert_eq!(delivered.messages.len(), 1, "hop {hop} delivers");
        assert_eq!(delivered.messages[0].message_id, id);
        assert_eq!(
            attribute(&delivered.messages[0], "ApproximateReceiveCount"),
            Some((hop + 1).to_string().as_str())
        );
        assert_eq!(delivered.moved_to, None, "a delivery wakes no queue");
        let next = if queue == first { &second } else { &first };
        let moved = store
            .queue_operations
            .receive_messages(&queue, 1, Some(DurationSeconds::from(0)), None)
            .await
            .unwrap();
        assert!(moved.messages.is_empty(), "hop {hop} moves");
        assert_eq!(moved.moved_to.as_deref(), Some(queue_name_of(next)));
        queue = next.clone();
    }
    store.finish().await;
}

/// Review A1: in a chain whose second `maxReceiveCount` (1) is below the kept
/// count (3), the second queue still delivers the message once before it
/// moves it on.
#[tokio::test]
async fn given_a_chain_below_the_kept_count_when_received_then_each_hop_delivers() {
    let store = TestStore::new().await;
    let last = create(&store, &unique("chain_c"), &[]).await.unwrap();
    let middle = create(
        &store,
        &unique("chain_b"),
        &[("RedrivePolicy", policy(&last, "1"))],
    )
    .await
    .unwrap();
    let first = create(
        &store,
        &unique("chain_a"),
        &[("RedrivePolicy", policy(&middle, "3"))],
    )
    .await
    .unwrap();
    send(&store, &first, "chain").await;
    for _ in 0..3 {
        assert_eq!(receive(&store, &first, 1).await.len(), 1);
    }
    assert!(receive(&store, &first, 1).await.is_empty(), "moved on");
    let in_middle = receive(&store, &middle, 1).await;
    assert_eq!(in_middle.len(), 1, "the middle delivers once");
    assert_eq!(
        attribute(&in_middle[0], "ApproximateReceiveCount"),
        Some("4")
    );
    assert!(receive(&store, &middle, 1).await.is_empty(), "moved on");
    let in_last = receive(&store, &last, 1).await;
    assert_eq!(in_last.len(), 1);
    assert_eq!(attribute(&in_last[0], "ApproximateReceiveCount"), Some("5"));
    store.finish().await;
}

/// Review A4: a stable-identity publication puts the same `MessageId` in the
/// target. The exhausted source copy is delivered as usual, and the source
/// keeps working for its other messages.
#[tokio::test]
async fn given_target_holds_the_id_when_exhausted_then_the_source_delivers_it() {
    let store = TestStore::new().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    let id = MessageId::random();
    publish(&store, &dlq, id, "same").await;
    publish(&store, &source, id, "same").await;
    for count in ["1", "2", "3"] {
        let received = receive(&store, &source, 1).await;
        assert_eq!(received.len(), 1, "the held message is delivered");
        assert_eq!(
            attribute(&received[0], "ApproximateReceiveCount"),
            Some(count)
        );
    }
    let other = send(&store, &source, "other").await;
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.extend(
            receive(&store, &source, 10)
                .await
                .into_iter()
                .map(|message| message.message_id),
        );
    }
    assert!(seen.contains(&other), "the source is not blocked: {seen:?}");
    let in_target = receive(&store, &dlq, 10).await;
    assert_eq!(in_target.len(), 2, "the published copy and the moved other");
    assert!(
        in_target
            .iter()
            .any(|message| message.message_id == id.to_string()
                && attribute(message, "DeadLetterQueueSourceArn").is_none()),
        "the target keeps its own copy"
    );
    store.finish().await;
}

/// Review A4: a target row with the same `MessageId` that is past its
/// retention, and that cleanup has not removed, is not held: the move
/// replaces it.
#[tokio::test]
async fn given_expired_target_copy_when_exhausted_then_the_move_replaces_it() {
    let store = TestStore::new().await;
    let dlq = create(
        &store,
        &unique("dlq"),
        &[("MessageRetentionPeriod", "60".to_owned())],
    )
    .await
    .unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    let id = MessageId::random();
    publish(&store, &dlq, id, "same").await;
    store.advance_time(Duration::from_secs(61));
    publish(&store, &source, id, "same").await;
    assert_eq!(receive(&store, &source, 1).await.len(), 1);
    let exhausted = store
        .queue_operations
        .receive_messages(&source, 1, Some(DurationSeconds::from(0)), None)
        .await
        .unwrap();
    assert!(exhausted.messages.is_empty(), "the message moves");
    assert_eq!(exhausted.moved_to.as_deref(), Some(queue_name_of(&dlq)));
    let moved = receive(&store, &dlq, 10).await;
    assert_eq!(moved.len(), 1);
    assert_eq!(moved[0].message_id, id.to_string());
    assert_eq!(
        attribute(&moved[0], "DeadLetterQueueSourceArn"),
        Some(arn(&source).as_str())
    );
    store.finish().await;
}

/// Review A6: a message older than the target's retention is deleted from the
/// source and not written to the target (the Drop decision).
#[tokio::test]
async fn given_message_past_target_retention_when_exhausted_then_only_the_source_row_is_deleted() {
    let store = TestStore::new().await;
    let dlq = create(
        &store,
        &unique("dlq"),
        &[("MessageRetentionPeriod", "60".to_owned())],
    )
    .await
    .unwrap();
    let source = create(
        &store,
        &unique("src"),
        &[("RedrivePolicy", policy(&dlq, "1"))],
    )
    .await
    .unwrap();
    send(&store, &source, "old").await;
    store.advance_time(Duration::from_secs(61));
    assert_eq!(receive(&store, &source, 1).await.len(), 1);
    let dropped = store
        .queue_operations
        .receive_messages(&source, 1, Some(DurationSeconds::from(0)), None)
        .await
        .unwrap();
    assert!(dropped.messages.is_empty());
    assert_eq!(dropped.moved_to, None, "nothing was written to the target");
    assert_eq!(message_total(&store, &source).await, 0);
    assert_eq!(message_total(&store, &dlq).await, 0);
    store.finish().await;
}

/// Review A5: `CreateQueue` with the same attributes returns the existing
/// queue after its dead-letter target was deleted.
#[tokio::test]
async fn given_deleted_target_when_identical_queue_created_again_then_the_existing_queue_returns() {
    let store = TestStore::new().await;
    let dlq = create(&store, &unique("dlq"), &[]).await.unwrap();
    let name = unique("src");
    let attributes = [("RedrivePolicy", policy(&dlq, "2"))];
    let source = create(&store, &name, &attributes).await.unwrap();
    store.queue_operations.delete_queue(&dlq).await.unwrap();
    assert_eq!(create(&store, &name, &attributes).await.unwrap(), source);
    let other = create(&store, &unique("src"), &attributes).await;
    assert!(
        matches!(
            other,
            Err(QueueError::Validation {
                kind: QueueValidationKind::InvalidParameterValue,
                ..
            })
        ),
        "a new queue still checks the target: {other:?}"
    );
    store.finish().await;
}

/// W14-I1 and probe 9: a dead-letter target outside the source account or
/// region is rejected with the AWS reason; the account rule comes first.
#[tokio::test]
async fn given_target_in_other_region_or_account_when_policy_is_set_then_aws_reason_is_returned() {
    let store = TestStore::new().await;
    let manager = queue::QueueManager::new(store.queue_operations.clone())
        .with_messaging_identity(identity());
    let source = create(&store, &unique("src"), &[]).await.unwrap();
    let dlq_name = unique("dlq");
    create(&store, &dlq_name, &[]).await.unwrap();

    for (target, reason) in [
        (
            format!("arn:aws:sqs:us-east-1:123456789012:{dlq_name}"),
            "Dead-letter target must be in same region as the source.",
        ),
        (
            format!("arn:aws:sqs:eu-central-1:999999999999:{dlq_name}"),
            "Dead-letter target owner should be same as the source.",
        ),
        (
            format!("arn:aws:sqs:us-east-1:999999999999:{dlq_name}"),
            "Dead-letter target owner should be same as the source.",
        ),
    ] {
        let policy = format!("{{\"deadLetterTargetArn\":\"{target}\",\"maxReceiveCount\":3}}");
        let error = manager
            .set_queue_attributes(queue_provider::SetQueueAttributesRequest {
                queue_url: source.clone(),
                attributes: HashMap::from([("RedrivePolicy".to_owned(), policy.clone())]),
            })
            .await
            .unwrap_err();
        assert_eq!(error.aws_query_error_type(), "InvalidParameterValue");
        assert_eq!(
            error.aws_query_message(),
            format!("Value {policy} for parameter RedrivePolicy is invalid. Reason: {reason}")
        );
    }
    store.finish().await;
}
