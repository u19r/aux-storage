//! W3 delivery contract (design `fable-w3-sns-design.md`, section 9.1) on the
//! provider that `STORAGE_TEST_BACKEND` selects. The store clock is the test
//! clock, so claims, leases and retry deadlines move with `advance_time`.
//! Each test also ports a claim test that wrote records with the deleted
//! `put_delivery_record` seam; records now come from a real publish.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use http_request::{
    HttpClient, HttpResponse, StatusCode, Transport, TransportFuture,
    reqwest::{Request, header::HeaderMap},
};
use pubsub_provider::{
    AskMode, ClaimTerms, ClaimedDelivery, DeliveryAttemptResult, DeliveryClaimRequest,
    DeliveryFailureCode, DeliveryPayload, DeliveryRecordId, DeliveryState, DestinationAsk,
    DropReason, LocalDelivery, LocalDeliveryItem, PublishRequest, PubsubMessageId,
    RenderedQueueMessage, SettleInput, SettleOutcome, SubscribeRequest, Subscription,
    SubscriptionProtocol, SweepCursor, TopicArn,
};
use queue_provider::QueueArn;
use storage_types::MessagingIdentity;

use super::{
    pubsub_contract_tests::{create_topic, queue_arn, subscribe},
    test_support::TestStore,
};

pub(super) fn unique(label: &str) -> String {
    format!("{label}-{}", uuid::Uuid::new_v4().simple())
}

pub(super) async fn queue(store: &TestStore, name: &str, attributes: &[(&str, &str)]) -> String {
    let manager = queue::QueueManager::new(store.queue_operations.clone());
    manager
        .create_queue(queue_provider::CreateQueueRequest {
            queue_name: name.to_owned(),
            attributes: (!attributes.is_empty()).then(|| {
                attributes
                    .iter()
                    .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                    .collect()
            }),
            ..Default::default()
        })
        .await
        .unwrap()
        .queue_url
}

async fn receive_all(store: &TestStore, url: &str) -> Vec<queue_provider::MessageResponse> {
    store
        .queue_operations
        .receive_messages(
            url,
            10,
            Some(storage_types::DurationSeconds::from(300)),
            None,
        )
        .await
        .unwrap()
        .messages
}

pub(super) async fn sqs_subscription(
    store: &TestStore,
    topic: &TopicArn,
    queue_name: &str,
    attributes: &[(&str, &str)],
) -> Subscription {
    subscribe(
        store,
        SubscribeRequest {
            topic_arn: topic.clone(),
            protocol: SubscriptionProtocol::Queue,
            endpoint: queue_arn(queue_name),
            attributes: attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            extra_json: serde_json::Value::Null,
        },
    )
    .await
    .unwrap()
}

/// A confirmed HTTPS subscription (its confirmation record is settled).
pub(super) async fn https_subscription(
    store: &TestStore,
    topic: &TopicArn,
    attributes: &[(&str, &str)],
) -> Subscription {
    let pending = subscribe(
        store,
        SubscribeRequest {
            topic_arn: topic.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: format!("https://example.test/{}", unique("hook")),
            attributes: attributes
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
            extra_json: serde_json::Value::Null,
        },
    )
    .await
    .unwrap();
    let token = pending.confirmation.token().unwrap().to_owned();
    store
        .pubsub_provider
        .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
            topic_arn: topic.clone(),
            token,
        })
        .await
        .unwrap();
    for record in claim(store, "confirmations").await {
        let report = settle(store, &record, DeliveryAttemptResult::Delivered, None).await;
        assert_eq!(report.outcome, SettleOutcome::Delivered);
    }
    store
        .pubsub_provider
        .get_subscription(&pending.subscription_arn)
        .await
        .unwrap()
        .unwrap()
}

pub(super) async fn publish(
    store: &TestStore,
    topic: &TopicArn,
    message: &str,
    attributes: HashMap<String, pubsub_provider::MessageAttribute>,
) -> PubsubMessageId {
    let id = PubsubMessageId::new();
    store
        .pubsub_provider
        .accept_publish(
            PublishRequest {
                topic_arn: topic.clone(),
                message: message.to_owned(),
                subject: None,
                message_attributes: attributes,
            },
            id.clone(),
            false,
        )
        .await
        .unwrap();
    id
}

pub(super) async fn materialize(store: &TestStore) -> usize {
    store
        .pubsub_provider
        .materialize_publish_intents(32)
        .await
        .unwrap()
        .records
}

pub(super) async fn claim(store: &TestStore, owner: &str) -> Vec<ClaimedDelivery> {
    claim_at(store, owner, store.now()).await
}

/// Every due destination that a full sweep finds, at most 32 records in
/// all, in one claim (the runner asks the same way from its candidates).
pub(super) async fn claim_at(
    store: &TestStore,
    owner: &str,
    now: storage_types::TimestampMillis,
) -> Vec<ClaimedDelivery> {
    let asks = due_asks(store, now, AskMode::Due, 32).await;
    if asks.is_empty() {
        return Vec::new();
    }
    claim_asks(store, owner, now, asks).await
}

pub(super) async fn claim_asks(
    store: &TestStore,
    owner: &str,
    now: storage_types::TimestampMillis,
    asks: Vec<DestinationAsk>,
) -> Vec<ClaimedDelivery> {
    store
        .pubsub_provider
        .claim_deliveries(DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: owner.to_owned(),
                now,
                lease_until: now + 1_000,
            },
            asks,
        })
        .await
        .unwrap()
        .into_iter()
        .flat_map(|claim| claim.claimed)
        .collect::<Vec<_>>()
        .into_iter()
        .fold(Vec::new(), |mut claims, claimed| {
            claims.push(claimed);
            claims.sort_by(|left: &ClaimedDelivery, right| left.state.id.cmp(&right.state.id));
            claims
        })
}

/// Asks for the due heads of a full sweep: `total` records shared over the
/// due destinations.
pub(super) async fn due_asks(
    store: &TestStore,
    now: storage_types::TimestampMillis,
    mode: AskMode,
    total: u32,
) -> Vec<DestinationAsk> {
    let mut cursor = SweepCursor::from_start(None);
    let mut heads = Vec::new();
    loop {
        let page = store
            .pubsub_provider
            .sweep_destination_heads(cursor, std::num::NonZeroU32::new(256).unwrap())
            .await
            .unwrap();
        heads.extend(page.heads.into_iter().filter(|head| head.head_due <= now));
        let Some(next) = page.next else {
            break;
        };
        cursor = next;
    }
    let count = u32::try_from(heads.len()).unwrap().max(1);
    heads
        .into_iter()
        .take(total as usize)
        .enumerate()
        .filter_map(|(index, head)| {
            let extra = u32::from((index as u32) < total % count);
            let take = std::num::NonZeroU32::new(total / count + extra)?;
            Some(DestinationAsk {
                destination: head.destination,
                take,
                mode,
            })
        })
        .collect()
}

pub(super) async fn settle(
    store: &TestStore,
    claimed: &ClaimedDelivery,
    result: DeliveryAttemptResult,
    dead_letter: Option<RenderedQueueMessage>,
) -> pubsub_provider::SettleReport {
    store
        .pubsub_provider
        .settle_delivery(SettleInput {
            claimed: claimed.state.clone(),
            fence: claimed.fence.clone(),
            message_id: claimed.payload.message_id.clone(),
            result,
            dead_letter,
            jitter: 0,
            identity: storage_types::MessagingIdentity::default(),
        })
        .await
        .unwrap()
}

pub(super) fn local_item(claimed: &ClaimedDelivery, body: &str) -> LocalDeliveryItem {
    LocalDeliveryItem {
        claimed: claimed.state.clone(),
        fence: claimed.fence.clone(),
        message_id: claimed.payload.message_id.clone(),
        message: RenderedQueueMessage {
            body: body.to_owned(),
            attributes: None,
        },
    }
}

async fn deliver_local(
    store: &TestStore,
    claimed: &ClaimedDelivery,
    queue_name: &str,
    body: &str,
) -> pubsub_provider::SettleReport {
    store
        .pubsub_provider
        .deliver_to_local_queue(LocalDelivery {
            queue: QueueArn::parse(&queue_arn(queue_name), &MessagingIdentity::default()).unwrap(),
            items: vec![local_item(claimed, body)],
            jitter: 0,
            identity: storage_types::MessagingIdentity::default(),
        })
        .await
        .unwrap()
        .settled()
        .unwrap()
        .remove(0)
}

pub(super) async fn record(
    store: &TestStore,
    id: &DeliveryRecordId,
) -> Option<(DeliveryState, DeliveryPayload)> {
    store.pubsub_provider.get_delivery(id).await.unwrap()
}

fn record_id(subscription: &Subscription, message: &PubsubMessageId) -> DeliveryRecordId {
    DeliveryRecordId(format!("{}:{message}", subscription.subscription_arn))
}

pub(super) fn rendered(body: &str) -> Option<RenderedQueueMessage> {
    Some(RenderedQueueMessage {
        body: body.to_owned(),
        attributes: None,
    })
}

/// SNS-CLAIM-01 (port): due records are claimed once in (due, id) order;
/// a later publication is not due yet.
#[tokio::test]
async fn given_due_and_future_records_when_claimed_then_once_in_order() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("claim-order"), HashMap::new())
        .await
        .unwrap();
    for label in ["b", "a"] {
        sqs_subscription(&store, &topic.topic_arn, &format!("q-{label}"), &[]).await;
    }
    let due_at = store.now();
    publish(&store, &topic.topic_arn, "due", HashMap::new()).await;
    store.advance_time(Duration::from_secs(5));
    publish(&store, &topic.topic_arn, "future", HashMap::new()).await;
    assert_eq!(materialize(&store).await, 4);
    let first = claim_at(&store, "worker-a", due_at).await;
    assert_eq!(first.len(), 2, "only the first publication is due");
    assert!(
        first[0].state.id.0 < first[1].state.id.0,
        "claim order is (due, id)"
    );
    assert!(
        first
            .iter()
            .all(|record| record.payload.message_body.as_deref() == Some("due"))
    );
    assert!(
        claim_at(&store, "worker-b", due_at).await.is_empty(),
        "claimed once"
    );
    store.finish().await;
}

/// SNS-CLAIM-02 (port): two concurrent claims never take one record twice.
#[tokio::test]
async fn given_ten_records_when_claimed_concurrently_then_never_double_claimed() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("claim-race"), HashMap::new())
        .await
        .unwrap();
    for index in 0..10 {
        sqs_subscription(&store, &topic.topic_arn, &format!("race-{index}"), &[]).await;
    }
    publish(&store, &topic.topic_arn, "race", HashMap::new()).await;
    assert_eq!(materialize(&store).await, 10);
    let (first, second) = tokio::join!(claim(&store, "worker-a"), claim(&store, "worker-b"));
    let unique = first
        .iter()
        .chain(&second)
        .map(|record| record.state.id.0.clone())
        .collect::<HashSet<_>>();
    assert_eq!(first.len() + second.len(), 10);
    assert_eq!(unique.len(), 10);
    store.finish().await;
}

/// W3 tests 4 and 5 with SNS-LEASE-01, SNS-STALE-01 and SNS-ABA-01 (ports):
/// an expired lease is taken over with a new generation; the old holder's
/// local delivery enqueues nothing; the new holder's delivery enqueues once
/// and deletes the record in one transaction; a late settle does not bring
/// the record back; a same-owner ABA settle is rejected.
#[tokio::test]
async fn given_lease_takeover_when_old_holder_delivers_then_the_fence_rejects_it() {
    let store = TestStore::new().await;
    let name = unique("fence");
    let url = queue(&store, &name, &[]).await;
    let topic = create_topic(&store, &name, HashMap::new()).await.unwrap();
    let subscription = sqs_subscription(&store, &topic.topic_arn, &name, &[]).await;
    let message = publish(&store, &topic.topic_arn, "fenced", HashMap::new()).await;
    materialize(&store).await;
    let id = record_id(&subscription, &message);

    let old = claim(&store, "worker-a").await.pop().unwrap();
    assert_eq!(old.state.generation, 1);
    assert!(
        claim(&store, "worker-b").await.is_empty(),
        "the lease holds"
    );
    store.advance_time(Duration::from_secs(2));
    let current = claim(&store, "worker-b").await.pop().unwrap();
    assert_eq!(current.state.generation, 2, "takeover after lease expiry");

    let stale = deliver_local(&store, &old, &name, "stale").await;
    assert_eq!(stale.outcome, SettleOutcome::ClaimLost);
    assert!(
        receive_all(&store, &url).await.is_empty(),
        "the old holder enqueued nothing"
    );
    assert_eq!(
        record(&store, &id).await,
        Some((current.state.clone(), current.payload.clone()))
    );

    let delivered = deliver_local(&store, &current, &name, "current").await;
    assert_eq!(delivered.outcome, SettleOutcome::Delivered);
    assert_eq!(delivered.woken_queue.as_deref(), Some(url.as_str()));
    let received = receive_all(&store, &url).await;
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].body, "current");
    assert_eq!(
        record(&store, &id).await,
        None,
        "the record is gone with the enqueue"
    );
    let late = settle(&store, &old, DeliveryAttemptResult::Delivered, None).await;
    assert_eq!(late.outcome, SettleOutcome::ClaimLost);
    assert_eq!(record(&store, &id).await, None, "no resurrection");

    let message = publish(&store, &topic.topic_arn, "aba", HashMap::new()).await;
    materialize(&store).await;
    let first = claim(&store, "worker-a").await.pop().unwrap();
    store.advance_time(Duration::from_secs(2));
    let second = claim(&store, "worker-a").await.pop().unwrap();
    assert_eq!(
        second.state.lease.as_ref().map(|lease| &lease.owner),
        first.state.lease.as_ref().map(|lease| &lease.owner),
        "same owner"
    );
    let aba = settle(&store, &first, DeliveryAttemptResult::Delivered, None).await;
    assert_eq!(
        aba.outcome,
        SettleOutcome::ClaimLost,
        "the generation fences one owner"
    );
    assert!(
        record(&store, &record_id(&subscription, &message))
            .await
            .is_some()
    );
    let done = settle(&store, &second, DeliveryAttemptResult::Delivered, None).await;
    assert_eq!(done.outcome, SettleOutcome::Delivered);
    store.finish().await;
}

/// W3 test 8: transient failures retry until the retry deadline; the next
/// failure after it moves the notification to the dead-letter queue.
#[tokio::test]
async fn given_transient_failures_when_deadline_passes_then_the_dead_letter_queue_has_it() {
    let store = TestStore::new().await;
    let dlq_name = unique("dlq");
    let dlq = queue(&store, &dlq_name, &[]).await;
    let topic = create_topic(&store, &unique("deadline"), HashMap::new())
        .await
        .unwrap();
    let policy = format!("{{\"deadLetterTargetArn\":\"{}\"}}", queue_arn(&dlq_name));
    let subscription = https_subscription(
        &store,
        &topic.topic_arn,
        &[("RedrivePolicy", policy.as_str())],
    )
    .await;
    let message = publish(&store, &topic.topic_arn, "late", HashMap::new()).await;
    materialize(&store).await;
    let transient = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503));

    let first = claim(&store, "worker").await.pop().unwrap();
    let retried = settle(&store, &first, transient.clone(), rendered("envelope")).await;
    assert_eq!(retried.outcome, SettleOutcome::Retried);
    let stored = record(&store, &record_id(&subscription, &message))
        .await
        .unwrap();
    assert_eq!(
        (
            stored.0.attempts,
            stored
                .0
                .last_failure
                .map(DeliveryFailureCode::stored)
                .as_deref()
        ),
        (1, Some("http_status_503"))
    );

    store.advance_time(Duration::from_secs(61 * 60));
    let second = claim(&store, "worker").await.pop().unwrap();
    let moved = settle(&store, &second, transient, rendered("envelope")).await;
    assert_eq!(moved.outcome, SettleOutcome::DeadLettered);
    assert_eq!(moved.woken_queue.as_deref(), Some(dlq.as_str()));
    let in_dlq = receive_all(&store, &dlq).await;
    assert_eq!(in_dlq.len(), 1);
    assert_eq!(in_dlq[0].body, "envelope");
    assert!(
        in_dlq[0].message_attributes.is_none(),
        "AWS adds no attributes"
    );
    assert_eq!(
        record(&store, &record_id(&subscription, &message)).await,
        None
    );
    store.finish().await;
}

/// W3 tests 9, 10 and 11: a missing dead-letter queue, a dead-letter queue
/// that rejects the message, and a deleted subscription each drop the
/// notification with a counted reason and delete the record.
#[tokio::test]
async fn given_unusable_dead_letter_paths_when_failed_then_each_drop_has_its_reason() {
    let store = TestStore::new().await;
    let small = unique("small-dlq");
    let small_url = queue(&store, &small, &[("MaximumMessageSize", "1024")]).await;
    let topic = create_topic(&store, &unique("drops"), HashMap::new())
        .await
        .unwrap();
    let missing = format!(
        "{{\"deadLetterTargetArn\":\"{}\"}}",
        queue_arn(&unique("absent"))
    );
    let rejecting = format!("{{\"deadLetterTargetArn\":\"{}\"}}", queue_arn(&small));
    let to_missing = https_subscription(
        &store,
        &topic.topic_arn,
        &[("RedrivePolicy", missing.as_str())],
    )
    .await;
    let to_small = https_subscription(
        &store,
        &topic.topic_arn,
        &[("RedrivePolicy", rejecting.as_str())],
    )
    .await;
    let deleted = https_subscription(&store, &topic.topic_arn, &[]).await;
    publish(&store, &topic.topic_arn, "x", HashMap::new()).await;
    materialize(&store).await;
    store
        .pubsub_provider
        .delete_subscription(&deleted.subscription_arn)
        .await
        .unwrap();
    let permanent = DeliveryAttemptResult::Permanent(DeliveryFailureCode::HttpStatus(400));
    let large = "y".repeat(2_000);
    let mut reasons = HashMap::new();
    for claimed in claim(&store, "worker").await {
        let report = settle(&store, &claimed, permanent.clone(), rendered(&large)).await;
        reasons.insert(claimed.state.subscription_arn.clone(), report.outcome);
        assert_eq!(record(&store, &claimed.state.id).await, None);
    }
    assert_eq!(
        reasons[&to_missing.subscription_arn],
        SettleOutcome::Dropped(DropReason::DeadLetterMissing)
    );
    assert_eq!(
        reasons[&to_small.subscription_arn],
        SettleOutcome::Dropped(DropReason::DeadLetterRejected)
    );
    assert_eq!(
        reasons[&deleted.subscription_arn],
        SettleOutcome::Dropped(DropReason::SubscriptionDeleted)
    );
    assert!(receive_all(&store, &small_url).await.is_empty());
    store.finish().await;
}

/// W3 test 12: four subscriptions, two filter policies accept the message:
/// exactly two delivery records are written.
#[tokio::test]
async fn given_filter_policies_when_materialized_then_only_matching_subscriptions_get_records() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("filter"), HashMap::new())
        .await
        .unwrap();
    let red = sqs_subscription(
        &store,
        &topic.topic_arn,
        "f-red",
        &[("FilterPolicy", r#"{"c":["red"]}"#)],
    )
    .await;
    let any = sqs_subscription(&store, &topic.topic_arn, "f-any", &[]).await;
    let blue = sqs_subscription(
        &store,
        &topic.topic_arn,
        "f-blue",
        &[("FilterPolicy", r#"{"c":["blue"]}"#)],
    )
    .await;
    let body = sqs_subscription(
        &store,
        &topic.topic_arn,
        "f-body",
        &[
            ("FilterPolicy", r#"{"c":["red"]}"#),
            ("FilterPolicyScope", "MessageBody"),
        ],
    )
    .await;
    let message = publish(
        &store,
        &topic.topic_arn,
        r#"{"c":"green"}"#,
        HashMap::from([("c".to_owned(), "red".to_owned().into())]),
    )
    .await;
    assert_eq!(
        materialize(&store).await,
        2,
        "two of four subscriptions match"
    );
    for (subscription, expected) in [(&red, true), (&any, true), (&blue, false), (&body, false)] {
        assert_eq!(
            record(&store, &record_id(subscription, &message))
                .await
                .is_some(),
            expected,
            "{}",
            subscription.endpoint
        );
    }
    store.finish().await;
}

/// W3 test 27: an error after the commit of a materialization (an unknown
/// outcome to the caller) does not write the records twice.
#[tokio::test]
async fn given_unknown_materialization_outcome_when_retried_then_records_are_written_once() {
    let probe = Arc::new(AfterCommitFault::default());
    let store = TestStore::new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let topic = create_topic(&store, &unique("unknown"), HashMap::new())
        .await
        .unwrap();
    for index in 0..3 {
        sqs_subscription(&store, &topic.topic_arn, &format!("u-{index}"), &[]).await;
    }
    publish(&store, &topic.topic_arn, "once", HashMap::new()).await;
    // The claim commits first; the chunk commit is the second one.
    probe.fail_commit_number(2);
    assert!(
        store
            .pubsub_provider
            .materialize_publish_intents(32)
            .await
            .unwrap()
            .first_error
            .is_some(),
        "the failed chunk is reported"
    );
    assert!(probe.failed(), "the fault reached the chunk commit");
    assert_eq!(
        materialize(&store).await,
        0,
        "the intent advanced with the records"
    );
    assert_eq!(
        claim(&store, "worker").await.len(),
        3,
        "one record per subscription"
    );
    store.finish().await;
}

/// W3 test 6 (review C5): an error at the commit of the local enqueue is
/// settled as transient. After a commit that applied, the settle finds no
/// record (`ClaimLost`) and the queue holds the message once; after an abort,
/// the record remains and is retried. Never neither, never twice.
#[tokio::test]
async fn given_error_at_local_enqueue_commit_when_settled_then_message_is_once_or_retried() {
    let probe = Arc::new(AfterCommitFault::default());
    let store = TestStore::new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    for applied in [true, false] {
        let name = unique("unknown-enqueue");
        let url = queue(&store, &name, &[]).await;
        let topic = create_topic(&store, &name, HashMap::new()).await.unwrap();
        let subscription = sqs_subscription(&store, &topic.topic_arn, &name, &[]).await;
        let message = publish(&store, &topic.topic_arn, "x", HashMap::new()).await;
        materialize(&store).await;
        let claimed = claim(&store, "worker").await.pop().unwrap();
        probe
            .before
            .store(!applied, std::sync::atomic::Ordering::SeqCst);
        probe.fail_commit_number(1);
        let report = deliver_local(&store, &claimed, &name, "body").await;
        assert!(probe.failed(), "the fault reached the enqueue commit");
        probe.fail_commit_number(0);
        let queued = receive_all(&store, &url).await.len();
        let remaining = record(&store, &record_id(&subscription, &message)).await;
        // Either the message is queued once and the record is gone, or
        // nothing is queued and the record remains for a retry. A driver may
        // also retry an attempt that failed before its commit itself.
        match (queued, remaining) {
            (1, None) => assert!(matches!(
                report.outcome,
                SettleOutcome::ClaimLost | SettleOutcome::Delivered
            )),
            (0, Some(retried)) => {
                assert!(!applied, "a commit that applied left its record");
                assert_eq!(report.outcome, SettleOutcome::Retried);
                assert_eq!(retried.0.attempts, 1);
            }
            (queued, remaining) => panic!(
                "applied={applied}: queued {queued}, record remains {}",
                remaining.is_some()
            ),
        }
        if applied {
            assert_eq!(report.outcome, SettleOutcome::ClaimLost);
        }
    }
    store.finish().await;
}

/// Fails the native call after the Nth commit reached its durable
/// acknowledgement: the commit happened and the caller sees an error.
#[derive(Default)]
struct AfterCommitFault {
    target: std::sync::atomic::AtomicUsize,
    commits: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicBool,
    /// Fail before the commit instead: the attempt aborts.
    before: std::sync::atomic::AtomicBool,
}

impl AfterCommitFault {
    fn fail_commit_number(&self, number: usize) {
        use std::sync::atomic::Ordering;
        self.commits.store(0, Ordering::SeqCst);
        self.failed.store(false, Ordering::SeqCst);
        self.target.store(number, Ordering::SeqCst);
    }

    fn failed(&self) -> bool {
        self.failed.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn at_boundary(&self, before_commit: bool) -> storage_types::StorageResult<()> {
        use std::sync::atomic::Ordering;
        if before_commit != self.before.load(Ordering::SeqCst) {
            return Ok(());
        }
        let number = self.commits.fetch_add(1, Ordering::SeqCst) + 1;
        if number == self.target.load(Ordering::SeqCst) {
            self.failed.store(true, Ordering::SeqCst);
            return Err(storage_types::StorageError::internal(
                "test: outcome after commit",
            ));
        }
        Ok(())
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
impl sql_backend::SqlFaultProbe for AfterCommitFault {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            self.at_boundary(boundary == sql_backend::SqlCommitBoundary::BeforeCommit)
        })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for AfterCommitFault {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(
            async move { self.at_boundary(boundary == kv_backend::KvCommitBoundary::BeforeCommit) },
        )
    }
}

/// Answers each HTTP request with the next status and records the body.
#[derive(Debug, Clone)]
pub(super) struct ScriptedEndpoint {
    statuses: Arc<Mutex<Vec<StatusCode>>>,
    bodies: Arc<Mutex<Vec<String>>>,
}

impl ScriptedEndpoint {
    fn new(statuses: Vec<StatusCode>) -> Self {
        Self {
            statuses: Arc::new(Mutex::new(statuses)),
            bodies: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Transport for ScriptedEndpoint {
    fn send(&self, request: Request) -> TransportFuture {
        let status = self.statuses.lock().unwrap().remove(0);
        let body = request
            .body()
            .and_then(|body| body.as_bytes())
            .unwrap_or_default();
        self.bodies
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(body).into_owned());
        let url = request.url().clone();
        Box::pin(async move {
            Ok(HttpResponse::from_mock(
                status,
                HeaderMap::new(),
                Vec::new(),
                url,
            ))
        })
    }
}

pub(super) fn manager(
    store: &TestStore,
    endpoint: Option<&ScriptedEndpoint>,
) -> (pubsub::PubsubManager, Arc<queue::QueueManager>) {
    let queues = Arc::new(queue::QueueManager::new(store.queue_operations.clone()));
    let mut builder = pubsub::PubsubManager::builder()
        .provider(store.pubsub_provider.clone())
        .queue_wake(queues.wake_handle());
    if let Some(endpoint) = endpoint {
        builder = builder.http_client(
            HttpClient::builder()
                .with_transport(endpoint.clone())
                .build()
                .unwrap(),
        );
    }
    (builder.build().unwrap(), queues)
}

/// W3 test 7: HTTP 400 on a subscription with a dead-letter queue: one
/// request, the dead-letter queue holds the notification envelope with no
/// added attributes, and the record is gone.
#[tokio::test]
async fn given_http_400_with_redrive_policy_when_delivered_then_the_envelope_is_dead_lettered() {
    let store = TestStore::new().await;
    let endpoint = ScriptedEndpoint::new(vec![StatusCode::NO_CONTENT, StatusCode::BAD_REQUEST]);
    let (manager, _) = manager(&store, Some(&endpoint));
    let dlq_name = unique("http-dlq");
    let dlq = queue(&store, &dlq_name, &[]).await;
    let topic = manager.create_topic(unique("http400")).await.unwrap();
    let policy = format!("{{\"deadLetterTargetArn\":\"{}\"}}", queue_arn(&dlq_name));
    manager
        .subscribe(SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: "https://example.test/hook".to_owned(),
            attributes: HashMap::from([("RedrivePolicy".to_owned(), policy)]),
            extra_json: serde_json::Value::Null,
        })
        .await
        .unwrap();
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        1,
        "confirmation"
    );
    let pending = store
        .pubsub_provider
        .list_subscriptions(pubsub_provider::ListSubscriptionsRequest {
            topic_arn: Some(topic.topic_arn.clone()),
            next_token: None,
        })
        .await
        .unwrap()
        .subscriptions
        .remove(0);
    manager
        .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token: pending.confirmation.token().unwrap().to_owned(),
        })
        .await
        .unwrap();
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn.clone(),
            message: "rejected".to_owned(),
            subject: None,
            message_attributes: HashMap::from([("k".to_owned(), "v".to_owned().into())]),
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 1);
    assert_eq!(
        endpoint.bodies.lock().unwrap().len(),
        2,
        "one notification request"
    );
    let in_dlq = receive_all(&store, &dlq).await;
    assert_eq!(in_dlq.len(), 1);
    let envelope: serde_json::Value = serde_json::from_str(&in_dlq[0].body).unwrap();
    assert_eq!(envelope["Type"], "Notification");
    assert_eq!(envelope["Message"], "rejected");
    assert_eq!(envelope["TopicArn"], topic.topic_arn.as_str());
    assert!(
        in_dlq[0].message_attributes.is_none(),
        "no ErrorCode or ErrorMessage"
    );
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "the record is gone"
    );
    store.finish().await;
}

/// W3 tests 28 and 29 through `PubsubManager`: a queue ARN endpoint
/// delivers; a URL or another account is `InvalidParameter`; a missing queue
/// subscribes and its delivery is a counted permanent drop; a raw delivery
/// with 11 attributes is a permanent failure after one attempt.
#[tokio::test]
async fn given_sqs_endpoints_when_subscribed_and_delivered_then_arn_rules_hold() {
    let store = TestStore::new().await;
    let (manager, queues) = manager(&store, None);
    let name = unique("arn");
    let url = queue(&store, &name, &[]).await;
    let raw_name = unique("raw");
    let raw_url = queue(&store, &raw_name, &[]).await;
    let topic = manager.create_topic(unique("arn-topic")).await.unwrap();
    let request = |endpoint: String, raw: bool| SubscribeRequest {
        topic_arn: topic.topic_arn.clone(),
        protocol: SubscriptionProtocol::Queue,
        endpoint,
        attributes: HashMap::from([("RawMessageDelivery".to_owned(), raw.to_string())]),
        extra_json: serde_json::Value::Null,
    };
    for endpoint in [
        url.clone(),
        queue_arn(&name).replace("000000000000", "111111111111"),
    ] {
        let error = manager
            .subscribe(request(endpoint.clone(), false))
            .await
            .unwrap_err();
        assert_eq!(
            error.aws_query_message(),
            "Invalid parameter: SQS endpoint ARN",
            "{endpoint}"
        );
    }
    manager
        .subscribe(request(queue_arn(&name), false))
        .await
        .unwrap();
    manager
        .subscribe(request(queue_arn(&raw_name), true))
        .await
        .unwrap();
    manager
        .subscribe(request(queue_arn(&unique("missing")), false))
        .await
        .expect("a missing queue subscribes, as in AWS");
    let attributes = (0..11)
        .map(|index| (format!("a{index}"), "v".to_owned().into()))
        .collect();
    manager
        .publish(PublishRequest {
            topic_arn: topic.topic_arn.clone(),
            message: "body".to_owned(),
            subject: None,
            message_attributes: attributes,
        })
        .await
        .unwrap();
    assert_eq!(manager.process_due_deliveries(10).await.unwrap(), 3);
    assert_eq!(
        manager.process_due_deliveries(10).await.unwrap(),
        0,
        "no retry of a permanent drop"
    );
    let delivered = queues
        .receive_message(queue_provider::ReceiveMessageRequest {
            queue_url: url,
            max_number_of_messages: Some(10),
            visibility_timeout: Some(300),
            wait_time_seconds: Some(0),
            attribute_names: None,
            message_attribute_names: None,
        })
        .await
        .unwrap()
        .messages;
    assert_eq!(delivered.len(), 1, "the ARN endpoint receives the envelope");
    assert!(
        receive_all(&store, &raw_url).await.is_empty(),
        "11 raw attributes are dropped"
    );
    store.finish().await;
}

/// W3 test 14 with the attribute results of section 14: one validator for
/// `Subscribe` and `SetSubscriptionAttributes`.
#[tokio::test]
async fn given_subscription_attributes_when_set_then_supported_ones_are_stored_and_others_fail() {
    let store = TestStore::new().await;
    let (manager, _) = manager(&store, None);
    let topic = manager.create_topic(unique("attrs")).await.unwrap();
    let request = |attributes: &[(&str, &str)]| SubscribeRequest {
        topic_arn: topic.topic_arn.clone(),
        protocol: SubscriptionProtocol::Queue,
        endpoint: queue_arn("attrs-queue"),
        attributes: attributes
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect(),
        extra_json: serde_json::Value::Null,
    };
    let invalid = |error: pubsub_provider::PubsubError| {
        assert_eq!(
            error.aws_query_error_type(),
            "InvalidParameter",
            "{error:?}"
        );
        error.aws_query_message()
    };
    assert_eq!(
        invalid(
            manager
                .subscribe(request(&[("Foo", "1")]))
                .await
                .unwrap_err()
        ),
        "Invalid parameter: Attributes Reason: Unknown attribute Foo"
    );
    for rejected in [
        ("RedrivePolicy", ""),
        ("RedrivePolicy", r#"{"deadLetterTargetArn":"x","other":1}"#),
        ("FilterPolicy", "not json"),
        ("FilterPolicyScope", "Body"),
        ("RawMessageDelivery", "yes"),
        ("DeliveryPolicy", r#"{"bogus":{}}"#),
    ] {
        invalid(manager.subscribe(request(&[rejected])).await.unwrap_err());
    }
    let missing_dlq = format!(
        "{{\"deadLetterTargetArn\":\"{}\"}}",
        queue_arn("absent-dlq")
    );
    let arn = manager
        .subscribe(request(&[
            ("FilterPolicy", r#"{"c":["red"]}"#),
            ("RedrivePolicy", missing_dlq.as_str()),
            ("DeliveryPolicy", "{}"),
        ]))
        .await
        .unwrap()
        .subscription_arn;
    let arn = pubsub_provider::SubscriptionArn::new(arn).unwrap();
    let attributes = || async {
        manager
            .get_subscription_attributes(pubsub_provider::GetSubscriptionAttributesRequest {
                subscription_arn: arn.clone(),
            })
            .await
            .unwrap()
            .attributes
    };
    let stored = attributes().await;
    assert_eq!(stored["FilterPolicy"], r#"{"c":["red"]}"#);
    assert_eq!(stored["FilterPolicyScope"], "MessageAttributes");
    assert_eq!(stored["RedrivePolicy"], missing_dlq);
    assert_eq!(stored["DeliveryPolicy"], "{}");
    assert_eq!(
        invalid(
            manager
                .subscribe(request(&[("FilterPolicy", r#"{"c":["blue"]}"#)]))
                .await
                .unwrap_err()
        ),
        "Invalid parameter: Attributes Reason: Subscription already exists with different \
         attributes"
    );
    for (name, value) in [("FilterPolicy", ""), ("RedrivePolicy", "{}")] {
        manager
            .set_subscription_attributes(pubsub_provider::SetSubscriptionAttributesRequest {
                subscription_arn: arn.clone(),
                attributes: HashMap::from([(name.to_owned(), value.to_owned())]),
            })
            .await
            .unwrap();
    }
    let cleared = attributes().await;
    assert!(!cleared.contains_key("FilterPolicy"));
    assert!(
        !cleared.contains_key("FilterPolicyScope"),
        "the scope is returned only with a policy"
    );
    assert!(
        !cleared.contains_key("RedrivePolicy"),
        "{{}} removes the policy"
    );
    store.finish().await;
}

/// Review C10 with test 29: a delivery to a queue that does not exist is a
/// permanent `destination_missing` failure, decided in the same transaction
/// as the enqueue attempt: with a dead-letter queue the move commits there;
/// without one the drop is counted with its reason.
#[tokio::test]
async fn given_missing_destination_queue_when_delivered_then_one_transaction_moves_or_drops() {
    let (store, probe) = super::queue_claim_tests::probed_store().await;
    let dlq_name = unique("dm-dlq");
    let dlq = queue(&store, &dlq_name, &[]).await;
    let topic = create_topic(&store, &unique("dm"), HashMap::new())
        .await
        .unwrap();
    let policy = format!("{{\"deadLetterTargetArn\":\"{}\"}}", queue_arn(&dlq_name));
    let missing_with_dlq = unique("dm-missing-a");
    let missing_plain = unique("dm-missing-b");
    let with_dlq = sqs_subscription(
        &store,
        &topic.topic_arn,
        &missing_with_dlq,
        &[("RedrivePolicy", policy.as_str())],
    )
    .await;
    sqs_subscription(&store, &topic.topic_arn, &missing_plain, &[]).await;
    publish(&store, &topic.topic_arn, "nowhere", HashMap::new()).await;
    materialize(&store).await;
    for claimed in claim(&store, "worker").await {
        let has_dlq = claimed.state.subscription_arn == with_dlq.subscription_arn;
        let name = if has_dlq {
            &missing_with_dlq
        } else {
            &missing_plain
        };
        probe.reset();
        let report = deliver_local(&store, &claimed, name, "envelope").await;
        assert_eq!(probe.commits(), 1, "decided in the enqueue transaction");
        assert_eq!(
            report.failure,
            Some(DeliveryFailureCode::DestinationMissing)
        );
        if has_dlq {
            assert_eq!(report.outcome, SettleOutcome::DeadLettered);
            assert_eq!(report.woken_queue.as_deref(), Some(dlq.as_str()));
        } else {
            assert_eq!(
                report.outcome,
                SettleOutcome::Dropped(DropReason::PermanentNoDeadLetter)
            );
        }
        assert_eq!(record(&store, &claimed.state.id).await, None);
    }
    assert_eq!(receive_all(&store, &dlq).await.len(), 1);
    store.finish().await;
}

/// Review D2: an unleased intent is due at its acceptance, so the publish
/// probe (and the age gauge built from it) reports a real age, not the
/// least possible time.
#[tokio::test]
async fn given_unleased_intent_when_probed_then_due_time_is_its_acceptance() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("aged"), HashMap::new())
        .await
        .unwrap();
    let accepted_at = store.now();
    publish(&store, &topic.topic_arn, "aged", HashMap::new()).await;
    store.advance_time(Duration::from_secs(5));
    let due = store
        .pubsub_provider
        .next_publish_due()
        .await
        .unwrap()
        .expect("one intent is due");
    assert_eq!(due, accepted_at);
    assert_eq!(
        store.now().timestamp_millis() - due.timestamp_millis(),
        5_000
    );
    store.finish().await;
}

/// Review D8: a chunk whose commit fails does not keep the other claimed
/// chunks leased; they commit in the same pass and the failure is reported.
#[tokio::test]
async fn given_failed_chunk_commit_when_materialized_then_other_chunks_commit() {
    // An error after the commit (the caller cannot know the outcome); a
    // fault before the commit is retried inside some drivers.
    let probe = Arc::new(AfterCommitFault::default());
    let store = TestStore::new_with_native_probes(
        #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let topic = create_topic(&store, &unique("chunks"), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(&store, &topic.topic_arn, "chunk-q", &[]).await;
    for message in ["one", "two", "three"] {
        publish(&store, &topic.topic_arn, message, HashMap::new()).await;
    }
    // The claim commits first; the first chunk commit is the second one.
    probe.fail_commit_number(2);
    let report = store
        .pubsub_provider
        .materialize_publish_intents(32)
        .await
        .unwrap();
    assert!(probe.failed(), "the fault reached a chunk commit");
    assert_eq!(report.failed_chunks, 1);
    assert_eq!(report.chunks, 2, "the other chunks committed");
    assert_eq!(
        claim(&store, "worker").await.len(),
        3,
        "the failed commit applied; the others were not left leased"
    );
    store.finish().await;
}

pub(super) fn batch_of(
    store: &TestStore,
    queue_name: &str,
    claims: &[ClaimedDelivery],
) -> LocalDelivery {
    let _ = store;
    LocalDelivery {
        queue: QueueArn::parse(&queue_arn(queue_name), &MessagingIdentity::default()).unwrap(),
        items: claims
            .iter()
            .map(|claimed| local_item(claimed, claimed.payload.message_body.as_deref().unwrap()))
            .collect(),
        jitter: 0,
        identity: MessagingIdentity::default(),
    }
}

pub(super) async fn queue_bodies(store: &TestStore, url: &str) -> Vec<String> {
    let mut bodies = Vec::new();
    loop {
        let page = receive_all(store, url).await;
        if page.is_empty() {
            return bodies;
        }
        bodies.extend(page.into_iter().map(|message| message.body));
    }
}

pub(super) async fn publish_batch_records(
    store: &TestStore,
    label: &str,
    bodies: &[String],
) -> (String, String) {
    let queue_name = unique(label);
    let url = queue(store, &queue_name, &[("MaximumMessageSize", "1024")]).await;
    let topic = create_topic(store, &unique(label), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(
        store,
        &topic.topic_arn,
        &queue_name,
        &[("RawMessageDelivery", "true")],
    )
    .await;
    for body in bodies {
        publish(store, &topic.topic_arn, body, HashMap::new()).await;
    }
    let mut written = 0;
    while written < bodies.len() {
        written += materialize(store).await;
    }
    (queue_name, url)
}

/// Review E1: one batch with one message that the queue rejects (over its
/// `MaximumMessageSize`) delivers the other 31 in the same attempt and
/// settles the one as a permanent failure (no dead-letter queue: dropped).
#[tokio::test]
async fn given_batch_with_one_oversized_message_when_delivered_then_31_arrive_and_one_is_permanent()
{
    let store = TestStore::new().await;
    let bodies: Vec<String> = (0..32)
        .map(|index| {
            if index == 7 {
                "x".repeat(2_000)
            } else {
                format!("m-{index}")
            }
        })
        .collect();
    let (queue_name, url) = publish_batch_records(&store, "oversized", &bodies).await;
    let records = claim(&store, "batch").await;
    assert_eq!(records.len(), 32);
    let reports = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &records))
        .await
        .unwrap()
        .settled()
        .unwrap();
    let delivered = reports
        .iter()
        .filter(|report| report.outcome == SettleOutcome::Delivered)
        .count();
    let dropped: Vec<_> = reports
        .iter()
        .filter(|report| matches!(report.outcome, SettleOutcome::Dropped(_)))
        .collect();
    assert_eq!(delivered, 31);
    assert_eq!(dropped.len(), 1);
    assert_eq!(
        dropped[0].outcome,
        SettleOutcome::Dropped(DropReason::PermanentNoDeadLetter)
    );
    assert_eq!(
        dropped[0].failure,
        Some(DeliveryFailureCode::PayloadRejected)
    );
    assert_eq!(queue_bodies(&store, &url).await.len(), 31);
    assert!(claim(&store, "after").await.is_empty(), "no record remains");
    store.finish().await;
}

/// Review E1: another node took over 5 records of a batch (their fence
/// fails): the batch delivers 27 and reports 5 `ClaimLost`; the other node
/// delivers its 5; every message is in the queue exactly once.
#[tokio::test]
async fn given_batch_with_five_records_taken_over_when_delivered_then_27_arrive_and_5_claims_are_lost()
 {
    let store = TestStore::new().await;
    let bodies: Vec<String> = (0..32).map(|index| format!("t-{index}")).collect();
    let (queue_name, url) = publish_batch_records(&store, "takeover", &bodies).await;
    let first = claim(&store, "node-a").await;
    assert_eq!(first.len(), 32);
    store.advance_time(Duration::from_secs(2));
    let now = store.now();
    let asks = due_asks(&store, now, AskMode::Due, 5).await;
    let taken = claim_asks(&store, "node-b", now, asks).await;
    assert_eq!(taken.len(), 5);
    let reports = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &first))
        .await
        .unwrap()
        .settled()
        .unwrap();
    let lost = reports
        .iter()
        .filter(|report| report.outcome == SettleOutcome::ClaimLost)
        .count();
    let delivered = reports
        .iter()
        .filter(|report| report.outcome == SettleOutcome::Delivered)
        .count();
    assert_eq!((delivered, lost), (27, 5));
    let reports = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &taken))
        .await
        .unwrap()
        .settled()
        .unwrap();
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    let mut received = queue_bodies(&store, &url).await;
    received.sort();
    let mut expected = bodies.clone();
    expected.sort();
    assert_eq!(received, expected, "each message once, no duplicate");
    store.finish().await;
}

#[tokio::test]
async fn given_subscription_attribute_change_when_published_then_next_publish_uses_it() {
    let store = TestStore::new().await;
    let queue_name = unique("attr-change");
    queue(&store, &queue_name, &[]).await;
    let topic = create_topic(&store, &unique("attr-change"), HashMap::new())
        .await
        .unwrap();
    let subscription = sqs_subscription(&store, &topic.topic_arn, &queue_name, &[]).await;
    publish(&store, &topic.topic_arn, "first", HashMap::new()).await;
    store
        .pubsub_provider
        .set_subscription_attributes(
            &subscription.subscription_arn,
            pubsub_provider::SubscriptionAttributes::parse(
                &HashMap::from([("RawMessageDelivery".to_owned(), "true".to_owned())]),
                &MessagingIdentity::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    publish(&store, &topic.topic_arn, "second", HashMap::new()).await;
    let mut written = 0;
    while written < 2 {
        written += materialize(&store).await;
    }
    let claims = claim(&store, "attr-change").await;
    assert_eq!(claims.len(), 2);
    let flag_of = |message: &str| {
        let claim = claims
            .iter()
            .find(|claim| claim.payload.message_body.as_deref() == Some(message))
            .unwrap();
        claim
            .payload
            .subscription
            .as_ref()
            .unwrap()
            .raw_message_delivery
    };
    // The publish before the change kept the snapshot it took; the one after
    // took the changed record.
    assert!(!flag_of("first"), "first keeps its snapshot");
    assert!(flag_of("second"), "second sees the change");
    store.finish().await;
}

#[tokio::test]
async fn given_confirmed_subscription_when_topic_attributes_read_then_page_sees_confirmation() {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("confirm-page"), HashMap::new())
        .await
        .unwrap();
    let pending = subscribe(
        &store,
        SubscribeRequest {
            topic_arn: topic.topic_arn.clone(),
            protocol: SubscriptionProtocol::Https,
            endpoint: format!("https://example.test/{}", unique("hook")),
            attributes: HashMap::new(),
            extra_json: serde_json::Value::Null,
        },
    )
    .await
    .unwrap();
    let before = store
        .pubsub_provider
        .get_topic_attributes(pubsub_provider::GetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(
        (
            before["SubscriptionsConfirmed"].as_str(),
            before["SubscriptionsPending"].as_str()
        ),
        ("0", "1")
    );
    store
        .pubsub_provider
        .confirm_subscription(pubsub_provider::ConfirmSubscriptionRequest {
            topic_arn: topic.topic_arn.clone(),
            token: pending.confirmation.token().unwrap().to_owned(),
        })
        .await
        .unwrap();
    let after = store
        .pubsub_provider
        .get_topic_attributes(pubsub_provider::GetTopicAttributesRequest {
            topic_arn: topic.topic_arn.clone(),
        })
        .await
        .unwrap()
        .attributes;
    assert_eq!(
        (
            after["SubscriptionsConfirmed"].as_str(),
            after["SubscriptionsPending"].as_str()
        ),
        ("1", "0")
    );
    store.finish().await;
}
