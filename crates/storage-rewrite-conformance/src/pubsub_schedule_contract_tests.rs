//! W3S delivery schedule contract (plan section 6) on the provider that
//! `STORAGE_TEST_BACKEND` selects: the write bound of a dead destination for
//! a simulated hour with explicit times, the deadline bypass, the takeover
//! of a crashed owner, the sweep, and claims that write no payload.

use std::{collections::HashMap, num::NonZeroU32, time::Duration};

use pubsub::{
    DeliveryRunnerConfig,
    delivery_scheduler::{BreakerSignal, Completed, DeliveryScheduler, SchedulerConfig},
};
use pubsub_provider::{
    AskMode, ClaimTerms, DeliveryAttemptResult, DeliveryClaimRequest, DeliveryFailureCode,
    DestinationAsk, DestinationKey, DropReason, PublishRequest, PubsubMessageId, SettleOutcome,
    SweepCursor, TopicArn,
};
use storage_types::TimestampMillis;

use super::{
    pubsub_contract_tests::create_topic,
    pubsub_delivery_contract_tests::{
        claim_asks, https_subscription, materialize, settle, sqs_subscription, unique,
    },
    queue_claim_tests::{ClaimProbe, probed_store},
    test_support::TestStore,
};

const HOUR_MILLIS: i64 = 3_600_000;
/// The measured writes of `dead_destination_hour` (see the W3S plan 6 (i)).
const DEAD_HOUR_WRITES: usize = 648;

/// `subscriptions` HTTPS subscriptions on one host (one destination) and
/// `publishes` messages of `body`: `subscriptions x publishes` records.
pub(super) async fn http_backlog(
    store: &TestStore,
    subscriptions: usize,
    publishes: usize,
    body: &str,
) -> DestinationKey {
    let topic = create_topic(store, &unique("dead"), HashMap::new())
        .await
        .unwrap();
    let mut destination = None;
    for _ in 0..subscriptions {
        let subscription = https_subscription(store, &topic.topic_arn, &[]).await;
        destination = Some(DestinationKey::of_endpoint(
            subscription.protocol,
            &subscription.endpoint,
        ));
    }
    publish_all(store, &topic.topic_arn, publishes, body).await;
    destination.unwrap()
}

pub(super) async fn publish_all(store: &TestStore, topic: &TopicArn, publishes: usize, body: &str) {
    for _ in 0..publishes {
        store
            .pubsub_provider
            .accept_publish(
                PublishRequest {
                    topic_arn: topic.clone(),
                    message: body.to_owned(),
                    subject: None,
                    message_attributes: HashMap::new(),
                },
                PubsubMessageId::new(),
                false,
            )
            .await
            .unwrap();
    }
    while materialize(store).await > 0 {}
}

pub(super) fn ask(destination: &DestinationKey, take: u32, mode: AskMode) -> Vec<DestinationAsk> {
    vec![DestinationAsk {
        destination: destination.clone(),
        take: NonZeroU32::new(take).unwrap(),
        mode,
    }]
}

/// Move the store clock to `at` (it only moves forward).
fn clock_to(store: &TestStore, at: TimestampMillis) {
    let behind = at.timestamp_millis() - store.now().timestamp_millis();
    if behind > 0 {
        store.advance_time(Duration::from_millis(behind as u64));
    }
}

/// One node of the simulated hour: its scheduling core and its owner name.
struct Node {
    owner: String,
    starts_at: TimestampMillis,
    core: DeliveryScheduler,
}

/// What one node does at `now`, as its runner would: a due sweep page, then
/// claims while the core asks, and between claims the completion of one
/// record at a time (every attempt fails at once), so a slot that a failure
/// returns can be refilled at once if the core allows it.
async fn run_node(store: &TestStore, node: &mut Node, now: TimestampMillis) {
    if let Some((cursor, limit)) = node.core.sweep_request(now) {
        let page = store
            .pubsub_provider
            .sweep_destination_heads(cursor, limit)
            .await
            .unwrap();
        node.core.on_sweep(&page, now);
    }
    let mut pending = std::collections::VecDeque::new();
    loop {
        if let Some(asks) = node.core.next_claim(now, 0) {
            let claims = store
                .pubsub_provider
                .claim_deliveries(DeliveryClaimRequest {
                    terms: ClaimTerms {
                        owner: node.owner.clone(),
                        now,
                        lease_until: now + 60_000,
                    },
                    asks,
                })
                .await
                .unwrap();
            node.core.on_claimed(&claims, now);
            pending.extend(claims.into_iter().flat_map(|claim| claim.claimed));
            continue;
        }
        let Some(record) = pending.pop_front() else {
            return;
        };
        let failure = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpConnect);
        let report = settle(store, &record, failure, None).await;
        node.core.on_completed(
            Completed {
                destination: record.state.destination.clone(),
                records: 1,
                signal: BreakerSignal::Failed,
                retry_due: report.retry_due,
                unsettled_until: None,
                // The shortest open interval (-20%): the most probes.
                jitter: 0,
            },
            now,
        );
    }
}

/// State row writes of 12 nodes whose scheduling cores claim and fail a
/// dead destination for one simulated hour against the real store. Node `n`
/// starts at `n x 100 ms`; every node wakes when its core asks to. The
/// sweep interval is 30 s (the write count does not depend on it: an open
/// breaker is not asked, and a timer wakes a closed one). The hour ends
/// 10 s before the records' retry deadline, so no deadline bypass falls in
/// it; every claim, a `DeadlineOnly` one too, is counted.
async fn dead_destination_hour(store: &TestStore, probe: &ClaimProbe, records: usize) -> usize {
    http_backlog(store, records / 100, 100, "dead").await;
    let config = SchedulerConfig::from(&DeliveryRunnerConfig {
        sweep_interval: Duration::from_secs(30),
        ..DeliveryRunnerConfig::default()
    });
    let start = store.now();
    let end = start + (HOUR_MILLIS - 10_000);
    let mut nodes = (0..12_i64)
        .map(|node| Node {
            owner: format!("node-{node}"),
            starts_at: start + node * 100,
            core: DeliveryScheduler::new(config, None),
        })
        .collect::<Vec<_>>();
    probe.reset();
    let mut now = start;
    while now < end {
        clock_to(store, now);
        for node in nodes.iter_mut().filter(|node| node.starts_at <= now) {
            run_node(store, node, now).await;
        }
        let next = nodes
            .iter()
            .filter_map(|node| {
                if node.starts_at > now {
                    Some(node.starts_at)
                } else {
                    node.core.next_wake(now)
                }
            })
            .min();
        let Some(next) = next else {
            break;
        };
        now = next;
    }
    assert_eq!(probe.payload_writes(), 0, "no payload row is written (R1)");
    probe.state_writes()
}

/// Goal (i), W3S review finding 3: the state row writes of a dead
/// destination in one hour follow the breaker cycle of each node's real
/// scheduling core, and are the same with four times the backlog. The count
/// is exact for the shortest jittered intervals (the worst case).
#[tokio::test]
async fn given_dead_destination_for_a_simulated_hour_when_twelve_owners_claim_and_fail_then_writes_are_bounded()
 {
    let (small, small_probe) = probed_store().await;
    let small_writes = dead_destination_hour(&small, &small_probe, 1_000).await;
    small.finish().await;
    let (large, large_probe) = probed_store().await;
    let large_writes = dead_destination_hour(&large, &large_probe, 4_000).await;
    large.finish().await;
    eprintln!(
        "pubsub dead destination hour: backend={} state_writes(1000)={small_writes} \
         state_writes(4000)={large_writes}",
        TestStore::selected_backend()
    );
    assert_eq!(small_writes, DEAD_HOUR_WRITES, "{small_writes} writes");
    assert_eq!(
        small_writes, large_writes,
        "the bound does not depend on the backlog"
    );
}

/// F6: records at their retry deadline get their final attempt while the
/// breaker is open: a `DeadlineOnly` ask with 8 free slots takes 8 of 20,
/// and each failure is dropped with `deadline_no_dlq`.
#[tokio::test]
async fn given_open_breaker_and_expired_deadlines_when_deadline_only_claim_then_eight_get_final_decision()
 {
    let store = TestStore::new().await;
    let destination = http_backlog(&store, 20, 1, "late").await;
    let now = store.advance_time(Duration::from_millis(HOUR_MILLIS as u64 + 1_000));
    let claimed = claim_asks(
        &store,
        "node",
        now,
        ask(&destination, 8, AskMode::DeadlineOnly),
    )
    .await;
    assert_eq!(claimed.len(), 8);
    for record in &claimed {
        assert!(record.state.retry_deadline <= now);
        let failure = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpTimeout);
        let report = settle(&store, record, failure, None).await;
        assert_eq!(
            report.outcome,
            SettleOutcome::Dropped(DropReason::DeadlineNoDeadLetter)
        );
    }
    let left = claim_asks(&store, "node", now, ask(&destination, 32, AskMode::Due)).await;
    assert_eq!(left.len(), 12);
    store.finish().await;
}

/// Records whose deadline has not passed are not taken by a
/// `DeadlineOnly` ask.
#[tokio::test]
async fn given_records_before_deadline_when_deadline_only_claim_then_nothing_is_taken() {
    let store = TestStore::new().await;
    let destination = http_backlog(&store, 3, 1, "early").await;
    let now = store.now();
    let claimed = claim_asks(
        &store,
        "node",
        now,
        ask(&destination, 8, AskMode::DeadlineOnly),
    )
    .await;
    assert!(claimed.is_empty());
    store.finish().await;
}

/// Takeover: an owner that crashed with 5 leased records; another owner
/// that never saw the destination finds its head in the sweep at the lease
/// end and claims the 5 records.
#[tokio::test]
async fn given_crashed_owner_when_lease_passes_then_sweep_and_claim_take_over() {
    let store = TestStore::new().await;
    let destination = http_backlog(&store, 5, 1, "crash").await;
    let now = store.now();
    let first = claim_asks(&store, "node-1", now, ask(&destination, 8, AskMode::Due)).await;
    assert_eq!(first.len(), 5);
    let lease_end = first[0].state.lease.as_ref().unwrap().until;
    let heads = sweep_all(&store).await;
    let head = heads
        .iter()
        .find(|head| head.destination == destination)
        .expect("the sweep sees the leased partition");
    assert_eq!(head.head_due, lease_end, "a lease end is the due time");
    let early = claim_asks(
        &store,
        "node-2",
        now + 500,
        ask(&destination, 8, AskMode::Due),
    )
    .await;
    assert!(early.is_empty(), "the lease holds");
    let taken = claim_asks(
        &store,
        "node-2",
        lease_end,
        ask(&destination, 8, AskMode::Due),
    )
    .await;
    assert_eq!(taken.len(), 5);
    for record in &taken {
        let report = settle(&store, record, DeliveryAttemptResult::Delivered, None).await;
        assert_eq!(report.outcome, SettleOutcome::Delivered);
    }
    for record in &first {
        let report = settle(&store, record, DeliveryAttemptResult::Delivered, None).await;
        assert_eq!(
            report.outcome,
            SettleOutcome::ClaimLost,
            "the fence stops the old owner"
        );
    }
    store.finish().await;
}

async fn sweep_all(store: &TestStore) -> Vec<pubsub_provider::DestinationHead> {
    let mut cursor = SweepCursor::from_start(None);
    let mut heads = Vec::new();
    loop {
        let page = store
            .pubsub_provider
            .sweep_destination_heads(cursor, NonZeroU32::new(256).unwrap())
            .await
            .unwrap();
        heads.extend(page.heads);
        let Some(next) = page.next else {
            return heads;
        };
        cursor = next;
    }
}

/// R6: 300 destinations swept in pages of 256: every head once.
#[tokio::test]
async fn given_records_of_300_destinations_when_swept_in_pages_of_256_then_every_head_is_returned_once()
 {
    let store = TestStore::new().await;
    let topic = create_topic(&store, &unique("sweep"), HashMap::new())
        .await
        .unwrap();
    for _ in 0..300 {
        sqs_subscription(&store, &topic.topic_arn, &unique("q"), &[]).await;
    }
    publish_all(&store, &topic.topic_arn, 1, "sweep").await;
    let heads = sweep_all(&store).await;
    let distinct: std::collections::HashSet<_> =
        heads.iter().map(|head| head.destination.clone()).collect();
    assert_eq!(heads.len(), 300);
    assert_eq!(distinct.len(), 300);
    store.finish().await;
}

/// (iii): a claim of 32 records with 256 KiB bodies writes 32 small state
/// rows and no payload row.
#[tokio::test]
async fn given_32_large_records_when_claimed_then_only_state_rows_are_written() {
    let (store, probe) = probed_store().await;
    let body = "p".repeat(256 * 1024 - 64);
    let destination = http_backlog(&store, 1, 32, &body).await;
    probe.reset();
    let now = store.now();
    let claimed = claim_asks(&store, "node", now, ask(&destination, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    assert_eq!(probe.payload_writes(), 0);
    assert_eq!(probe.state_writes(), 32);
    assert_eq!(probe.commits(), 1);
    for record in &claimed {
        assert!(record.state.encode_value().unwrap().len() <= 512);
        assert_eq!(record.payload.message_body.as_deref(), Some(body.as_str()));
    }
    store.finish().await;
}

/// W3S review finding 5: with fewer than `take + 1` records the claim still
/// reports the next due time (here the lease end of leased records).
#[tokio::test]
async fn given_leased_records_when_a_larger_ask_takes_none_then_next_due_is_the_lease_end() {
    let store = TestStore::new().await;
    let destination = http_backlog(&store, 3, 1, "leased").await;
    let now = store.now();
    let leased = claim_asks(&store, "other", now, ask(&destination, 3, AskMode::Due)).await;
    assert_eq!(leased.len(), 3);
    let claims = store
        .pubsub_provider
        .claim_deliveries(DeliveryClaimRequest {
            terms: ClaimTerms {
                owner: "node".to_owned(),
                now,
                lease_until: now + 1_000,
            },
            asks: ask(&destination, 8, AskMode::Due),
        })
        .await
        .unwrap();
    assert!(claims[0].claimed.is_empty());
    assert_eq!(claims[0].next_due, Some(leased[0].state.due_at));
    store.finish().await;
}
