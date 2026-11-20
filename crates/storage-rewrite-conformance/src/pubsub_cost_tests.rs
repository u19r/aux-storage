//! Native transactions, native calls and rows written of the SNS path on
//! the provider that `STORAGE_TEST_BACKEND` selects (W3S plan 3.3, Option 4
//! column). `calls` counts SQL statements and KV read waves; KV staged
//! writes are counted as logical mutations. The test prints the counts for
//! the package report and asserts the transactions and the rows written.

use std::{collections::HashMap, sync::Arc};

use pubsub_provider::{
    AskMode, DeliveryAttemptResult, DeliveryFailureCode, DestinationKey, LocalDelivery,
    SettleOutcome, SubscriptionProtocol,
};
use queue_provider::{Queue, QueueArn};
use storage_types::MessagingIdentity;

use super::{
    native_census_test_support::NativeCensus,
    pubsub_contract_tests::{create_topic, queue_arn},
    pubsub_delivery_contract_tests::{claim_asks, local_item, settle, sqs_subscription, unique},
    pubsub_schedule_contract_tests::{ask, http_backlog, publish_all},
    queue_claim_tests::{ClaimProbe, probed_store},
    queue_state_split_tests::shared_database,
    test_support::TestStore,
};

/// (transactions, calls, state rows written, payload rows written, KV
/// mutations) since the last reset.
fn costs(probe: &ClaimProbe) -> (usize, usize, usize, usize, usize) {
    let costs = (
        probe.commits(),
        probe.calls(),
        probe.state_writes(),
        probe.payload_writes(),
        probe.mutations(),
    );
    probe.reset();
    costs
}

#[tokio::test]
async fn given_option_4_events_when_measured_then_costs_follow_the_plan() {
    let (store, probe) = probed_store().await;
    let kv = matches!(
        TestStore::selected_backend().as_str(),
        "rocksdb" | "foundationdb"
    );

    // Materialize one publish to 10 subscriptions (one chunk).
    let topic = create_topic(&store, &unique("cost"), HashMap::new())
        .await
        .unwrap();
    for _ in 0..10 {
        sqs_subscription(&store, &topic.topic_arn, &unique("q"), &[]).await;
    }
    probe.reset();
    publish_all(&store, &topic.topic_arn, 1, "fan").await;
    let _publish_and_materialize = costs(&probe);

    // A claim of 32 HTTP records, one success, one transient failure.
    let destination = http_backlog(&store, 1, 32, "http").await;
    probe.reset();
    let now = store.now();
    let claimed = claim_asks(&store, "cost", now, ask(&destination, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    let claim = costs(&probe);
    let success = settle(&store, &claimed[0], DeliveryAttemptResult::Delivered, None).await;
    assert_eq!(success.outcome, SettleOutcome::Delivered);
    let http_success = costs(&probe);
    let failure = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503));
    let retried = settle(&store, &claimed[1], failure, None).await;
    assert_eq!(retried.outcome, SettleOutcome::Retried);
    let http_transient = costs(&probe);

    // 32 local deliveries to one queue in one transaction.
    let name = unique("local");
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name.clone(),
            attributes: HashMap::new(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .unwrap();
    let local_topic = create_topic(&store, &unique("local"), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(&store, &local_topic.topic_arn, &name, &[]).await;
    publish_all(&store, &local_topic.topic_arn, 32, "local").await;
    let queue = DestinationKey::of_endpoint(SubscriptionProtocol::Queue, &queue_arn(&name));
    let now = store.now();
    let local = claim_asks(&store, "cost", now, ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(local.len(), 32);
    probe.reset();
    let reports = store
        .pubsub_provider
        .deliver_to_local_queue(LocalDelivery {
            queue: QueueArn::parse(&queue_arn(&name), &MessagingIdentity::default()).unwrap(),
            items: local
                .iter()
                .map(|claimed| local_item(claimed, "m"))
                .collect(),
            jitter: 0,
            identity: MessagingIdentity::default(),
        })
        .await
        .unwrap()
        .settled()
        .unwrap();
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    let local_batch = costs(&probe);

    // Materialization of one chunk of 10, measured alone.
    publish_all(&store, &topic.topic_arn, 0, "").await;
    store
        .pubsub_provider
        .accept_publish(
            pubsub_provider::PublishRequest {
                topic_arn: topic.topic_arn.clone(),
                message: "fan".to_owned(),
                subject: None,
                message_attributes: HashMap::new(),
            },
            pubsub_provider::PubsubMessageId::new(),
            false,
        )
        .await
        .unwrap();
    probe.reset();
    assert_eq!(
        store
            .pubsub_provider
            .materialize_publish_intents(32)
            .await
            .unwrap()
            .records,
        10
    );
    let materialize = costs(&probe);

    eprintln!(
        "pubsub w3s costs: backend={} (transactions, calls, state_rows, payload_rows, \
         kv_mutations) materialize_10={materialize:?} claim_32={claim:?} \
         http_success={http_success:?} http_transient={http_transient:?} local_32={local_batch:?}",
        TestStore::selected_backend()
    );
    for (name, cost) in [
        ("claim", claim),
        ("success", http_success),
        ("transient", http_transient),
        ("local", local_batch),
    ] {
        assert_eq!(cost.0, 1, "{name} is one transaction");
    }
    assert_eq!(claim.3, 0, "a claim writes no payload (R1)");
    assert_eq!(http_transient.3, 0, "a retry writes no payload (R1)");
    if kv {
        assert_eq!(claim.4, 64, "32 state keys moved");
        assert_eq!(http_success.4, 2, "state and payload deleted");
        assert_eq!(http_transient.4, 2, "one state key moved");
        assert!(materialize.4 >= 20, "10 payload and 10 state keys");
    } else {
        assert_eq!(claim.2, 32, "32 state rows");
        assert_eq!(
            (http_success.2, http_success.3),
            (1, 1),
            "one fenced delete pair"
        );
        assert_eq!(http_transient.2, 1, "one state row");
        // Postgres carries these deletes with COMMIT (`finish_write_batch`,
        // W3 C8), which the statement probe does not see; the census test
        // `given_local_delivery_when_censused_then_one_read_wave_and_commit`
        // pins Postgres.
        if TestStore::selected_backend() != "postgres" {
            assert_eq!((local_batch.2, local_batch.3), (32, 32), "32 delete pairs");
        }
        assert_eq!(
            (materialize.2, materialize.3),
            (10, 10),
            "10 payloads, 10 states"
        );
    }
    store.finish().await;
}

fn backend() -> String {
    TestStore::selected_backend()
}

fn families(set: &std::collections::BTreeSet<String>) -> Vec<&str> {
    set.iter().map(String::as_str).collect()
}

async fn deliver_locally(
    store: &TestStore,
    name: &str,
    claimed: &[pubsub_provider::ClaimedDelivery],
) -> Vec<pubsub_provider::SettleReport> {
    store
        .pubsub_provider
        .deliver_to_local_queue(LocalDelivery {
            queue: QueueArn::parse(&queue_arn(name), &MessagingIdentity::default()).unwrap(),
            items: claimed
                .iter()
                .map(|claimed| local_item(claimed, "m"))
                .collect(),
            jitter: 0,
            identity: MessagingIdentity::default(),
        })
        .await
        .unwrap()
        .settled()
        .unwrap()
}

async fn create_census_queue(store: &TestStore, name: &str) {
    store
        .queue_operations
        .create_queue(Queue {
            queue_url: format!("http://localhost/000000000000/{name}"),
            queue_name: name.to_owned(),
            attributes: HashMap::new(),
            created_at: store.now(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn given_local_delivery_when_censused_then_one_read_wave_and_commit() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let name = unique("census");
    create_census_queue(&store, &name).await;
    let topic = create_topic(&store, &unique("census"), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(&store, &topic.topic_arn, &name, &[]).await;
    publish_all(&store, &topic.topic_arn, 64, "census").await;
    let queue = DestinationKey::of_endpoint(SubscriptionProtocol::Queue, &queue_arn(&name));

    // Warm statement caches and the catalog.
    let now = store.now();
    let warm = claim_asks(&store, "census", now, ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(warm.len(), 32);
    let reports = deliver_locally(&store, &name, &warm).await;
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );

    let now = store.now();
    let claimed = claim_asks(&store, "census", now, ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    census.reset();
    let reports = deliver_locally(&store, &name, &claimed).await;
    let counts = census.snapshot();
    eprintln!("w8e local_32 backend={} {counts}", backend());

    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    assert_eq!(counts.commits, 1);
    assert_eq!(counts.rollbacks, 0);
    match backend().as_str() {
        "foundationdb" => {
            // RV, fence+catalog+absence wave, commit.
            assert_eq!(counts.waves, 3);
            assert!(counts.keys >= 65, "keys={}", counts.keys);
            // 32 absence reads and 32 fence reads are all conflict reads
            // (W8-E-e).
            assert_eq!(counts.conflicts.read_keys, 64, "counts={counts}");
        }
        "postgres" => {
            // Read pipeline with BEGIN; counted writes with COMMIT. Before: 4.
            assert_eq!(counts.waves, 2);
            assert_eq!(counts.lone_begins, 0);
            assert_eq!(counts.locks.for_update, 32);
        }
        _ => {}
    }
    store.finish().await;
}

/// Every visible message of the queue, received with a long visibility.
async fn receive_count(store: &TestStore, url: &str) -> usize {
    let mut total = 0;
    loop {
        let received = store
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
            .len();
        if received == 0 {
            return total;
        }
        total += received;
    }
}

#[tokio::test]
async fn given_stale_catalog_hint_when_delivered_locally_then_absence_read_falls_back() {
    if !shared_database() {
        eprintln!(
            "skipped: {} has no second store over one database",
            backend()
        );
        return;
    }
    let census = NativeCensus::new();
    let a = TestStore::new_with_census(Arc::clone(&census)).await;
    let name = unique("stale");
    create_census_queue(&a, &name).await;
    let topic = create_topic(&a, &unique("stale"), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(&a, &topic.topic_arn, &name, &[]).await;
    publish_all(&a, &topic.topic_arn, 96, "stale").await;
    let queue = DestinationKey::of_endpoint(SubscriptionProtocol::Queue, &queue_arn(&name));
    let url = format!("http://localhost/000000000000/{name}");
    let foundationdb = backend() == "foundationdb";

    // Step 1: no hint on this node yet.
    let claimed = claim_asks(&a, "stale", a.now(), ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    census.reset();
    let reports = deliver_locally(&a, &name, &claimed).await;
    let cold = census.snapshot();
    eprintln!("w8e-e cold backend={} {cold}", backend());
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    assert_eq!(cold.commits, 1);
    if foundationdb {
        // No hint: RV, fence + catalog, absence, commit. Observes the hint.
        assert_eq!(cold.waves, 4);
        // 32 fences + 32 absence reads, all conflict reads.
        assert_eq!(cold.conflicts.read_keys, 64);
    }

    // Step 2: another store purges, so the hint of `a` names the old
    // incarnation.
    let b = a.open_same_database().await;
    b.queue_operations.purge_queue(&url).await.expect("purge");
    drop(b);
    let claimed = claim_asks(&a, "stale", a.now(), ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    census.reset();
    let reports = deliver_locally(&a, &name, &claimed).await;
    let stale = census.snapshot();
    eprintln!("w8e-e stale backend={} {stale}", backend());
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    assert_eq!(stale.commits, 1);
    assert_eq!(stale.rollbacks, 0, "a hint mismatch is not a conflict");
    if foundationdb {
        // RV, fence + catalog + absence under the old incarnation, absence
        // under the new incarnation, commit.
        assert_eq!(stale.waves, 4);
        // Plus 32 absence reads under the old incarnation.
        assert_eq!(stale.conflicts.read_keys, 96);
    }

    // Step 3: the previous delivery refreshed the hint.
    let claimed = claim_asks(&a, "stale", a.now(), ask(&queue, 32, AskMode::Due)).await;
    assert_eq!(claimed.len(), 32);
    census.reset();
    let reports = deliver_locally(&a, &name, &claimed).await;
    let warm = census.snapshot();
    eprintln!("w8e-e refreshed backend={} {warm}", backend());
    assert!(
        reports
            .iter()
            .all(|report| report.outcome == SettleOutcome::Delivered)
    );
    assert_eq!(warm.commits, 1);
    if foundationdb {
        assert_eq!(warm.waves, 3);
        assert_eq!(warm.conflicts.read_keys, 64);
    }

    // Step 1's messages were purged; steps 2 and 3 wrote 32 each.
    assert_eq!(receive_count(&a, &url).await, 64);
    a.finish().await;
}

#[tokio::test]
async fn given_publish_when_censused_then_no_write_round_trip_before_commit() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let name = unique("census");
    create_census_queue(&store, &name).await;
    let topic = create_topic(&store, &unique("pub"), HashMap::new())
        .await
        .unwrap();
    sqs_subscription(&store, &topic.topic_arn, &name, &[]).await;
    let publish = |message: &str| pubsub_provider::PublishRequest {
        topic_arn: topic.topic_arn.clone(),
        message: message.to_owned(),
        subject: None,
        message_attributes: HashMap::new(),
    };
    store
        .pubsub_provider
        .accept_publish(
            publish("warm"),
            pubsub_provider::PubsubMessageId::new(),
            false,
        )
        .await
        .unwrap();

    census.reset();
    let accepted = store
        .pubsub_provider
        .accept_publish(
            publish("measured"),
            pubsub_provider::PubsubMessageId::new(),
            false,
        )
        .await;
    let counts = census.snapshot();
    eprintln!("w8e publish_1 backend={} {counts}", backend());

    assert!(accepted.is_ok());
    assert_eq!(counts.commits, 1);
    match backend().as_str() {
        // Read version, topic, subscription index scan (the entry holds the
        // record, W8-E-f), commit. Before: 5 (a dependent `~p/s` point batch).
        "foundationdb" => {
            assert_eq!(counts.waves, 4, "{counts}");
            assert_eq!(counts.keys, 1, "{counts}");
            assert_eq!(counts.ranges, 1, "{counts}");
            assert_eq!(counts.rows_read, 2, "{counts}");
            // The topic read keeps its conflict key; the index scan is a
            // snapshot read.
            assert_eq!(counts.conflicts.read_keys, 1, "{counts}");
            assert_eq!(counts.conflicts.read_ranges, 0, "{counts}");
            assert_eq!(families(&counts.touched_read), ["~p/t", "~p/x"], "{counts}");
        }
        // Topic, subscription index scan, commit. Before: 4.
        "rocksdb" => {
            assert_eq!(counts.waves, 3, "{counts}");
            assert_eq!(counts.keys, 1, "{counts}");
            assert_eq!(counts.ranges, 1, "{counts}");
            assert_eq!(counts.rows_read, 2, "{counts}");
            assert_eq!(families(&counts.touched_read), ["~p/t", "~p/x"], "{counts}");
        }
        // Topic with BEGIN, subscription page, inserts with COMMIT. Before: 5.
        "postgres" => {
            assert_eq!(counts.waves, 3);
            assert_eq!(counts.lone_begins, 0);
        }
        _ => {}
    }
    store.finish().await;
}

#[tokio::test]
async fn given_subscription_attribute_change_when_censused_then_index_written_without_a_wave() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let name = unique("census");
    create_census_queue(&store, &name).await;
    let topic = create_topic(&store, &unique("attr"), HashMap::new())
        .await
        .unwrap();
    let subscription = sqs_subscription(&store, &topic.topic_arn, &name, &[]).await;

    census.reset();
    let changed = store
        .pubsub_provider
        .set_subscription_attributes(
            &subscription.subscription_arn,
            pubsub_provider::SubscriptionAttributes::parse(
                &HashMap::from([("RawMessageDelivery".to_owned(), "true".to_owned())]),
                &MessagingIdentity::default(),
            )
            .unwrap(),
        )
        .await;
    let counts = census.snapshot();
    eprintln!("w8e-f set_attributes backend={} {counts}", backend());

    assert!(changed.is_ok());
    assert_eq!(counts.commits, 1);
    match backend().as_str() {
        // Read version, `~p/s` row, topic (is_active), commit: unchanged by
        // W8-E-f. Writes: `~p/s` and `~p/x` sets, each with its chunk clear
        // range. Before: wck=1 wcr=1.
        "foundationdb" => {
            assert_eq!(counts.waves, 4, "{counts}");
            assert_eq!(counts.keys, 2, "{counts}");
            assert_eq!(counts.conflicts.read_keys, 2, "{counts}");
            assert_eq!(counts.conflicts.write_keys, 2, "{counts}");
            assert_eq!(counts.conflicts.write_ranges, 2, "{counts}");
            assert_eq!(families(&counts.touched_read), ["~p/s", "~p/t"], "{counts}");
            assert_eq!(
                families(&counts.touched_written),
                ["0:chunk", "~p/s", "~p/x"],
                "{counts}"
            );
        }
        // `~p/s` row, topic, commit.
        "rocksdb" => {
            assert_eq!(counts.waves, 3, "{counts}");
            assert_eq!(counts.keys, 2, "{counts}");
            assert_eq!(families(&counts.touched_read), ["~p/s", "~p/t"], "{counts}");
            assert!(counts.touched_written.contains("~p/s"), "{counts}");
            assert!(counts.touched_written.contains("~p/x"), "{counts}");
        }
        _ => {}
    }
    store.finish().await;
}

#[tokio::test]
async fn given_claim_and_settle_when_censused_then_begin_rides_with_first_statement() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let destination = http_backlog(&store, 1, 64, "lazy").await;
    // Warm statement caches and the catalog.
    let warm = claim_asks(
        &store,
        "lazy",
        store.now(),
        ask(&destination, 32, AskMode::Due),
    )
    .await;
    assert_eq!(warm.len(), 32);
    census.reset();
    let claimed = claim_asks(
        &store,
        "lazy",
        store.now(),
        ask(&destination, 32, AskMode::Due),
    )
    .await;
    let claim = census.snapshot();
    assert_eq!(claimed.len(), 32);
    census.reset();
    assert_eq!(
        settle(&store, &claimed[0], DeliveryAttemptResult::Delivered, None)
            .await
            .outcome,
        SettleOutcome::Delivered
    );
    let success = census.snapshot();
    census.reset();
    let failure = DeliveryAttemptResult::Transient(DeliveryFailureCode::HttpStatus(503));
    assert_eq!(
        settle(&store, &claimed[1], failure, None).await.outcome,
        SettleOutcome::Retried
    );
    let transient = census.snapshot();
    eprintln!(
        "pg-begin backend={} claim_32 {claim} success {success} transient {transient}",
        backend()
    );
    if backend() == "postgres" {
        for counts in [&claim, &success, &transient] {
            assert_eq!(counts.begins, 1, "{counts}");
            assert_eq!(counts.commits, 1, "{counts}");
            assert_eq!(counts.lone_begins, 0, "{counts}");
        }
    }
    store.finish().await;
}
