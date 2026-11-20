//! Local SQS batch bounds, lock order and failure reads (W3S findings 4, 6
//! and 7) on the provider that `STORAGE_TEST_BACKEND` selects.

use std::{collections::HashMap, sync::atomic::Ordering};

use pubsub_provider::{LocalOutcome, SettleOutcome};

use super::{
    pubsub_contract_tests::create_topic,
    pubsub_delivery_contract_tests::{
        batch_of, claim, manager, materialize, publish, publish_batch_records, queue_bodies,
        sqs_subscription, unique,
    },
    queue_claim_tests::probed_store,
    test_support::TestStore,
};

/// Finding 4: a batch over 1 MiB of bodies is refused before any I/O, so
/// the runner splits it; no record is settled.
#[tokio::test]
async fn given_11_records_of_100_kib_when_batched_then_the_provider_refuses_over_one_mib() {
    let store = TestStore::new().await;
    let body = "b".repeat(100 * 1024);
    let bodies: Vec<String> = (0..11).map(|_| body.clone()).collect();
    let (queue_name, _url) = publish_batch_records(&store, "bytes", &bodies).await;
    let records = claim(&store, "bytes").await;
    assert_eq!(records.len(), 11);
    let outcome = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &records))
        .await
        .unwrap();
    assert_eq!(outcome, LocalOutcome::TooLarge, "11 x 100 KiB > 1 MiB");
    let outcome = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &records[..10]))
        .await
        .unwrap();
    assert!(matches!(outcome, LocalOutcome::Settled(reports) if reports.len() == 10));
    store.finish().await;
}

/// Finding 4: a capacity rejection of a batch of 16 is not a record failure.
/// Before the change every record was settled as transient (attempts + 1)
/// and none reached the queue in the pass; now the batch is split in halves
/// and all 16 arrive in the same pass.
#[tokio::test]
async fn given_capacity_rejection_when_batch_delivered_then_halves_deliver() {
    let (store, probe) = probed_store().await;
    let bodies: Vec<String> = (0..16).map(|index| format!("m-{index}")).collect();
    let (_queue_name, url) = publish_batch_records(&store, "halves", &bodies).await;
    let (manager, _queues) = manager(&store, None);
    // The pass commits an empty materialization (measured here), then the
    // claim; the commit after them is the local batch.
    probe.reset();
    assert_eq!(materialize(&store).await, 0);
    let before_claim = probe.commits();
    probe
        .reject_capacity_after
        .store(before_claim + 2, Ordering::SeqCst);
    assert_eq!(manager.process_due_deliveries(16).await.unwrap(), 16);
    assert_eq!(probe.reject_capacity_after.load(Ordering::SeqCst), 0);
    assert_eq!(queue_bodies(&store, &url).await.len(), 16);
    assert!(
        claim(&store, "after").await.is_empty(),
        "no record was retried"
    );
    store.finish().await;
}

/// Finding 7: 32 records of one subscription whose queue is gone read the
/// subscription and its topic once, not once for each record (64 reads).
#[tokio::test]
async fn given_missing_queue_when_32_records_settle_then_subscription_is_read_once() {
    let (store, probe) = probed_store().await;
    let topic = create_topic(&store, &unique("memo"), HashMap::new())
        .await
        .unwrap();
    let queue_name = unique("gone");
    sqs_subscription(&store, &topic.topic_arn, &queue_name, &[]).await;
    for index in 0..32 {
        publish(
            &store,
            &topic.topic_arn,
            &format!("m-{index}"),
            HashMap::new(),
        )
        .await;
    }
    let mut written = 0;
    while written < 32 {
        written += materialize(&store).await;
    }
    let records = claim(&store, "memo").await;
    assert_eq!(records.len(), 32);
    probe.reset();
    let reports = store
        .pubsub_provider
        .deliver_to_local_queue(batch_of(&store, &queue_name, &records))
        .await
        .unwrap()
        .settled()
        .unwrap();
    assert!(
        reports
            .iter()
            .all(|report| matches!(report.outcome, SettleOutcome::Dropped(_)))
    );
    // SQL counts statements: one fenced read, one state delete and one payload
    // delete for each record, the catalog, one subscription and one topic read
    // (3 x 32 + 3 = 99 on SQL; KV counts 4 read waves). One subscription and
    // topic read for each record adds 64 calls.
    assert!(
        probe.calls() <= 3 * 32 + 4,
        "calls {} for 32 failed records",
        probe.calls()
    );
    store.finish().await;
}

/// Finding 6: two batches that hold the same records in opposite order lock
/// them in record id order, so neither waits for a Postgres deadlock check
/// (1 s by default).
#[cfg(feature = "postgres")]
#[tokio::test]
#[ignore = "needs Postgres"]
async fn given_opposite_batch_order_when_settled_on_two_connections_then_both_commit_fast() {
    let store = TestStore::new().await;
    for round in 0..5 {
        let bodies: Vec<String> = (0..8).map(|index| format!("r{round}-{index}")).collect();
        let (queue_name, _url) = publish_batch_records(&store, "order", &bodies).await;
        let records = claim(&store, "order").await;
        let forward = batch_of(&store, &queue_name, &records);
        let mut reversed = forward.clone();
        reversed.items.reverse();
        let started = std::time::Instant::now();
        let (first, second) = tokio::join!(
            store.pubsub_provider.deliver_to_local_queue(forward),
            store.pubsub_provider.deliver_to_local_queue(reversed)
        );
        assert!(first.is_ok() && second.is_ok());
        assert!(
            started.elapsed() < std::time::Duration::from_millis(500),
            "round {round}: {:?}",
            started.elapsed()
        );
    }
    store.finish().await;
}
