//! W3 step 9 claim path (design 7.1 and 7.2) on the provider that
//! `STORAGE_TEST_BACKEND` selects: tests 16 and 17. Both need concurrent
//! native writers, so they run on Postgres and FoundationDB only.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use pubsub_provider::{
    AskMode, ClaimedDelivery, DestinationAsk, DestinationKey, PublishRequest, PubsubMessageId,
    SubscribeRequest, SubscriptionProtocol, SweepCursor,
};

use super::{
    pubsub_contract_tests::{create_topic, queue_arn, subscribe},
    pubsub_delivery_contract_tests::claim_asks,
    queue_claim_tests::{concurrent_backend, probed_store},
    test_support::TestStore,
};

async fn fanout(store: &TestStore, label: &str, subscriptions: usize, publishes: usize) {
    let topic = create_topic(
        store,
        &format!("{label}-{}", uuid::Uuid::new_v4().simple()),
        HashMap::new(),
    )
    .await
    .unwrap();
    for index in 0..subscriptions {
        subscribe(
            store,
            SubscribeRequest {
                topic_arn: topic.topic_arn.clone(),
                protocol: SubscriptionProtocol::Queue,
                endpoint: queue_arn(&format!("{label}-{index}")),
                attributes: HashMap::new(),
                extra_json: serde_json::Value::Null,
            },
        )
        .await
        .unwrap();
    }
    for _ in 0..publishes {
        store
            .pubsub_provider
            .accept_publish(
                PublishRequest {
                    topic_arn: topic.topic_arn.clone(),
                    message: "claim".to_owned(),
                    subject: None,
                    message_attributes: HashMap::new(),
                },
                PubsubMessageId::new(),
                false,
            )
            .await
            .unwrap();
    }
    while store
        .pubsub_provider
        .materialize_publish_intents(32)
        .await
        .unwrap()
        .records
        > 0
    {}
}

/// One claim of `take` records of `destination`.
async fn claim_one(
    store: &TestStore,
    owner: &str,
    destination: &DestinationKey,
    take: u32,
) -> Vec<ClaimedDelivery> {
    let ask = DestinationAsk {
        destination: destination.clone(),
        take: std::num::NonZeroU32::new(take).unwrap(),
        mode: AskMode::Due,
    };
    claim_asks(store, owner, store.now(), vec![ask]).await
}

/// The destinations with due records.
async fn due_destinations(store: &TestStore) -> Vec<DestinationKey> {
    let mut cursor = SweepCursor::from_start(None);
    let mut destinations = Vec::new();
    loop {
        let page = store
            .pubsub_provider
            .sweep_destination_heads(cursor, std::num::NonZeroU32::new(256).unwrap())
            .await
            .unwrap();
        destinations.extend(page.heads.into_iter().map(|head| head.destination));
        let Some(next) = page.next else {
            return destinations;
        };
        cursor = next;
    }
}

/// Test 16: two claimers, the first held at its commit: the second takes
/// another record at once (`FOR UPDATE SKIP LOCKED` on Postgres, disjoint
/// keys on FoundationDB) and does not wait for the first.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres or foundationdb"]
async fn given_claim_held_at_commit_when_another_claims_then_it_takes_another_record_at_once() {
    assert!(concurrent_backend(), "select postgres or foundationdb");
    let (store, probe) = probed_store().await;
    let store = Arc::new(store);
    fanout(&store, "held", 1, 2).await;
    let destination = due_destinations(&store).await.remove(0);
    probe.hold_next_commit.store(true, Ordering::SeqCst);
    let held = {
        let store = Arc::clone(&store);
        let destination = destination.clone();
        tokio::spawn(async move { claim_one(&store, "held", &destination, 1).await })
    };
    probe.held.notified().await;
    let other = tokio::time::timeout(
        Duration::from_secs(3),
        claim_one(&store, "other", &destination, 1),
    )
    .await
    .expect("the second claim did not wait for the held one");
    probe.release.notify_one();
    let held = held.await.unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(held.len(), 1);
    assert_ne!(held[0].state.id, other[0].state.id, "disjoint records");
    Arc::into_inner(store).unwrap().finish().await;
}

/// Counts claim attempts (each starts with one snapshot scan wave) on KV.
#[derive(Default)]
struct ScanCounter {
    scans: AtomicUsize,
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
impl sql_backend::SqlFaultProbe for ScanCounter {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for ScanCounter {
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        if call.operation == kv_backend::KvFaultOperation::ScanBatch {
            self.scans.fetch_add(1, Ordering::SeqCst);
        }
        None
    }
}

/// Test 17: 4 claimers on 200 due records with a snapshot window and a
/// random slot: native conflict retries are less than half of the claims.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires STORAGE_TEST_BACKEND=foundationdb"]
async fn given_200_due_records_when_4_claimers_drain_them_then_retries_are_under_half() {
    let counter = Arc::new(ScanCounter::default());
    let store = Arc::new(
        TestStore::new_with_native_probes(
            #[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
            counter.clone(),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            counter.clone(),
        )
        .await,
    );
    fanout(&store, "wide", 20, 10).await;
    let destinations = Arc::new(due_destinations(&store).await);
    assert_eq!(destinations.len(), 20);
    counter.scans.store(0, Ordering::SeqCst);
    // Each claimer asks the destinations in its own rotated order, as the
    // runners of several nodes do.
    let claimers = (0..4)
        .map(|index| {
            let store = Arc::clone(&store);
            let destinations = Arc::clone(&destinations);
            tokio::spawn(async move {
                let mut claimed = Vec::new();
                let mut attempts = 0;
                let count = destinations.len();
                for round in 0..count * 3 {
                    let destination = &destinations[(index * 5 + round) % count];
                    let records =
                        claim_one(&store, &format!("claimer-{index}"), destination, 8).await;
                    attempts += 1;
                    claimed.extend(records.into_iter().map(|record| record.state.id.0));
                }
                (claimed, attempts)
            })
        })
        .collect::<Vec<_>>();
    let mut ids = HashSet::new();
    let mut claims = 0;
    for claimer in claimers {
        let (claimed, attempts) = tokio::time::timeout(Duration::from_secs(10), claimer)
            .await
            .expect("claimers exceeded the test budget")
            .unwrap();
        claims += attempts;
        for id in claimed {
            assert!(ids.insert(id), "a record was claimed twice");
        }
    }
    assert_eq!(ids.len(), 200);
    let scans = counter.scans.load(Ordering::SeqCst);
    let retries = scans.saturating_sub(claims);
    eprintln!(
        "pubsub claim counters: backend={} claims={claims} scans={scans} retries={retries}",
        TestStore::selected_backend()
    );
    if TestStore::selected_backend() == "foundationdb" {
        assert!(
            retries * 2 < claims,
            "{retries} retries for {claims} claims"
        );
    }
    Arc::into_inner(store).unwrap().finish().await;
}
