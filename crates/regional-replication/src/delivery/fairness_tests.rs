use std::{collections::HashMap, sync::Arc, time::Duration};

use bg_jobs_tokio::test_support::JobClock;
use replication_types::{event::content::CanonicalEventCodec, identity::origin::OriginId};
use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{
    AttemptMode, AttributeValue, GetItemRequest, KeyAttributes, PutItemRequest, ReadProtection,
    TableName, WireItem,
};

use super::DeliveryScope;
use crate::test_support as fixture;

#[tokio::test]
async fn given_a_failed_peer_and_hot_table_when_delivery_restarts_then_other_peers_tables_and_partitions_progress_and_recovery_catches_up()
 {
    let directory = tempfile::tempdir().unwrap();
    let paths = (1..=3)
        .map(|index| directory.path().join(format!("region-{index}.sqlite")))
        .collect::<Vec<_>>();
    let mut stores = Vec::new();
    for path in &paths {
        stores.push(fixture::open(path).await);
    }
    let mut cluster = fixture::NativeCluster::start(&stores.iter().collect::<Vec<_>>()).await;
    for name in ["busy_table", "quiet_table"] {
        cluster.install(&stores[0], name, &[2, 3]).await;
    }
    // Closing the actual receiver leaves a durable backlog for peer 2, rather
    // than a mock error.
    cluster.stop_receiver(1).await;
    let mut expected = Vec::new();
    let mut partitions = Vec::new();
    for name in ["busy_table", "quiet_table"] {
        let state = fixture::installed_state(&stores[0], 1, name).await;
        let mut selected = Vec::new();
        for ordinal in 0..64 {
            let mut key = KeyAttributes::default();
            key.insert("pk", AttributeValue::S(format!("key-{ordinal}")));
            let partition = fixture::partitions().select(
                state.configuration.lineage,
                &fixture::codec().encode_key(&key).unwrap(),
            );
            if u16::from(partition) == 0 || partitions.contains(&partition) {
                continue;
            }
            selected.push(partition);
            partitions.push(partition);
            expected.push((name, key));
            if selected.len() == 2 {
                break;
            }
        }
        assert_eq!(selected.len(), 2);
    }
    // Keep the busy scope active during each bounded round, so success requires
    // rotation.
    for round in 0..2 {
        for (name, key) in &expected {
            put(&cluster, name, key, &format!("round-{round}")).await;
        }
        let targets = targets(&stores[0], &cluster.journals[0], &expected).await;
        let mut complete = false;
        for turn in 0..128 {
            put(
                &cluster,
                expected[0].0,
                &expected[0].1,
                &format!("hot-{round}-{turn}"),
            )
            .await;
            cluster.workers[0].drive_once().await.unwrap();
            cluster.clock.advance(Duration::from_secs(1));
            if through(&stores[0], &cluster.journals[0], &targets, 3).await {
                complete = true;
                break;
            }
        }
        assert!(
            complete,
            "failed peer or hot table starved a live peer/table/partition"
        );
        for (name, key) in &expected[1..] {
            assert_item(&stores[2], name, key, Some(&format!("round-{round}"))).await;
        }
        for (name, key) in &expected {
            assert_item(&stores[1], name, key, None).await;
        }
        assert!(
            !through(&stores[0], &cluster.journals[0], &targets, 2).await,
            "failed peer acquired a false durable ACK"
        );
        let previous_time = cluster.clock.wall_now();
        cluster.shutdown().await;
        drop(stores);
        stores = Vec::new();
        for path in &paths {
            stores.push(fixture::open(path).await);
        }
        cluster = fixture::NativeCluster::start(&stores.iter().collect::<Vec<_>>()).await;
        let now = cluster.clock.wall_now();
        if previous_time > now {
            cluster
                .clock
                .advance(previous_time.duration_since(now).unwrap());
        }
        cluster.clock.advance(Duration::from_secs(1));
        if round == 0 {
            cluster.stop_receiver(1).await;
        }
    }
    // Restore the receiver after the second restart and drain the same durable
    // backlog.
    let targets = targets(&stores[0], &cluster.journals[0], &expected).await;
    let recovery_turns = targets
        .iter()
        .map(|(_, position)| u64::from(*position) + 1)
        .sum::<u64>();
    let mut recovered = false;
    let mut counts = Vec::new();
    for _ in 0..recovery_turns {
        counts.clear();
        for worker in &cluster.workers {
            counts.push(worker.drive_once().await.unwrap().counts);
        }
        cluster.clock.advance(Duration::from_secs(1));
        if through(&stores[0], &cluster.journals[0], &targets, 2).await
            && through(&stores[0], &cluster.journals[0], &targets, 3).await
        {
            recovered = true;
            break;
        }
    }
    if !recovered {
        let mut inventory = Vec::new();
        for (scope, target) in &targets {
            let position = super::load_delivery_progress(
                &stores[0],
                Arc::clone(&cluster.journals[0]),
                scope.clone(),
            )
            .await
            .unwrap()
            .through();
            inventory.push((
                scope.destination.region.clone(),
                u16::from(scope.origin.journal_partition),
                u64::from(*target),
                u64::from(position),
            ));
        }
        panic!("restored peer did not catch up: {inventory:?}, last drives: {counts:?}");
    }
    for (name, key) in &expected {
        let source = stores[0]
            .get_item(GetItemRequest::new(TableName::new(*name), key.clone()))
            .await
            .unwrap();
        for store in &stores[1..] {
            let destination = store
                .get_item(GetItemRequest::new(TableName::new(*name), key.clone()))
                .await
                .unwrap();
            assert_eq!(
                source
                    .item
                    .as_ref()
                    .map(|item| item.to_attribute_map().unwrap()),
                destination
                    .item
                    .map(|item| item.into_attribute_map().unwrap())
            );
        }
    }
    cluster.shutdown().await;
}

async fn put(cluster: &fixture::NativeCluster, name: &str, key: &KeyAttributes, value: &str) {
    let item = WireItem::try_from(&HashMap::from([
        ("pk".to_owned(), key.get("pk").unwrap().clone()),
        ("value".to_owned(), AttributeValue::S(value.into())),
    ]))
    .unwrap();
    cluster.operations[0]
        .put_item(PutItemRequest::new(TableName::new(name), item))
        .await
        .unwrap();
}

async fn assert_item(store: &fixture::Store, name: &str, key: &KeyAttributes, value: Option<&str>) {
    let response = store
        .get_item(GetItemRequest::new(TableName::new(name), key.clone()))
        .await
        .unwrap();
    assert_eq!(
        response.item.map(|item| item
            .into_attribute_map()
            .unwrap()
            .get("value")
            .unwrap()
            .clone()),
        value.map(|value| AttributeValue::S(value.into()))
    );
}

async fn targets(
    store: &fixture::Store,
    journal: &Arc<crate::journal::JournalStore>,
    expected: &[(&str, KeyAttributes)],
) -> Vec<(
    DeliveryScope,
    replication_types::identity::origin::OriginPosition,
)> {
    let mut targets = Vec::new();
    for (name, key) in expected {
        let state = fixture::installed_state(store, 1, name).await;
        let partition = fixture::partitions().select(
            state.configuration.lineage,
            &fixture::codec().encode_key(key).unwrap(),
        );
        let owner = Arc::clone(journal);
        let head = store
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    Ok(owner
                        .load_source_heads(attempt, &[partition], ReadProtection::Snapshot)
                        .await?[0])
                })
            })
            .await
            .unwrap();
        for destination in state
            .sources
            .iter()
            .filter(|source| source.region != fixture::region(1))
        {
            targets.push((
                DeliveryScope {
                    origin: OriginId {
                        region: fixture::region(1),
                        database_group: [1; 16].try_into().unwrap(),
                        journal_generation: 1.into(),
                        journal_partition: partition,
                    },
                    table: state.configuration.table,
                    local_table: state.configuration.table,
                    lineage: state.configuration.lineage,
                    membership: state.membership,
                    destination: destination.clone(),
                },
                head,
            ));
        }
    }
    targets
}

async fn through(
    store: &fixture::Store,
    journal: &Arc<crate::journal::JournalStore>,
    targets: &[(
        DeliveryScope,
        replication_types::identity::origin::OriginPosition,
    )],
    peer: u8,
) -> bool {
    for (scope, target) in targets
        .iter()
        .filter(|(scope, _)| scope.destination.region == fixture::region(peer))
    {
        let owner = Arc::clone(journal);
        let scope = scope.clone();
        let position = store
            .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    Ok(owner
                        .load_delivery_progress(attempt, &scope)
                        .await?
                        .through())
                })
            })
            .await
            .unwrap();
        if position < *target {
            return false;
        }
    }
    true
}
