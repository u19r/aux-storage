use std::sync::{
    Arc,
    atomic::{AtomicI64, Ordering},
};

use storage_provider::{DynamoOperations, StreamDurationTrimConfig, StreamDurationTrimWorker};
use storage_types::{
    Clock, ItemHistoryAddress, StreamName, TableHistoryTarget, TableName, TimestampMillis,
};
use stream_provider::{
    StreamOperations, StreamRowKind, SystemStreamLineage, SystemStreamSplitChild,
    SystemStreamSplitOperationId, SystemStreamSplitRequest,
};

struct CapturedClock(AtomicI64);

impl Clock for CapturedClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[tokio::test]
async fn given_cross_lineage_capture_times_when_newer_revisions_expire_first_then_the_older_snapshot_survives_its_own_horizon()
 {
    let recent = 1_800_000_000_000;
    let hour = 3_600_000;
    let clock = Arc::new(CapturedClock(AtomicI64::new(recent - 72 * hour)));
    let store = crate::SqlStore::initialize_schema(
        crate::sqlite::SqliteBackend::initialize(":memory:")
            .await
            .unwrap(),
    )
    .await
    .unwrap()
    .with_clock(clock.clone());
    let name = TableName::new("lineage-retention");
    let created = store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": name,
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "AuxStreamDurationHours": 24,
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    let target = TableHistoryTarget::try_from(
        created
            .table_description
            .latest_stream_arn
            .as_deref()
            .unwrap(),
    )
    .unwrap();
    let key: storage_types::KeyAttributes =
        serde_json::from_value(serde_json::json!({"pk": {"S": "one"}})).unwrap();
    let history = StreamRowKind::Item(
        ItemHistoryAddress {
            history_id: target.history_id,
            stream_name: StreamName::table_item_stream(
                &name,
                &storage_types::ItemKey::table_key(
                    name.clone(),
                    storage_types::AttributeValue::S("one".into()),
                    None,
                ),
            )
            .unwrap(),
        }
        .physical_stream_name()
        .unwrap(),
    );
    clock.0.store(recent, Ordering::SeqCst);
    put_revision(&store, &name, "recent-parent").await;
    let scope = store.system_stream_scope().await.unwrap();
    let parent = store
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .remove(0);
    let children = ["child-left", "child-right"].map(|id| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                id,
                id,
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    store
        .split_system_stream(
            SystemStreamSplitRequest::try_new(
                scope.clone(),
                parent,
                children,
                SystemStreamSplitOperationId::try_new("retention-split").unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // Requests captured before the recent parent write can settle later on
    // another lineage. Version order must not stand in for expiry order.
    clock.0.store(recent - 48 * hour, Ordering::SeqCst);
    put_revision(&store, &name, "older-child").await;
    store
        .delete_item(storage_types::DeleteItemRequest::new(
            name.clone(),
            key.clone(),
        ))
        .await
        .unwrap();
    clock.0.store(recent + hour, Ordering::SeqCst);
    cleanup_page(&store).await;
    assert_eq!(
        store
            .read_forward(history.clone(), None, 10)
            .await
            .unwrap()
            .items
            .len(),
        3
    );
    let visible = store
        .get_stream_records(storage_types::GetStreamRecordsRequest::table(
            name.clone(),
            None,
            Some(10),
        ))
        .await
        .unwrap();
    assert_eq!(visible.records().len(), 1);
    assert_eq!(
        serde_json::to_value(&visible.records()[0].item).unwrap()["value"]["S"],
        "recent-parent"
    );
    storage_provider::SystemStreamTrimExecutor::new(&store, 10)
        .run_page(scope, clock.now() - 24 * hour, None)
        .await
        .unwrap();
    assert_eq!(
        store
            .read_forward(history.clone(), None, 10)
            .await
            .unwrap()
            .items
            .len(),
        3,
        "system trim changed item history"
    );
    clock.0.store(recent + 25 * hour, Ordering::SeqCst);
    for _ in 0..8 {
        cleanup_page(&store).await;
        clock.0.fetch_add(1, Ordering::SeqCst);
    }
    assert!(
        store
            .read_forward(history, None, 10)
            .await
            .unwrap()
            .items
            .is_empty()
    );
    assert!(
        store
            .get_stream_records(storage_types::GetStreamRecordsRequest::table(
                name.clone(),
                None,
                Some(10)
            ))
            .await
            .unwrap()
            .records()
            .is_empty()
    );
    assert!(
        store
            .get_item(storage_types::GetItemRequest::new(name, key))
            .await
            .unwrap()
            .item
            .is_none()
    );
}

async fn put_revision(
    store: &crate::SqlStore<crate::sqlite::SqliteBackend>,
    name: &TableName,
    value: &str,
) {
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item": {"pk": {"S": "one"}, "value": {"S": value}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn cleanup_page(store: &crate::SqlStore<crate::sqlite::SqliteBackend>) {
    StreamDurationTrimWorker::new(store, StreamDurationTrimConfig::default())
        .run_due_page(store.now(), store.now())
        .await
        .unwrap();
}
