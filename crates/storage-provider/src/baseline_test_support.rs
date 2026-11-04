//! Real-provider baseline/history contract. Construction stays with each native
//! backend.

use std::sync::Arc;

use storage_types::{
    AttemptMode, AttributeValue, BoundTableInfo, ItemHistoryEntry, ItemHistoryKind,
    ItemHistoryPublication, ItemHistoryReference, ItemProvenance, ItemStreamVersion, KeyAttributes,
    StorageResult, TableName,
};
use stream_provider::{ItemHistoryReadAttempt, ItemHistoryReadBatch};

use crate::{
    BoundDeleteItem, CatalogAttempt, DynamoOperations, DynamoPort, DynamoStreamWriteAttempt,
    GetItemAttempt as _, ItemWriteObservationAttempt, PutItemShape,
    TableHistoryRetentionAttempt as _,
    maintenance::{StreamRetentionFinish, StreamRetentionFinishReason, StreamRetentionPort},
};

pub async fn qualify_baseline_history<P: DynamoPort + DynamoOperations>(store: P)
where for<'a> P::Attempt<'a>: StreamRetentionPort {
    let name = TableName::new("baseline-history");
    store.create_table(serde_json::from_value(serde_json::json!({
        "TableName": name, "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    })).unwrap()).await.unwrap();
    assert_live_revisions(&store, &name, None).await;
    let first = write_baseline(&store, &name, Some("a")).await.unwrap();
    assert_live_revisions(&store, &name, Some(first.version().get())).await;
    let first_entry = read_history(&store, first.clone()).await.unwrap().unwrap();
    assert_eq!(
        first_entry.metadata().publication(),
        &ItemHistoryPublication::PredecessorAnchor
    );
    let second = write_baseline(&store, &name, Some("b")).await.unwrap();
    assert_live_revisions(&store, &name, Some(second.version().get())).await;
    assert!(read_history(&store, first).await.unwrap().is_none());
    assert!(
        read_history(&store, second.clone())
            .await
            .unwrap()
            .is_some()
    );
    assert!(second.version().get() > 1);
    let quiet_delete = write_baseline(&store, &name, None).await.unwrap();
    assert_live_revisions(&store, &name, None).await;
    assert!(read_history(&store, second).await.unwrap().is_none());
    assert!(
        read_history(&store, quiet_delete).await.unwrap().is_none(),
        "quiet delete invented an event"
    );
    let anchor = write_baseline(&store, &name, Some("c")).await.unwrap();
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item": {"pk": {"S": "key"}, "body": {"S": "ordinary"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let event = ItemHistoryReference::try_new(
        anchor.address().clone(),
        ItemStreamVersion::new(anchor.version().get() + 1),
    )
    .unwrap();
    let published = read_history(&store, event.clone()).await.unwrap().unwrap();
    assert_eq!(
        published.metadata().previous_version(),
        Some(anchor.version())
    );
    assert_eq!(published.metadata().kind(), ItemHistoryKind::Image);
    assert!(
        read_history(&store, anchor.clone())
            .await
            .unwrap()
            .is_some()
    );
    let quiet_delete = write_baseline(&store, &name, None).await.unwrap();
    assert!(read_history(&store, quiet_delete).await.unwrap().is_none());
    assert!(read_history(&store, event.clone()).await.unwrap().is_some());
    assert!(read_history(&store, anchor).await.unwrap().is_some());
    let current = write_baseline(&store, &name, Some("d")).await.unwrap();
    assert!(
        read_history(&store, event.clone()).await.unwrap().is_some(),
        "unretired event lost"
    );
    qualify_retired_release(&store, name, event, current)
        .await
        .unwrap();
}

async fn assert_live_revisions<P: DynamoPort>(store: &P, name: &TableName, expected: Option<u64>) {
    let name = name.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &name).await?;
                let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
                let absent =
                    KeyAttributes::from([("pk".into(), AttributeValue::S("absent".into()))]);
                let revisions = attempt
                    .read_primary_live_revisions(&[
                        (&table, &key),
                        (&table, &absent),
                        (&table, &key),
                    ])
                    .await?;
                assert_eq!(
                    revisions
                        .into_iter()
                        .map(|revision| revision.map(|revision| revision.get()))
                        .collect::<Vec<_>>(),
                    [expected, None, expected]
                );
                assert!(attempt.read_primary_live_revisions(&[]).await?.is_empty());
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn write_baseline<P: DynamoPort>(
    store: &P,
    name: &TableName,
    value: Option<&str>,
) -> StorageResult<ItemHistoryReference> {
    let name = name.clone();
    let value = value.map(|value| value.repeat(100_000));
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &name).await?;
                let key = KeyAttributes::from([("pk".into(), AttributeValue::S("key".into()))]);
                let (observed, lifecycle) = attempt.observe_for_write(&table, &key).await?;
                let revision = match &observed {
                    crate::ItemWriteObservation::Absent => 1,
                    crate::ItemWriteObservation::Live { revision, .. }
                    | crate::ItemWriteObservation::Deleted { revision, .. } => revision.get() + 1,
                };
                let reference = ItemHistoryReference::try_new(
                    stream_provider::dynamo_item_history_address(&table, &key)?,
                    ItemStreamVersion::new(revision),
                )?;
                if let Some(value) = value {
                    let request = serde_json::from_value(serde_json::json!({
                        "TableName": name, "Item": {"pk": {"S": "key"}, "body": {"S": value}}
                    }))
                    .unwrap();
                    let bound = PutItemShape::try_from_request(request)?.bind_table(table, None)?;
                    let (accepted, _) = bound.accept_old(observed, lifecycle)?.into_result()?;
                    crate::prepare_baseline_upsert(attempt, accepted, ItemProvenance::default())
                        .await?;
                } else {
                    let accepted = BoundDeleteItem::for_maintenance(table, key)?
                        .accept_old(observed, lifecycle)?
                        .into_result()?;
                    crate::prepare_baseline_delete(attempt, &accepted, ItemProvenance::default())
                        .await?;
                }
                attempt.flush_dynamo_write().await?;
                Ok(reference)
            })
        })
        .await
}

async fn load_table(
    attempt: &mut impl CatalogAttempt,
    name: &TableName,
) -> StorageResult<Arc<BoundTableInfo>> {
    let stamp = attempt.read_catalog_stamp(name).await?.unwrap();
    attempt.read_table_at_stamp(name, stamp).await
}

async fn read_history<P: DynamoPort>(
    store: &P,
    reference: ItemHistoryReference,
) -> StorageResult<Option<ItemHistoryEntry>> {
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let entries = attempt
                    .load_item_history_entries(ItemHistoryReadBatch {
                        references: &[reference],
                        maximum_bytes: std::num::NonZeroUsize::new(4_000_000).unwrap(),
                    })
                    .await?;
                Ok(entries.into_iter().next().flatten())
            })
        })
        .await
}

async fn qualify_retired_release<P: DynamoPort + DynamoOperations>(
    store: &P,
    name: TableName,
    unretired: ItemHistoryReference,
    current: ItemHistoryReference,
) -> StorageResult<()>
where
    for<'a> P::Attempt<'a>: StreamRetentionPort,
{
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item": {"pk": {"S": "key"}, "body": {"S": "retired"}}
            }))
            .unwrap(),
        )
        .await?;
    let event = ItemHistoryReference::try_new(
        unretired.address().clone(),
        ItemStreamVersion::new(current.version().get() + 1),
    )?;
    let metadata = read_history(store, event.clone())
        .await?
        .unwrap()
        .metadata()
        .clone();
    let retire_name = name.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let table = load_table(attempt, &retire_name).await?;
                let state = attempt
                    .load_table_history_retention(&table, storage_types::ReadProtection::Conflict)
                    .await?
                    .unwrap();
                let mut next = state.clone();
                let ItemHistoryPublication::Event {
                    source_scope,
                    table_position,
                    ..
                } = metadata.publication()
                else {
                    panic!("event")
                };
                assert_eq!(&next.frontier.source_scope, source_scope);
                let lane = next
                    .frontier
                    .lineages
                    .iter_mut()
                    .find(|lane| {
                        &lane.shard_id == table_position.shard_id()
                            && &lane.lineage_id == table_position.lineage_id()
                    })
                    .unwrap();
                lane.retired_through = Some(table_position.offset().clone());
                let marker = crate::StreamTrimDueMarker::new(
                    state.next_due_at.unwrap(),
                    state.scope.clone(),
                    state.policy_version,
                );
                attempt
                    .finish_stream_trim_markers(vec![StreamRetentionFinish {
                        marker: marker.clone(),
                        reason: StreamRetentionFinishReason::Page { deleted_rows: 0 },
                        update: Some(crate::StreamTrimStateUpdate {
                            expected_state: state,
                            write: crate::StreamTrimStateWrite {
                                state: next,
                                next_marker: Some(marker),
                                replaced_marker: None,
                            },
                        }),
                    }])
                    .await
            })
        })
        .await?;
    write_baseline(store, &name, Some("e")).await?;
    assert!(
        read_history(store, event).await?.is_none(),
        "retired former-current leaked"
    );
    assert!(
        read_history(store, unretired).await?.is_some(),
        "release deleted another revision"
    );
    Ok(())
}
