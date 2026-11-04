//! Shared native proof for complete table-history deletion and source
//! continuity.

use std::num::NonZeroU32;

use storage_types::{
    AttemptMode, DeleteTableRequest, ItemHistoryReference, SystemHistoryRecord, TableHistoryId,
    TableHistoryState, TableHistoryTarget,
};
use stream_provider::{ItemHistoryReadAttempt, ItemHistoryReadBatch, SystemHistoryReadAttempt};

use crate::{
    CatalogAttempt, DynamoOperations, DynamoPort, TableReclaimAttempt, TableReclaimRepository,
    TableReclaimStep,
};

pub async fn qualify_table_history_reclaim<P: DynamoPort + DynamoOperations>(
    store: &P,
) -> TableHistoryId
where for<'a> P::Attempt<'a>: TableReclaimAttempt + crate::maintenance::StreamRetentionPort {
    let deleted = create_table(store, "history-reclaim-deleted").await;
    let peer = create_table(store, "history-reclaim-peer").await;
    store.update_time_to_live(serde_json::from_value(serde_json::json!({
        "TableName": deleted.table_name, "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expires"}
    })).unwrap()).await.unwrap();
    for index in 0..66 {
        let key = format!("{index}/{}", "k".repeat(index * 17));
        put_item(store, &deleted, &key).await;
    }
    put_item(store, &peer, "peer").await;
    let before = load_source_rows(store).await;
    assert_eq!(before.len(), 67);
    let records = before
        .iter()
        .filter(|record| record.table_history_id() == Some(deleted.history_id))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 66);
    store
        .delete_table(DeleteTableRequest {
            table_name: deleted.table_name.clone(),
        })
        .await
        .unwrap();
    assert_deleting(store, deleted.history_id).await;
    let replacement = create_table(store, deleted.table_name.as_ref()).await;
    assert_ne!(replacement.history_id, deleted.history_id);
    put_item(store, &replacement, "0/").await;
    let before = load_source_rows(store).await;
    assert_eq!(before.len(), 68);
    // Metadata transition followed by one bounded source page leaves two
    // references.
    for _ in 0..2 {
        assert_eq!(
            store.reclaim_table_page().await.unwrap(),
            TableReclaimStep::Advanced
        );
    }
    assert_deleting(store, deleted.history_id).await;
    assert_source_count(store, deleted.history_id, 2).await;
    let mut pages = 2;
    for _ in 0..200 {
        let step = store.reclaim_table_page().await.unwrap();
        if step == TableReclaimStep::Complete {
            break;
        }
        pages += 1;
    }
    assert!(
        pages > 12,
        "fixture must require several source and history pages"
    );
    assert_eq!(
        store.reclaim_table_page().await.unwrap(),
        TableReclaimStep::Complete
    );
    assert_reclaimed(store, &deleted, &[peer, replacement.clone()], records).await;
    let current = store
        .get_item(
            serde_json::from_value(serde_json::json!({
                "TableName": replacement.table_name, "Key": {"pk": {"S": "0/"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        current.item.is_some(),
        "old-ID cleanup removed the replacement item"
    );
    let after = load_source_rows(store).await;
    assert_eq!(after.len(), before.len());
    for (before, after) in before.iter().zip(&after) {
        assert_eq!(before.position(), after.position());
        assert_eq!(before.created_at(), after.created_at());
        if before.table_history_id() == Some(deleted.history_id) {
            assert!(after.pointer().is_none());
        } else {
            assert_eq!(before, after);
        }
    }
    deleted.history_id
}

/// Native cleanup must wait for its external owner, including after ordinary
/// metadata updates and immediate public-name reuse.
pub async fn qualify_external_cleanup_fence<P: DynamoPort + DynamoOperations>(store: &P)
where for<'a> P::Attempt<'a>: TableReclaimAttempt {
    let deleted = create_table(store, "external-cleanup-fence").await;
    let name = deleted.table_name.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                attempt.store_external_cleanup_barrier(&name, stamp).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    store
        .update_table(
            serde_json::from_value(serde_json::json!({
                "TableName": deleted.table_name, "AuxStreamDurationHours": 48
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    store
        .delete_table(DeleteTableRequest {
            table_name: deleted.table_name.clone(),
        })
        .await
        .unwrap();
    let replacement = create_table(store, deleted.table_name.as_ref()).await;
    assert_ne!(replacement.history_id, deleted.history_id);
    put_item(store, &replacement, "fresh").await;
    for _ in 0..3 {
        assert!(matches!(
            store.reclaim_table_page().await.unwrap_err().as_ref(),
            storage_types::StorageEnum::ServiceUnavailable { .. }
        ));
        assert_deleting(store, deleted.history_id).await;
    }
    let current = store
        .get_item(
            serde_json::from_value(serde_json::json!({
                "TableName": replacement.table_name, "Key": {"pk": {"S": "fresh"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(current.item.is_some());
}

async fn create_table<P: DynamoOperations>(store: &P, name: &str) -> TableHistoryTarget {
    let request = serde_json::from_value(serde_json::json!({
        "TableName": name, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))
    .unwrap();
    let created = store.create_table(request).await.unwrap();
    TableHistoryTarget::try_from(&created.table_description).unwrap()
}

async fn put_item<P: DynamoOperations>(store: &P, table: &TableHistoryTarget, key: &str) {
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": table.table_name, "Item": {"pk": {"S": key}, "body": {"S": "snapshot"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn load_source_rows<P: DynamoPort>(store: &P) -> Vec<SystemHistoryRecord> {
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let (source, _) = attempt.read_system_page_start(None).await?;
                let initial = source.initial_checkpoint().unwrap();
                let high = source.capture_high().unwrap();
                let plan = source.plan_page(&initial, &high, 200).unwrap();
                let pages = attempt.read_system_history_pages(plan.as_ref()).await?;
                let observations = pages
                    .iter()
                    .zip(plan.as_ref())
                    .map(
                        |(page, read)| stream_provider::SystemStreamHistoryObservation {
                            read,
                            retired_through: page.retired_through.clone(),
                            records: page
                                .observations
                                .iter()
                                .map(|row| stream_provider::SystemStreamHistoryRecord {
                                    position: row.position.clone(),
                                })
                                .collect(),
                        },
                    )
                    .collect::<Vec<_>>();
                plan.finish(&observations).unwrap();
                Ok(pages.into_iter().flat_map(|page| page.records).collect())
            })
        })
        .await
        .unwrap()
}

async fn assert_deleting<P: DynamoPort>(store: &P, history: TableHistoryId) {
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let states = attempt.load_item_history_table_states(&[history]).await?;
                assert!(matches!(
                    states.as_slice(),
                    [Some(TableHistoryState::Deleting)]
                ));
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn assert_source_count<P: DynamoPort>(store: &P, history: TableHistoryId, count: usize)
where for<'a> P::Attempt<'a>: TableReclaimAttempt {
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let page = attempt
                    .load_table_system_rows(history, NonZeroU32::new(100).unwrap())
                    .await?;
                assert_eq!(page.records.len(), count);
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn assert_reclaimed<P: DynamoPort>(
    store: &P,
    deleted: &TableHistoryTarget,
    survivors: &[TableHistoryTarget; 2],
    records: Vec<SystemHistoryRecord>,
) where
    for<'a> P::Attempt<'a>: TableReclaimAttempt + crate::maintenance::StreamRetentionPort,
{
    let deleted = deleted.clone();
    let survivors = survivors.clone();
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let references = records
                    .iter()
                    .map(|record| record.pointer().unwrap().metadata().reference().clone())
                    .collect::<Vec<ItemHistoryReference>>();
                let reads = table_history_reads(&records);
                let pages = stream_provider::TableHistoryIndexAttempt::load_table_history_rows(
                    attempt, &reads,
                )
                .await?;
                assert!(pages.iter().all(|page| page.rows.is_empty() && !page.more));
                let scopes = std::iter::once(&deleted)
                    .chain(survivors.iter())
                    .map(|target| {
                        crate::StreamTrimScope::table(
                            crate::table_retention_scope_id(target.history_id),
                            target.table_name.clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                let policies = crate::maintenance::StreamRetentionPort::load_stream_trim_states(
                    attempt, &scopes,
                )
                .await?;
                assert!(policies[0].is_none() && policies[1].is_some() && policies[2].is_some());
                let states = attempt
                    .load_item_history_table_states(&[
                        deleted.history_id,
                        survivors[0].history_id,
                        survivors[1].history_id,
                    ])
                    .await?;
                assert!(matches!(
                    states.as_slice(),
                    [
                        None,
                        Some(TableHistoryState::Active(_)),
                        Some(TableHistoryState::Active(_))
                    ]
                ));
                let deleted_source = attempt
                    .load_table_system_rows(deleted.history_id, NonZeroU32::new(100).unwrap())
                    .await?;
                assert!(deleted_source.records.is_empty() && !deleted_source.more);
                for survivor in survivors {
                    let source = attempt
                        .load_table_system_rows(survivor.history_id, NonZeroU32::new(100).unwrap())
                        .await?;
                    assert_eq!(source.records.len(), 1);
                }
                let entries = attempt
                    .load_item_history_entries(ItemHistoryReadBatch {
                        references: &references,
                        maximum_bytes: crate::constants::TABLE_HISTORY_READ_BYTES,
                    })
                    .await?;
                assert!(entries.iter().all(Option::is_none));
                Ok(())
            })
        })
        .await
        .unwrap();
}

fn table_history_reads(
    records: &[SystemHistoryRecord],
) -> Vec<stream_provider::TableHistoryIndexRead> {
    let mut reads = Vec::<stream_provider::TableHistoryIndexRead>::new();
    for record in records {
        let pointer = record.pointer().unwrap();
        let storage_types::ItemHistoryPublication::Event {
            source_scope,
            table_position,
            ..
        } = pointer.metadata().publication()
        else {
            panic!("source event");
        };
        let lane = storage_types::TableHistoryLane::try_from((
            pointer.metadata().address().history_id,
            source_scope,
            table_position,
        ))
        .unwrap();
        let through =
            storage_types::TableStreamSequence::try_from(table_position.offset()).unwrap();
        if let Some(existing) = reads.iter_mut().find(|read| read.lane == lane) {
            existing.through = existing.through.max(through);
        } else {
            reads.push(stream_provider::TableHistoryIndexRead {
                lane,
                through,
                exclusive_start: None,
                direction: stream_provider::ReadDirection::Forward,
                limit: NonZeroU32::new(100).unwrap(),
            });
        }
    }
    reads
}
