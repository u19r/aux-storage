//! Shared native proof for complete table-history deletion and source
//! continuity.

use std::num::NonZeroU32;

use storage_types::{
    AttemptMode, DeadlineClass, DeleteTableRequest, TableHistoryId, TableHistoryState,
    TableHistoryTarget,
};
use stream_provider::{PositionedStreamEntry, StreamAttempt};

use crate::{
    CatalogAttempt, DynamoOperations, DynamoPort, MaintenanceOperations, ReclaimAttempt,
    TableReclaimStep,
};

pub async fn qualify_table_history_reclaim<
    P: DynamoPort + DynamoOperations + MaintenanceOperations,
>(
    store: &P,
) -> TableHistoryId
where for<'a> P::Attempt<'a>: ReclaimAttempt + crate::maintenance::StreamRetentionPort {
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
        .filter(|record| record.history_id == deleted.history_id)
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
    // Two 64-entry pages, then one page per remaining phase; SQL drops the
    // item family in one page, so 8 is the smallest provider count.
    assert!(
        pages >= 8,
        "fixture must require bounded entry and table family pages"
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
    let survivors = before
        .into_iter()
        .filter(|entry| entry.history_id != deleted.history_id)
        .collect::<Vec<_>>();
    assert_eq!(after.len(), 2);
    for (before, after) in survivors.iter().zip(&after) {
        assert_eq!(before.history_id, after.history_id);
        assert_eq!(before.position, after.position);
        assert_eq!(
            before.encode_value().unwrap(),
            after.encode_value().unwrap()
        );
    }
    deleted.history_id
}

/// Native cleanup must wait for its external owner, including after ordinary
/// metadata updates and immediate public-name reuse.
pub async fn qualify_external_cleanup_fence<
    P: DynamoPort + DynamoOperations + MaintenanceOperations,
>(
    store: &P,
) where for<'a> P::Attempt<'a>: ReclaimAttempt {
    let deleted = create_table(store, "external-cleanup-fence").await;
    let name = deleted.table_name.clone();
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                attempt.store_external_cleanup_barrier(&name, stamp).await?;
                Ok(())
            })
        },
    )
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

/// A cleanup barrier whose stamp no longer names the live catalog record is
/// one conflict on every family, whatever the cause: absent, deleted or stale.
/// Each barrier is one native attempt with no retry.
pub async fn qualify_external_cleanup_barrier_misses<P: DynamoPort + DynamoOperations>(store: &P)
where for<'a> P::Attempt<'a>: ReclaimAttempt {
    let absent = create_table(store, "barrier-absent").await;
    let mut stamp = read_stamp(store, &absent).await;
    // A KV table id no table has; a UUID no SQL row has.
    stamp.history_id = TableHistoryId::from(u32::MAX - 7);
    assert_barrier_conflict(store_barrier(store, &absent, stamp).await);

    let deleted = create_table(store, "barrier-deleted").await;
    let stamp = read_stamp(store, &deleted).await;
    store
        .delete_table(DeleteTableRequest {
            table_name: deleted.table_name.clone(),
        })
        .await
        .unwrap();
    assert_barrier_conflict(store_barrier(store, &deleted, stamp).await);

    let stale = create_table(store, "barrier-stale").await;
    let stamp = read_stamp(store, &stale).await;
    store_barrier(store, &stale, stamp).await.unwrap();
    assert_barrier_conflict(store_barrier(store, &stale, stamp).await);
}

async fn read_stamp<P: DynamoPort + DynamoOperations>(
    store: &P,
    table: &TableHistoryTarget,
) -> crate::CatalogStamp
where
    for<'a> P::Attempt<'a>: ReclaimAttempt,
{
    let name = table.table_name.clone();
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move { Ok(attempt.read_catalog_stamp(&name).await?.unwrap()) })
        },
    )
    .await
    .unwrap()
}

async fn store_barrier<P: DynamoPort + DynamoOperations>(
    store: &P,
    table: &TableHistoryTarget,
    stamp: crate::CatalogStamp,
) -> storage_types::StorageResult<()>
where
    for<'a> P::Attempt<'a>: ReclaimAttempt,
{
    let name = table.table_name.clone();
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                attempt.store_external_cleanup_barrier(&name, stamp).await?;
                Ok(())
            })
        },
    )
    .await
}

fn assert_barrier_conflict(result: storage_types::StorageResult<()>) {
    let Err(error) = result else {
        panic!("a cleanup barrier with an unmatched stamp must fail");
    };
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    assert!(!matches!(
        error.as_ref(),
        storage_types::StorageEnum::ResourceNotFound { .. }
    ));
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

async fn load_source_rows<P: DynamoPort>(store: &P) -> Vec<PositionedStreamEntry> {
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let source = attempt.read_system_page_start().await?;
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
                                .records
                                .iter()
                                .map(|(entry, _)| stream_provider::SystemStreamHistoryRecord {
                                    position: entry.position.clone(),
                                })
                                .collect(),
                        },
                    )
                    .collect::<Vec<_>>();
                plan.finish(&observations).unwrap();
                Ok(pages
                    .into_iter()
                    .flat_map(|page| page.records)
                    .map(|(entry, _)| entry)
                    .collect())
            })
        },
    )
    .await
    .unwrap()
}

async fn assert_deleting<P: DynamoPort>(store: &P, history: TableHistoryId) {
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let states = attempt.load_table_history_states(&[history]).await?;
                assert!(matches!(
                    states.as_slice(),
                    [Some(TableHistoryState::Deleting)]
                ));
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

async fn assert_source_count<P: DynamoPort>(store: &P, history: TableHistoryId, count: usize)
where for<'a> P::Attempt<'a>: ReclaimAttempt {
    let entries = load_source_rows(store).await;
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.history_id == history)
            .count(),
        count
    );
}

async fn assert_reclaimed<P: DynamoPort>(
    store: &P,
    deleted: &TableHistoryTarget,
    survivors: &[TableHistoryTarget; 2],
    records: Vec<PositionedStreamEntry>,
) where
    for<'a> P::Attempt<'a>: ReclaimAttempt + crate::maintenance::StreamRetentionPort,
{
    let deleted = deleted.clone();
    let survivors = survivors.clone();
    crate::DynamoPort::with_attempt(
        store,
        storage_types::AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                let reads = table_history_reads(&records);
                let pages = stream_provider::TableHistoryIndexAttempt::load_table_history_rows(
                    attempt, &reads,
                )
                .await?;
                assert!(
                    pages
                        .iter()
                        .all(|page| page.entries.is_empty() && !page.more)
                );
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
                    .load_table_history_states(&[
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
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

fn table_history_reads(
    records: &[PositionedStreamEntry],
) -> Vec<stream_provider::TableHistoryIndexRead> {
    let mut reads = Vec::<stream_provider::TableHistoryIndexRead>::new();
    for record in records {
        let lane = storage_types::TableHistoryLane::try_from((
            record.history_id,
            &record.source_scope,
            &record.position,
        ))
        .unwrap();
        let through =
            storage_types::TableStreamSequence::try_from(record.position.offset()).unwrap();
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

/// SYSTEM retirement preserves the independent Forever table policy.
pub async fn qualify_system_trim<
    P: DynamoPort + DynamoOperations + MaintenanceOperations + stream_provider::StreamOperations,
>(
    store: &P,
) where for<'a> P::Attempt<'a>: stream_provider::StreamAttempt {
    use stream_provider::{SystemStreamPageRequest, SystemStreamTrimPageRequest};
    let name = "system-trim-forever";
    store
        .create_table(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "KEYS_ONLY"},
                "AuxStreamDurationHours": -1
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    for key in ["first", "second"] {
        store
            .put_item(
                serde_json::from_value(serde_json::json!({
                    "TableName": name, "Item": {"pk": {"S": key}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let scope = store.system_stream_scope().await.unwrap();
    let before = store
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 1).unwrap())
        .await
        .unwrap();
    let stale = before.issued_checkpoint().token().clone();
    let request =
        SystemStreamTrimPageRequest::try_new(scope.clone(), (i64::MAX - 1).into(), 1, None)
            .unwrap();
    let first = trim_page(store, request.clone()).await;
    assert_eq!(first.reclaimed_records(), 1);
    assert_eq!(trim_page(store, request).await, first);
    let next = SystemStreamTrimPageRequest::try_new(
        scope.clone(),
        (i64::MAX - 1).into(),
        1,
        first.next_continuation().cloned(),
    )
    .unwrap();
    let second = trim_page(store, next.clone()).await;
    assert_eq!(second.reclaimed_records(), 1);
    assert_eq!(trim_page(store, next).await, second);
    assert!(second.next_continuation().is_none());
    let repeated = trim_page(
        store,
        SystemStreamTrimPageRequest::try_new(scope.clone(), i64::MAX.into(), 1, None).unwrap(),
    )
    .await;
    assert_eq!(
        repeated.scanned_records(),
        0,
        "retired canonical rows must not be rescanned"
    );
    assert!(
        store
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), Some(stale), 10).unwrap()
            )
            .await
            .is_err()
    );
    let empty = store
        .get_system_stream_page(SystemStreamPageRequest::try_new(scope.clone(), None, 10).unwrap())
        .await
        .unwrap();
    assert!(empty.records().is_empty());
    let table = store
        .get_stream_records(
            serde_json::from_value(serde_json::json!({"TableName": name, "Limit": 10})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        table.records().len(),
        2,
        "SYSTEM trim deleted Forever table entries"
    );
    store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item": {"pk": {"S": "later"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let later = store
        .get_system_stream_page(
            SystemStreamPageRequest::try_new(
                scope,
                Some(empty.issued_checkpoint().token().clone()),
                10,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(later.records().len(), 1);
    assert_eq!(
        later.records()[0].keys.get("pk"),
        Some(&storage_types::AttributeValue::S("later".to_owned()))
    );
}

async fn trim_page<P: DynamoPort + MaintenanceOperations>(
    store: &P,
    request: stream_provider::SystemStreamTrimPageRequest,
) -> stream_provider::SystemStreamTrimPageResult
where
    for<'a> P::Attempt<'a>: stream_provider::StreamAttempt,
{
    MaintenanceOperations::trim_system_stream_page(store, request)
        .await
        .unwrap()
}
