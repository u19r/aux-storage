use std::sync::Arc;

use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use storage_provider::{DynamoOperations, DynamoPort, TableAppendOperations};
use storage_types::{AttemptMode, PutItemRequest, ReadProtection, TableName};

use super::JournalStore;
use crate::{history_capacity::HistoryByteAccount, test_support as fixture};

#[tokio::test]
async fn given_full_customer_history_when_acceptance_stops_then_native_recovery_retains_headroom_and_refund_restores_acceptance()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    let mut policy = fixture::backlog();
    let ceiling = u64::try_from(fixture::budgets().attempt.data_bytes.get()).unwrap();
    policy.acceptance_bytes = (ceiling * 2).try_into().unwrap();
    policy.recovery_reserve_bytes = ceiling.try_into().unwrap();
    policy.hot_history_bytes = policy.acceptance_bytes;
    let cluster =
        fixture::NativeCluster::start_with_backlog(&[&source, &destination], policy).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("accepted"),
        ))
        .await
        .unwrap();
    let original = fixture::frame(&source, 1, 0, 1).await;
    let journal = Arc::clone(&cluster.journals[0]);
    // Real charged rows exercise shared storage accounting; never fabricate an
    // account balance.
    let mut charges = Vec::new();
    let mut ordinal = 0u32;
    loop {
        let (_, used) = account(&source, &journal).await;
        if used == ceiling - 128 {
            break;
        }
        let key = journal
            .record_key(
                [
                    b"fixture-capacity-".as_slice(),
                    ordinal.to_be_bytes().as_slice(),
                ]
                .concat(),
            )
            .unwrap();
        let key_bytes =
            u64::try_from(fixture::physical_limits().validate_key(&key).unwrap()).unwrap();
        let size = usize::try_from((ceiling - 128 - used - key_bytes).min(1044480)).unwrap();
        let charge = journal
            .records
            .prepare_bytes(key.clone(), None, vec![0; size].into())
            .unwrap();
        let owner = Arc::clone(&journal);
        source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move { owner.records.store(attempt, &[charge]).await })
            })
            .await
            .unwrap();
        charges.push(key);
        ordinal += 1;
    }
    let before = account(&source, &journal).await;
    let heads = source_heads(&source, &journal).await;
    assert_eq!(before.1, ceiling - 128);
    let error = cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item(&"rejected".repeat(1024)),
        ))
        .await
        .expect_err("customer crossed its retained-history ceiling");
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::LimitExceeded { .. }
    ));
    fixture::assert_item(&source, Some("accepted")).await;
    let after = account(&source, &journal).await;
    assert_eq!(after.0.revision, before.0.revision);
    assert_eq!(
        after.0.bytes, before.0.bytes,
        "aborted customer attempt leaked history charges"
    );
    let retained = fixture::frame(&source, 1, 0, 1).await;
    assert_eq!(
        retained.events()[0].as_ref().bytes,
        original.events()[0].as_ref().bytes
    );
    assert_eq!(
        source_heads(&source, &journal).await,
        heads,
        "rejected customer allocated an original position"
    );
    let required_through = original.manifest().range.through();
    // Charged native setup grows beyond the customer stop without fabricating
    // the account.
    let recovery_key = journal
        .record_key(b"fixture-recovery-headroom".to_vec())
        .unwrap();
    let recovery = journal
        .records
        .prepare_bytes(recovery_key.clone(), None, vec![1; 65536].into())
        .unwrap();
    let owner = Arc::clone(&journal);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { owner.records.store(attempt, &[recovery]).await })
        })
        .await
        .unwrap();
    let (_, used) = account(&source, &journal).await;
    assert!(used > ceiling && used < ceiling * 2);
    for _ in 0..512 {
        cluster.drive_once().await;
        let owner = Arc::clone(&journal);
        let scope = fixture::delivery_scope(&source, &retained).await;
        let through = source
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
        if through == required_through {
            break;
        }
    }
    fixture::assert_item(&destination, Some("accepted")).await;
    let owner = Arc::clone(&journal);
    let scope = fixture::delivery_scope(&source, &retained).await;
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                assert_eq!(
                    u64::from(
                        owner
                            .load_delivery_progress(attempt, &scope)
                            .await?
                            .through()
                    ),
                    u64::from(required_through),
                    "reserved recovery failed to persist the source ACK above the customer stop"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    // Shared native refunds join the actual row removals in one caller-owned
    // attempt per wave.
    charges.push(recovery_key);
    for wave in charges.chunks(8) {
        let keys = wave.to_vec();
        let owner = Arc::clone(&journal);
        source
            .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    let rows = owner
                        .records
                        .load(attempt, &keys, ReadProtection::Conflict)
                        .await?;
                    let changes = rows
                        .into_iter()
                        .map(|row| {
                            let row = row.unwrap();
                            RecordChange::Delete {
                                key: row.key,
                                expected: row.revision,
                            }
                        })
                        .collect::<Vec<_>>();
                    owner.records.store(attempt, &changes).await
                })
            })
            .await
            .unwrap();
    }
    assert!(account(&source, &journal).await.1 < ceiling);
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("accepted-after-refund"),
        ))
        .await
        .unwrap();
    fixture::assert_item(&source, Some("accepted-after-refund")).await;
    cluster.shutdown().await;
}

async fn source_heads(store: &fixture::Store, journal: &Arc<JournalStore>) -> Vec<u64> {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let partitions = (0..64u16).map(Into::into).collect::<Vec<_>>();
                Ok(journal
                    .load_source_heads(attempt, &partitions, ReadProtection::Snapshot)
                    .await?
                    .into_iter()
                    .map(u64::from)
                    .collect())
            })
        })
        .await
        .unwrap()
}

async fn account(store: &fixture::Store, journal: &Arc<JournalStore>) -> (DurableRecord, u64) {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let rows = journal
                    .records
                    .load(
                        attempt,
                        &[journal.records.capacity.root_key().try_into()?],
                        ReadProtection::Snapshot,
                    )
                    .await?;
                let row = rows.into_iter().next().flatten().unwrap();
                let account: HistoryByteAccount = journal.records.decode(&row)?;
                Ok((row, account.bytes))
            })
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn given_materialized_originals_when_public_writes_commit_then_capacity_delta_equals_complete_native_records()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let destination = fixture::open(&directory.path().join("destination.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &destination]).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    let journal = &cluster.journals[0];
    let physical = fixture::installed_state(&source, 1, fixture::NAME)
        .await
        .physical;
    let before = measure_owned_records(&source, journal, physical).await;
    for value in ["first", "second", "third"] {
        cluster.operations[0]
            .put_item(PutItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::item(value),
            ))
            .await
            .unwrap();
        let observed = measure_owned_records(&source, journal, physical).await;
        assert_eq!(
            observed.0 - before.0,
            observed.1 - before.1,
            "capacity must charge complete canonical records, metadata and receipts exactly once"
        );
    }
    verify_retained_generation_accounting(&source, journal, physical).await;
    let frame = fixture::frame(&source, 1, 0, 3).await;
    assert_eq!(frame.events().len(), 3);
    let streams = crate::streams::RegionalStreams {
        port: Arc::new(source.clone()),
        capture: crate::streams::RegionalAppendCapture {
            journal: Arc::clone(journal),
            codec: fixture::codec(),
        },
    };
    let operation = stream_provider::StreamAppendOperationId::fresh();
    let target = storage_types::TableHistoryTarget {
        history_id: physical,
        table_name: TableName::new(fixture::NAME),
    };
    let id = streams
        .append_table_item(operation, target.clone(), b"extension-payload")
        .await
        .unwrap();
    let appended = measure_owned_records(&source, journal, physical).await;
    let replay = streams
        .append_table_item(operation, target, b"extension-payload")
        .await
        .unwrap();
    assert_eq!(id, replay);
    let observed = measure_owned_records(&source, journal, physical).await;
    assert_eq!(
        observed, appended,
        "append replay changed retained records or charges"
    );
    assert_eq!(
        observed.0 - before.0,
        observed.1 - before.1,
        "capacity must include each complete original and receipt exactly once"
    );
}

async fn measure_owned_records(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    physical: storage_types::TableHistoryId,
) -> (u64, u64) {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let account_key = journal.records.capacity.root_key();
                let account = journal
                    .records
                    .load(
                        attempt,
                        &[account_key.try_into()?],
                        ReadProtection::Snapshot,
                    )
                    .await?
                    .pop()
                    .flatten()
                    .unwrap();
                let account: HistoryByteAccount = journal.records.decode(&account)?;
                let resource = journal.record_key(vec![0])?;
                let mut minimum = 1;
                let mut bytes = 0u64;
                for _ in 0..16 {
                    let Some(generation) = attempt
                        .find_record_generation(&resource, minimum, ReadProtection::Snapshot)
                        .await?
                    else {
                        return Ok((account.bytes, bytes));
                    };
                    bytes +=
                        measure_generation_records(&journal, attempt, generation, physical).await?;
                    let Some(next) = generation.checked_add(1) else {
                        return Ok((account.bytes, bytes));
                    };
                    minimum = next;
                }
                panic!("native accounting fixture exceeded generation bound");
            })
        })
        .await
        .unwrap()
}

async fn measure_generation_records(
    journal: &JournalStore,
    attempt: &mut impl infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    generation: u64,
    physical: storage_types::TableHistoryId,
) -> storage_types::StorageResult<u64> {
    let mut first: infrastructure_provider::record::RecordAddress =
        journal.record_key(vec![0])?.into();
    first.generation = generation;
    let mut last = first.clone();
    last.suffix = vec![255];
    let range = infrastructure_provider::page::BoundedRecordRange::try_new(
        first.try_into()?,
        last.try_into()?,
        journal.records.limits.max_records(),
    )?;
    let mut after = None;
    let mut bytes = 0;
    for _ in 0..16 {
        let page = journal
            .records
            .scan(
                attempt,
                infrastructure_provider::page::RecordPageRequest {
                    range: range.clone(),
                    after,
                    max_bytes: journal.records.limits.max_bytes(),
                    protection: ReadProtection::Snapshot,
                },
            )
            .await?;
        for row in page.records {
            if crate::archive::ArchiveKind::from_key(&row.key).is_some() {
                let payload: super::original_payload::OriginalPayload<'_> =
                    journal.records.decode(&row)?;
                assert_eq!(
                    payload.history, physical,
                    "every original must retain its immutable physical table ID"
                );
            }
            if row
                .key
                .as_ref()
                .suffix
                .starts_with(crate::constants::ACCEPTANCE_SUFFIX)
            {
                assert_eq!(
                    journal.decode_native_acceptance(&row)?.delivery.physical,
                    physical
                );
            }
            if row
                .key
                .as_ref()
                .suffix
                .starts_with(crate::constants::CANONICAL_VIEW_SUFFIX)
            {
                assert_eq!(
                    journal.decode_receiver_pointer(&row)?.unwrap().physical,
                    physical
                );
            }
            bytes += journal.records.limits.validate_record(&row)? as u64;
        }
        after = page.continuation;
        if after.is_none() {
            return Ok(bytes);
        }
    }
    panic!("native accounting fixture exceeded record page bound");
}

async fn store_history_change(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    change: RecordChange,
) -> storage_types::StorageResult<()> {
    let journal = Arc::clone(journal);
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { journal.records.store(attempt, &[change]).await })
        })
        .await
}

async fn verify_retained_generation_accounting(
    store: &fixture::Store,
    journal: &Arc<JournalStore>,
    physical: storage_types::TableHistoryId,
) {
    let before = measure_owned_records(store, journal, physical).await;
    let mut address: infrastructure_provider::record::RecordAddress = journal
        .record_key(b"fixture-retained-generation".to_vec())
        .unwrap()
        .into();
    address.generation = 3;
    let key: DurableRecordKey = address.clone().try_into().unwrap();
    let change = journal
        .records
        .prepare(key.clone(), None, &"retained metadata")
        .unwrap();
    store_history_change(store, journal, change).await.unwrap();
    let observed = measure_owned_records(store, journal, physical).await;
    assert!(observed.0 > before.0);
    assert_eq!(observed.0 - before.0, observed.1 - before.1);
    store_history_change(
        store,
        journal,
        RecordChange::Delete {
            key,
            expected: storage_types::Revision::INITIAL,
        },
    )
    .await
    .unwrap();
    assert_eq!(
        measure_owned_records(store, journal, physical).await,
        before
    );
    let mut other_resource = address.clone();
    other_resource.resource.push(0);
    let mut other_namespace = address.clone();
    other_namespace.namespace.push_str("-foreign");
    address.generation = 0;
    for invalid in [other_resource, other_namespace, address] {
        let change = journal
            .records
            .prepare(invalid.try_into().unwrap(), None, &"rejected metadata")
            .unwrap();
        assert!(store_history_change(store, journal, change).await.is_err());
        assert_eq!(
            measure_owned_records(store, journal, physical).await,
            before
        );
    }
}

#[tokio::test]
async fn given_same_baseline_in_two_incarnations_when_one_copy_is_removed_then_other_copy_survives_reopen()
 {
    use replication_types::event::{
        baseline::BaselineItem,
        content::{CanonicalEventCodec, SourceImages, SourceImagesRef},
    };
    use storage_provider::CatalogAttempt;
    use storage_types::{AttributeValue, KeyAttributes};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("copies.sqlite");
    let store = fixture::open(&path).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&store, &peer]).await;
    let first_name = TableName::new("first-copy");
    let second_name = TableName::new("second-copy");
    fixture::create_table(&store, &first_name).await;
    fixture::create_table(&store, &second_name).await;
    let journal = Arc::clone(&cluster.journals[0]);
    let owner = Arc::clone(&journal);
    let (second, identity, bytes) = store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let first = attempt
                    .read_catalog_stamp(&first_name)
                    .await?
                    .unwrap()
                    .history_id;
                let second = attempt
                    .read_catalog_stamp(&second_name)
                    .await?
                    .unwrap()
                    .history_id;
                let key =
                    KeyAttributes::from_iter([("pk".into(), AttributeValue::S("customer".into()))]);
                let codec = fixture::codec();
                let baseline = BaselineItem {
                    source_table: [1; 16].try_into().unwrap(),
                    lineage: [2; 16].try_into().unwrap(),
                    schema: 1.into(),
                    canonical_key: codec.encode_key(&key).unwrap(),
                    key,
                    images: SourceImages {
                        old: None,
                        new: Some(fixture::item("snapshot")),
                        old_indexers: None,
                        new_indexers: Some(Vec::new()),
                    },
                };
                let borrowed = BaselineItem {
                    source_table: baseline.source_table,
                    lineage: baseline.lineage,
                    schema: baseline.schema,
                    canonical_key: baseline.canonical_key.clone(),
                    key: baseline.key.clone(),
                    images: SourceImagesRef::from(&baseline.images),
                };
                let authority = owner
                    .store_baseline(attempt, (&borrowed, first), &codec)
                    .await?;
                owner
                    .retain_received_baseline(attempt, (&authority, second), &codec)
                    .await?;
                let identity = authority.provenance().version;
                assert_ne!(
                    owner.baseline_key(first, &identity)?,
                    owner.baseline_key(second, &identity)?
                );
                let old = owner
                    .records
                    .load(
                        attempt,
                        &[owner.baseline_key(first, &identity)?],
                        ReadProtection::Conflict,
                    )
                    .await?
                    .pop()
                    .flatten()
                    .unwrap();
                owner
                    .records
                    .store(
                        attempt,
                        &[RecordChange::Delete {
                            key: old.key,
                            expected: old.revision,
                        }],
                    )
                    .await?;
                assert!(
                    owner
                        .load_baseline(attempt, (first, &identity), &codec)
                        .await?
                        .is_none()
                );
                assert_eq!(
                    owner
                        .load_baseline(attempt, (second, &identity), &codec)
                        .await?
                        .unwrap()
                        .bytes(),
                    authority.bytes()
                );
                Ok((second, identity, authority.bytes().to_vec()))
            })
        })
        .await
        .unwrap();
    cluster.shutdown().await;
    drop(store);
    let reopened = fixture::reopen(&path).await;
    reopened
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let recovered = journal
                    .load_baseline(attempt, (second, &identity), &fixture::codec())
                    .await?
                    .unwrap();
                assert_eq!(recovered.bytes(), bytes);
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn given_regional_history_when_runtime_is_absent_after_reopen_then_native_cleanup_cannot_finalize_table()
 {
    use storage_provider::{
        CatalogAttempt, TableReclaimAttempt, TableReclaimPhase, TableReclaimRepository,
    };
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("owner.sqlite");
    let store = fixture::open(&path).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&store, &peer]).await;
    cluster.install(&store, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("retained"),
        ))
        .await
        .unwrap();
    let original = fixture::frame(&store, 1, 0, 1).await.events()[0].clone();
    let owner = Arc::clone(&cluster.journals[0]);
    let deleted = store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(attempt
                    .read_catalog_stamp(&TableName::new(fixture::NAME))
                    .await?
                    .unwrap()
                    .history_id)
            })
        })
        .await
        .unwrap();
    cluster.shutdown().await;
    drop(store);
    let reopened = fixture::reopen(&path).await;
    reopened
        .delete_table(storage_types::DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    fixture::create_table(&reopened, &TableName::new(fixture::NAME)).await;
    reopened
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("fresh"),
        ))
        .await
        .unwrap();
    for _ in 0..3 {
        assert!(matches!(
            reopened.reclaim_table_page().await.unwrap_err().as_ref(),
            storage_types::StorageEnum::ServiceUnavailable { .. }
        ));
    }
    reopened
        .with_dynamo_schema_attempt(move |attempt| {
            Box::pin(async move {
                let target = attempt.load_reclaim_target().await?.unwrap();
                assert_eq!(target.history_id, deleted);
                assert_eq!(target.phase, TableReclaimPhase::ExternalData);
                let fresh = attempt
                    .read_catalog_stamp(&TableName::new(fixture::NAME))
                    .await?
                    .unwrap()
                    .history_id;
                assert_ne!(fresh, deleted);
                let accepted = owner
                    .load_acceptance(attempt, (deleted, &original.as_ref().id), &fixture::codec())
                    .await?
                    .unwrap();
                assert_eq!(accepted.event.as_ref().bytes, original.as_ref().bytes);
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture::assert_item(&reopened, Some("fresh")).await;
}
