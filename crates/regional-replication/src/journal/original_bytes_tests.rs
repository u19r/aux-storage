use std::sync::Arc;

use replication_types::event::disposition::{CanonicalAcceptanceReceipt, EventDisposition};
use storage_provider::{CatalogAttempt, DynamoOperations, DynamoPort};
use storage_types::{
    AttemptMode, Clock, DeleteTableRequest, PutItemRequest, ReadProtection, TableName,
};

use super::JournalStore;
use crate::test_support as fixture;

#[tokio::test]
async fn given_one_event_in_two_physical_tables_when_one_copy_is_removed_then_the_other_survives_reopen()
 {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.sqlite");
    let source = fixture::open(&path).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &peer]).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("independent-copy"),
        ))
        .await
        .unwrap();
    let event = fixture::frame(&source, 1, 0, 1).await.events()[0].clone();
    let first = fixture::installed_state(&source, 1, fixture::NAME)
        .await
        .physical;
    fixture::create_table(&source, &TableName::new("other-copy")).await;
    let journal = Arc::clone(&cluster.journals[0]);
    let owner = Arc::clone(&journal);
    let original = event.clone();
    let now = source.now();
    let second = source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let name = TableName::new("other-copy");
                let stamp = attempt.read_catalog_stamp(&name).await?.unwrap();
                attempt.store_external_cleanup_barrier(&name, stamp).await?;
                let second = stamp.history_id;
                let mut prepared = owner
                    .prepare_import_events(attempt, &[(&original, second)], &fixture::codec())
                    .await?;
                let pending = prepared.begin_publication(&original).map_err(|error| {
                    error.with_context("a second physical table needs its own event acceptance")
                })?;
                let receipt = CanonicalAcceptanceReceipt {
                    event: original.as_ref().id.clone(),
                    digest: original.as_ref().digest,
                    disposition: EventDisposition::HistoryAccepted,
                };
                owner
                    .accept_import_event(attempt, (&original, &receipt, now), pending)
                    .await?;
                Ok(second)
            })
        })
        .await
        .unwrap();
    assert_ne!(first, second);
    let selector = Arc::clone(&journal);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                for _ in 0..256 {
                    if let Some(scope) = selector.select_pending_delivery(attempt).await?.scope {
                        assert_eq!(scope.physical, first);
                        return Ok(());
                    }
                }
                panic!("newly accepted source data did not create pending delivery");
            })
        })
        .await
        .unwrap();
    cluster.shutdown().await;
    let inventory =
        seed_opaque_family_inventory(&source, Arc::clone(&journal), first, second).await;
    source
        .delete_table(DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    let owner = JournalStore::try_new(
        (
            fixture::region(1),
            [1; 16].try_into().unwrap(),
            3.into(),
            [7; 16].try_into().unwrap(),
        ),
        128.try_into().unwrap(),
        fixture::physical_limits(),
        (fixture::backlog(), 1000.try_into().unwrap()),
    )
    .unwrap();
    let original = event.clone();
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let owner = owner.load_reclaim_generation(attempt, 1.into()).await?;
                assert_eq!(owner.partitions, fixture::partitions());
                assert_eq!(u64::from(owner.generation), 1);
                let (_, pointer) = owner
                    .load_acceptance_with_delivery(
                        attempt,
                        (first, &original.as_ref().id),
                        &fixture::codec(),
                        ReadProtection::Conflict,
                    )
                    .await?
                    .unwrap();
                let mut family = Some(super::table_owned_reclaim::TableOwnedFamily::SourceHold);
                while let Some(current) = family {
                    assert_eq!(
                        owner
                            .reclaim_table_owned_family(attempt, first, current)
                            .await?,
                        storage_provider::TableReclaimStep::Complete
                    );
                    family = current.next();
                }
                assert!(
                    owner
                        .load_receiver_original(
                            attempt,
                            (&pointer.receiver, pointer.position),
                            (&fixture::codec(), ReadProtection::Snapshot)
                        )
                        .await?
                        .is_none()
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(source);
    let reopened = fixture::reopen(&path).await;
    let owner = Arc::clone(&journal);
    reopened
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                require_retained_copy(&owner, attempt, &event, second).await?;
                let keys = inventory
                    .iter()
                    .map(|(key, _)| key.clone())
                    .collect::<Vec<_>>();
                let records = owner
                    .records
                    .load(attempt, &keys, ReadProtection::Snapshot)
                    .await?;
                for ((_, retained), actual) in inventory.into_iter().zip(records) {
                    assert_eq!(
                        actual.is_some(),
                        retained,
                        "family inventory crossed an immutable ID"
                    );
                    if let Some(actual) = actual {
                        assert_eq!(
                            owner.records.decode::<String>(&actual)?,
                            "independent inventory payload"
                        );
                    }
                }
                let selection = owner.select_pending_delivery(attempt).await?;
                assert!(
                    selection.scope.is_none(),
                    "the reclaimed table still has delivery work"
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    let error = reopened
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                journal
                    .load_receiver_original(
                        attempt,
                        (&journal.origin(0u16.into())?, u64::MAX.into()),
                        (&fixture::codec(), ReadProtection::Snapshot),
                    )
                    .await
                    .map(|_| ())
            })
        })
        .await
        .unwrap_err();
    assert!(matches!(
        error.as_ref(),
        storage_types::StorageEnum::HistoryUnavailable
    ));
}

// Special acceptance/receiver, source-hold, partial-import and delivery-current
// records have typed settlement tests. These remaining families promise opaque
// physical erasure: the cleanup owner must not decode their payloads.
async fn seed_opaque_family_inventory(
    store: &fixture::Store,
    journal: Arc<JournalStore>,
    removed: storage_types::TableHistoryId,
    retained: storage_types::TableHistoryId,
) -> Vec<(infrastructure_provider::record::DurableRecordKey, bool)> {
    use crate::constants;
    let mut prefixes = vec![
        constants::IMPORT_COVERAGE_SUFFIX.to_vec(),
        constants::EVENT_SUFFIX.to_vec(),
        constants::BASELINE_SUFFIX.to_vec(),
        constants::IMPORT_PAGE_SUFFIX.to_vec(),
        constants::RETAINED_POSITION_SUFFIX.to_vec(),
        constants::DELIVERY_RECEIPT_SUFFIX.to_vec(),
        constants::DELIVERY_PROGRESS_SUFFIX.to_vec(),
        constants::EXPECTED_WINNER_SUFFIX.to_vec(),
        constants::PROJECTION_OWNER_SUFFIX.to_vec(),
        constants::SOURCE_COPY_SUFFIX.to_vec(),
        constants::SOURCE_COPY_PAGE_SUFFIX.to_vec(),
        constants::SOURCE_TRANSFER_SUFFIX.to_vec(),
        constants::ADOPTED_TRANSFER_SUFFIX.to_vec(),
        constants::RETAINED_TRANSFER_PROGRESS_SUFFIX.to_vec(),
        constants::BASELINE_COPY_SUFFIX.to_vec(),
        constants::CAUSAL_STACK_SUFFIX.to_vec(),
        constants::CAUSAL_VISITED_SUFFIX.to_vec(),
    ];
    for original in [constants::EVENT_SUFFIX, constants::BASELINE_SUFFIX] {
        prefixes.push([constants::ARCHIVE_POINTER_SUFFIX, original].concat());
    }
    assert_eq!(prefixes.len(), 19);
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let mut keys = Vec::new();
                let mut changes = Vec::new();
                for prefix in prefixes {
                    for (physical, is_retained) in [(removed, false), (retained, true)] {
                        let key =
                            journal.table_identity_key(&prefix, physical, &"inventory-probe")?;
                        changes.push(journal.records.prepare(
                            key.clone(),
                            None,
                            &"independent inventory payload",
                        )?);
                        keys.push((key, is_retained));
                    }
                }
                journal.records.store(attempt, &changes).await?;
                Ok(keys)
            })
        })
        .await
        .unwrap()
}

async fn require_retained_copy(
    journal: &JournalStore,
    attempt: &mut impl infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    event: &replication_types::event::content::CanonicalEvent,
    physical: storage_types::TableHistoryId,
) -> storage_types::StorageResult<()> {
    let prepared = journal
        .prepare_import_events(attempt, &[(event, physical)], &fixture::codec())
        .await?;
    let accepted = prepared
        .observed_acceptance(&event.as_ref().id)
        .expect("removing the first copy must preserve the second acceptance");
    assert_eq!(
        accepted.receipt.disposition,
        EventDisposition::HistoryAccepted
    );
    assert_eq!(accepted.event.as_ref().bytes, event.as_ref().bytes);
    Ok(())
}
