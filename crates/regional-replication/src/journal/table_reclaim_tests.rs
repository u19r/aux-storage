use std::sync::Arc;

use replication_types::configuration::{installed::InstalledTableState, proposal::ControlProposal};
use storage_provider::{CatalogAttempt, DynamoOperations, DynamoPort};
use storage_types::{
    AttemptMode, Clock, DeleteTableRequest, GetItemRequest, PutItemRequest, ReadProtection,
    TableName,
};

use super::{JournalStore, table_reclaim_test_support::reclaim_deleted_table};
use crate::{bootstrap::HiddenTargetRequest, test_support as fixture};

#[tokio::test]
async fn given_deleted_source_and_target_ids_when_stale_capture_reservation_and_import_resume_then_reused_names_and_unrelated_data_are_unchanged()
 {
    let directory = tempfile::tempdir().unwrap();
    let source = fixture::open(&directory.path().join("source.sqlite")).await;
    let target = fixture::open(&directory.path().join("target.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &target]).await;
    cluster.install(&source, fixture::NAME, &[2]).await;
    let source_state = fixture::installed_state(&source, 1, fixture::NAME).await;
    let target_state = fixture::installed_state(&target, 2, fixture::NAME).await;
    let source_journal = Arc::clone(&cluster.journals[0]);
    let target_journal = Arc::clone(&cluster.journals[1]);
    let (reservation, proposal) =
        load_target_request(&source, Arc::clone(&source_journal), source_state.clone()).await;
    cluster.operations[0]
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("old-target-value"),
        ))
        .await
        .unwrap();
    let mut delivered = false;
    for _ in 0..512 {
        cluster.drive_once().await;
        delivered = target
            .get_item(GetItemRequest::new(
                TableName::new(fixture::NAME),
                fixture::key(),
            ))
            .await
            .unwrap()
            .item
            .is_some();
        if delivered {
            break;
        }
    }
    assert!(delivered, "the target never received its original item");
    let source_operations = Arc::clone(&cluster.operations[0]);
    let target_operations = Arc::clone(&cluster.operations[1]);
    cluster.shutdown().await;
    fixture::create_table(&target, &TableName::new("unrelated")).await;
    target
        .put_item(PutItemRequest::new(
            TableName::new("unrelated"),
            fixture::item("survivor"),
        ))
        .await
        .unwrap();
    let target_replacement = replace_table(&target, "target-replacement").await;
    reclaim_deleted_table(&target, target_operations.as_ref(), target_state.physical).await;
    let owner = Arc::clone(&target_journal);
    let old_request = reservation.clone();
    let now = target.now();
    let rejected = target
        .with_dynamo_schema_attempt(move |attempt| {
            Box::pin(async move {
                assert!(
                    owner
                        .load_target_tombstone(attempt, &old_request, ReadProtection::Conflict)
                        .await?
                );
                owner
                    .reserve_hidden_target(attempt, &old_request, now)
                    .await
            })
        })
        .await;
    assert!(
        rejected.is_err(),
        "a stale target reservation revived its deleted incarnation"
    );
    source_operations
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item("late-source-value"),
        ))
        .await
        .unwrap();
    let late = fixture::frame(&source, 1, 1, 2).await;
    assert_eq!(late.events().len(), 1);
    let event = late.events()[0].as_ref().id.clone();
    assert!(
        crate::import::execute_import(
            &target,
            Arc::clone(&target_journal),
            crate::import::ImportExecution {
                import: fixture::bounded_import(late),
                codec: fixture::codec(),
            }
        )
        .await
        .is_err(),
        "a stale import restored deleted target data"
    );
    let owner = Arc::clone(&target_journal);
    target
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let keys = [
                    owner.table_identity_key(
                        crate::constants::EVENT_SUFFIX,
                        target_state.physical,
                        &event,
                    )?,
                    owner.table_identity_key(
                        crate::constants::ACCEPTANCE_SUFFIX,
                        target_state.physical,
                        &event,
                    )?,
                ];
                assert!(
                    owner
                        .records
                        .load(attempt, &keys, ReadProtection::Snapshot)
                        .await?
                        .iter()
                        .all(Option::is_none)
                );
                Ok(())
            })
        })
        .await
        .unwrap();
    fixture::assert_item(&target, Some("target-replacement")).await;
    require_unrelated_item(&target).await;
    let source_replacement = replace_table(&source, "source-replacement").await;
    reclaim_deleted_table(&source, source_operations.as_ref(), source_state.physical).await;
    let owner = Arc::clone(&source_journal);
    assert!(
        source
            .with_dynamo_schema_attempt(move |attempt| Box::pin(async move {
                owner.begin_source_capture(attempt, &proposal).await
            }))
            .await
            .is_err(),
        "a stale capture recreated its source state"
    );
    assert!(
        source
            .with_dynamo_schema_attempt(move |attempt| Box::pin(async move {
                source_journal
                    .freeze_capture_schema(attempt, &source_state)
                    .await
            }))
            .await
            .is_err(),
        "an old source ID accepted the replacement's catalog"
    );
    fixture::assert_item(&source, Some("source-replacement")).await;
    require_catalog_identity(&source, source_replacement).await;
    fixture::assert_item(&target, Some("target-replacement")).await;
    require_catalog_identity(&target, target_replacement).await;
    require_unrelated_item(&target).await;
}

async fn load_target_request(
    source: &fixture::Store,
    journal: Arc<JournalStore>,
    state: InstalledTableState,
) -> (HiddenTargetRequest, ControlProposal) {
    source
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                let control = journal
                    .load_configuration_control(attempt, &state, ReadProtection::Snapshot)
                    .await?;
                let key = journal.identity_key(
                    crate::constants::ENABLE_OPERATION_SUFFIX,
                    &control.proposal.operation().id,
                )?;
                let row = journal
                    .records
                    .load(attempt, &[key], ReadProtection::Snapshot)
                    .await?
                    .pop()
                    .flatten()
                    .unwrap();
                let record: crate::enable::EnableRecord = journal.records.decode(&row)?;
                Ok((record.plan.targets[0].clone(), control.proposal))
            })
        })
        .await
        .unwrap_or_else(|error| panic!("load target request: {:#?}", error.as_ref()))
}

async fn replace_table(store: &fixture::Store, value: &str) -> storage_types::TableHistoryId {
    store
        .delete_table(DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    fixture::create_table(store, &TableName::new(fixture::NAME)).await;
    store
        .put_item(PutItemRequest::new(
            TableName::new(fixture::NAME),
            fixture::item(value),
        ))
        .await
        .unwrap();
    store
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
        .unwrap()
}

async fn require_unrelated_item(store: &fixture::Store) {
    let observed = store
        .get_item(GetItemRequest::new(
            TableName::new("unrelated"),
            fixture::key(),
        ))
        .await
        .unwrap()
        .item
        .unwrap();
    assert_eq!(
        observed.into_attribute_map().unwrap(),
        fixture::item("survivor").into_attribute_map().unwrap()
    );
}

async fn require_catalog_identity(store: &fixture::Store, expected: storage_types::TableHistoryId) {
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                assert_eq!(
                    attempt
                        .read_catalog_stamp(&TableName::new(fixture::NAME))
                        .await?
                        .unwrap()
                        .history_id,
                    expected
                );
                Ok(())
            })
        })
        .await
        .unwrap();
}
