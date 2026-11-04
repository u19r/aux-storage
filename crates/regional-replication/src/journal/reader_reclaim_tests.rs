use std::sync::Arc;

use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{
    configuration::installed::InstalledTableState, event::content::CanonicalEventCodec,
};
use storage_provider::{DynamoOperations, DynamoPort};
use storage_types::{AttemptMode, Clock, DeleteTableRequest, ReadProtection, TableName};

use super::{JournalStore, table_metadata_reclaim::TableMetadataFamily};
use crate::{
    history::{
        causal_stream::{
            CanonicalCausalStreamAction, CanonicalCausalStreamId, CanonicalCausalStreamRequest,
        },
        checkpoint::{
            CanonicalCheckpointAction, CanonicalCheckpointId, CanonicalCheckpointRequest,
        },
        portable_checkpoint::{PortableCheckpointAction, PortableCheckpointRequest},
        retention::CheckpointRetention,
        selection::CanonicalViewSelection,
        view::CanonicalViewRequest,
    },
    test_support as fixture,
};

#[tokio::test]
async fn given_held_released_and_expired_readers_when_one_table_is_reclaimed_then_its_roots_and_indexes_disappear_and_peer_readers_survive_reopen()
 {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("source.sqlite");
    let source = fixture::open(&path).await;
    let peer = fixture::open(&directory.path().join("peer.sqlite")).await;
    let cluster = fixture::NativeCluster::start(&[&source, &peer]).await;
    let mut selections = Vec::new();
    for name in [fixture::NAME, "independent-readers"] {
        cluster.install(&source, name, &[2]).await;
        let state = fixture::installed_state(&source, 1, name).await;
        let keys = create_readers(&cluster, &source, &state).await;
        selections.push((state.physical, keys));
    }
    let journal = Arc::clone(&cluster.journals[0]);
    cluster.shutdown().await;
    let owner = Arc::clone(&journal);
    let expired_at = source.now().checked_add(5_400_000).unwrap().into();
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                while owner.settle_next_reader_expiry(attempt, expired_at).await? {}
                Ok(())
            })
        })
        .await
        .unwrap();
    let first = selections[0].clone();
    let second = selections[1].clone();
    let before = load_readers(&source, Arc::clone(&journal), second.1.clone()).await;
    source
        .delete_table(DeleteTableRequest {
            table_name: TableName::new(fixture::NAME),
        })
        .await
        .unwrap();
    let owner = Arc::clone(&journal);
    source
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move {
                let mut family = Some(TableMetadataFamily::CausalStream);
                while let Some(current) = family {
                    let mut after = None;
                    loop {
                        after = owner
                            .reclaim_table_metadata(attempt, first.0, (current, after))
                            .await?;
                        if after.is_none() {
                            break;
                        }
                    }
                    family = current.next();
                }
                Ok(())
            })
        })
        .await
        .unwrap();
    drop(source);
    let reopened = fixture::reopen(&path).await;
    let removed = load_readers(&reopened, Arc::clone(&journal), selections[0].1.clone()).await;
    assert!(
        removed.iter().all(Option::is_none),
        "deleted reader roots, holds or deadlines survived"
    );
    assert_eq!(
        load_readers(&reopened, journal, second.1).await,
        before,
        "another physical table lost reader state"
    );
}

async fn create_readers(
    cluster: &fixture::NativeCluster,
    store: &fixture::Store,
    state: &InstalledTableState,
) -> Vec<DurableRecordKey> {
    let journal = &cluster.journals[0];
    let mut keys = Vec::new();
    let mut held = None;
    for (seconds, release) in [(7200, false), (3600, true), (3600, false)] {
        let operation = CanonicalCheckpointId::try_from(uuid::Uuid::new_v4()).unwrap();
        let deadline = store.now().checked_add(seconds * 1000).unwrap().into();
        let retention = CheckpointRetention::Until(deadline);
        drop(
            cluster.history[0]
                .checkpoint(CanonicalCheckpointRequest {
                    view: view(state),
                    operation,
                    action: CanonicalCheckpointAction::Create,
                    retention,
                })
                .await
                .unwrap()
                .unwrap(),
        );
        if release {
            drop(
                cluster.history[0]
                    .checkpoint(CanonicalCheckpointRequest {
                        view: view(state),
                        operation,
                        action: CanonicalCheckpointAction::Release,
                        retention,
                    })
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        if seconds == 7200 {
            held = Some(operation);
        }
        append_reader_keys(
            journal,
            &mut keys,
            state,
            (operation, retention),
            super::reader_hold::ReaderHoldKind::Receiver,
        );
    }
    let mut portable = None;
    for (retention, release) in [
        (CheckpointRetention::ExplicitRelease, false),
        (
            CheckpointRetention::Until(store.now().checked_add(3_600_000).unwrap().into()),
            true,
        ),
        (
            CheckpointRetention::Until(store.now().checked_add(3_600_000).unwrap().into()),
            false,
        ),
    ] {
        let operation = CanonicalCheckpointId::try_from(uuid::Uuid::new_v4()).unwrap();
        drop(
            cluster.history[0]
                .portable_checkpoint(PortableCheckpointRequest {
                    serving_region: fixture::region(1),
                    serving_group: [1; 16].try_into().unwrap(),
                    table: state.name.clone(),
                    lineage: state.configuration.lineage,
                    membership: state.membership,
                    portable_checkpoint: operation,
                    action: PortableCheckpointAction::CreateCapture,
                    retention,
                })
                .await
                .unwrap()
                .unwrap(),
        );
        if release {
            drop(
                cluster.history[0]
                    .portable_checkpoint(PortableCheckpointRequest {
                        serving_region: fixture::region(1),
                        serving_group: [1; 16].try_into().unwrap(),
                        table: state.name.clone(),
                        lineage: state.configuration.lineage,
                        membership: state.membership,
                        portable_checkpoint: operation,
                        action: PortableCheckpointAction::Release,
                        retention,
                    })
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        if retention == CheckpointRetention::ExplicitRelease {
            portable = Some(operation);
        }
        append_reader_keys(
            journal,
            &mut keys,
            state,
            (operation, retention),
            super::reader_hold::ReaderHoldKind::Source,
        );
    }
    let operation = CanonicalCausalStreamId::try_from(uuid::Uuid::new_v4()).unwrap();
    let mut request_view = view(state);
    request_view.checkpoint = held;
    drop(
        cluster.history[0]
            .causal_stream(CanonicalCausalStreamRequest {
                view: request_view,
                portable_checkpoint: portable.unwrap(),
                causal_stream: operation,
                action: CanonicalCausalStreamAction::Start,
                ancestry_scopes: Default::default(),
            })
            .await
            .unwrap(),
    );
    keys.push(
        journal
            .identity_key(crate::constants::CAUSAL_STREAM_SUFFIX, &operation)
            .unwrap(),
    );
    keys
}

fn view(state: &InstalledTableState) -> CanonicalViewRequest {
    let key = fixture::codec().encode_key(&fixture::key()).unwrap();
    let partition = fixture::partitions().select(state.configuration.lineage, &key);
    CanonicalViewRequest {
        serving_region: fixture::region(1),
        serving_group: [1; 16].try_into().unwrap(),
        table: state.name.clone(),
        lineage: state.configuration.lineage,
        membership: state.membership,
        receiver: fixture::journal(1).origin(partition).unwrap(),
        selection: CanonicalViewSelection::Item { key },
        checkpoint: None,
        after: None,
    }
}

fn append_reader_keys(
    journal: &JournalStore,
    keys: &mut Vec<DurableRecordKey>,
    state: &InstalledTableState,
    reader: (CanonicalCheckpointId, CheckpointRetention),
    kind: super::reader_hold::ReaderHoldKind,
) {
    let suffix = match kind {
        super::reader_hold::ReaderHoldKind::Receiver => {
            crate::constants::CANONICAL_CHECKPOINT_SUFFIX
        }
        super::reader_hold::ReaderHoldKind::Source => crate::constants::PORTABLE_CHECKPOINT_SUFFIX,
    };
    keys.push(journal.identity_key(suffix, &reader.0).unwrap());
    let reference = super::reader_hold::ReaderHoldReference {
        kind,
        operation: reader.0,
        lineage: state.configuration.lineage,
    };
    keys.push(journal.reader_hold_key(&reference).unwrap());
    if let CheckpointRetention::Until(deadline) = reader.1 {
        keys.push(
            journal
                .reader_expiry_key(super::reader_expiry::ReaderExpiryEntry {
                    reference,
                    deadline,
                })
                .unwrap(),
        );
    }
}

async fn load_readers(
    store: &fixture::Store,
    journal: Arc<JournalStore>,
    keys: Vec<DurableRecordKey>,
) -> Vec<Option<(storage_types::Revision, Vec<u8>)>> {
    store
        .with_dynamo_attempt(AttemptMode::ReadOnly, move |attempt| {
            Box::pin(async move {
                Ok(attempt
                    .load_records(&keys, ReadProtection::Snapshot, journal.physical_limits())
                    .await?
                    .into_iter()
                    .map(|row| row.map(|row| (row.revision, row.bytes.as_ref().to_vec())))
                    .collect())
            })
        })
        .await
        .unwrap()
}
