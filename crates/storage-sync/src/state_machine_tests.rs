use std::sync::{Arc, Mutex};

use openraft::{
    Entry, EntryPayload, LeaderId, LogId, SnapshotMeta, StoredMembership, storage::RaftStateMachine,
};
use storage_backfill::{
    LogicalBackfillActivationGate, LogicalBackfillCaller, LogicalBackfillChecksum,
    LogicalBackfillChunk, LogicalBackfillChunkId, LogicalBackfillChunkSummary,
    LogicalBackfillDomain, LogicalBackfillId, LogicalBackfillImport, LogicalBackfillManifest,
    LogicalBackfillRecord, LogicalBackfillResult, SyncLearnerCatchupPolicy,
};
use storage_types::{ItemStreamVersion, StorageResult};

use crate::{
    ResolvedSyncMutationBatch, SyncApply, SyncCommitMetadata, SyncMutationResponse,
    SyncRaftRequest, SyncRaftSnapshotPayload, SyncRaftStateMachine, SyncSnapshotInstallPhase,
    SyncTypeConfig,
};

#[derive(Default)]
struct RecordingApply {
    applied: Mutex<Vec<SyncCommitMetadata>>,
    imported_chunks: Mutex<Vec<LogicalBackfillChunk>>,
    fail_import: bool,
}

#[async_trait::async_trait]
impl SyncApply for RecordingApply {
    async fn apply_resolved_sync_mutations(
        &self,
        metadata: SyncCommitMetadata,
        _batch: ResolvedSyncMutationBatch,
    ) -> storage_types::StorageResult<Vec<SyncMutationResponse>> {
        self.applied.lock().unwrap().push(metadata.clone());
        Ok(vec![SyncMutationResponse {
            response_json: Some(format!(
                "{}:{}",
                metadata.log_id.term, metadata.log_id.index
            )),
        }])
    }
}

#[async_trait::async_trait]
impl LogicalBackfillImport for RecordingApply {
    async fn import_logical_chunk(
        &self,
        _manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> StorageResult<LogicalBackfillResult> {
        if self.fail_import {
            return Err(storage_types::StorageError::internal("blocked import"));
        }
        self.imported_chunks.lock().unwrap().push(chunk);
        Ok(LogicalBackfillResult::ChunkImported)
    }
}

#[tokio::test]
async fn raft_state_machine_applies_resolved_batches_and_tracks_log_id() {
    let mut state_machine = SyncRaftStateMachine::new(Arc::new(RecordingApply::default()));
    let log_id = LogId {
        leader_id: LeaderId::new(3, 7),
        index: 11,
    };
    let entry = Entry {
        log_id,
        payload: EntryPayload::Normal(SyncRaftRequest::new(ResolvedSyncMutationBatch::new(
            Vec::new(),
        ))),
    };

    let responses =
        <SyncRaftStateMachine<RecordingApply> as RaftStateMachine<SyncTypeConfig>>::apply(
            &mut state_machine,
            [entry],
        )
        .await
        .expect("apply entry");
    let (last_applied, _) = state_machine.applied_state().await.expect("applied state");

    assert_eq!(last_applied, Some(log_id));
    assert_eq!(
        responses[0].responses[0].response_json.as_deref(),
        Some("3:11")
    );
}

#[tokio::test]
async fn raft_state_machine_installs_logical_snapshot_through_shared_import_barrier() {
    let apply = Arc::new(RecordingApply::default());
    let mut state_machine = SyncRaftStateMachine::new(apply.clone());
    let log_id = LogId {
        leader_id: LeaderId::new(5, 9),
        index: 12,
    };
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("snapshot-1").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );
    let chunk = LogicalBackfillChunk {
        summary: LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new("chunk-1").unwrap(),
            domain: LogicalBackfillDomain::ItemRecords,
            record_count: 1,
            checksum: LogicalBackfillChecksum::new("checksum-1").unwrap(),
        },
        records: vec![LogicalBackfillRecord::PresentItem {
            table_name: "users".to_string(),
            key_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
            item_json: r#"{"pk":{"S":"u#1"}}"#.to_string(),
            item_stream_version: ItemStreamVersion::new(3),
        }],
    };
    let snapshot = SyncRaftSnapshotPayload::new(manifest.clone(), vec![chunk.clone()])
        .into_snapshot(SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-1".to_string(),
        })
        .expect("snapshot");

    <SyncRaftStateMachine<RecordingApply> as RaftStateMachine<SyncTypeConfig>>::install_snapshot(
        &mut state_machine,
        &snapshot.meta,
        snapshot.snapshot,
    )
    .await
    .expect("install snapshot");

    assert_eq!(
        state_machine.snapshot_install_phase(),
        SyncSnapshotInstallPhase::Installed
    );
    assert_eq!(
        state_machine.current_snapshot_manifest().unwrap().caller,
        LogicalBackfillCaller::SyncLearnerCatchup
    );
    assert_eq!(
        state_machine
            .current_snapshot_manifest()
            .unwrap()
            .activation_gate,
        LogicalBackfillActivationGate::RaftPromotionReadiness
    );
    assert_eq!(apply.imported_chunks.lock().unwrap().as_slice(), &[chunk]);
    assert!(
        state_machine
            .get_current_snapshot()
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn raft_state_machine_blocks_post_boundary_apply_until_snapshot_install_completes() {
    let apply = Arc::new(RecordingApply {
        fail_import: true,
        ..RecordingApply::default()
    });
    let mut state_machine = SyncRaftStateMachine::new(apply.clone());
    let snapshot_log_id = LogId {
        leader_id: LeaderId::new(5, 9),
        index: 12,
    };
    let snapshot = logical_snapshot(snapshot_log_id);

    <SyncRaftStateMachine<RecordingApply> as RaftStateMachine<SyncTypeConfig>>::install_snapshot(
        &mut state_machine,
        &snapshot.meta,
        snapshot.snapshot,
    )
    .await
    .expect_err("interrupted install should fail");

    assert_eq!(
        state_machine.snapshot_install_phase(),
        SyncSnapshotInstallPhase::RaftRecovering
    );

    let post_boundary = Entry {
        log_id: LogId {
            leader_id: LeaderId::new(5, 9),
            index: 13,
        },
        payload: EntryPayload::Normal(SyncRaftRequest::new(ResolvedSyncMutationBatch::new(
            Vec::new(),
        ))),
    };
    let error = <SyncRaftStateMachine<RecordingApply> as RaftStateMachine<SyncTypeConfig>>::apply(
        &mut state_machine,
        [post_boundary],
    )
    .await
    .expect_err("post-boundary apply should be blocked");

    assert!(error.to_string().contains("post-boundary apply is blocked"));
    assert!(apply.applied.lock().unwrap().is_empty());

    state_machine.discard_incomplete_snapshot_install();
    assert_eq!(
        state_machine.snapshot_install_phase(),
        SyncSnapshotInstallPhase::Idle
    );
}

fn logical_snapshot(log_id: LogId<u64>) -> openraft::Snapshot<SyncTypeConfig> {
    let manifest = LogicalBackfillManifest::for_policy(
        LogicalBackfillId::new("snapshot-block").unwrap(),
        &SyncLearnerCatchupPolicy,
        "sqlite",
        "sqlite",
        vec![LogicalBackfillDomain::ItemRecords],
    );
    let chunk = LogicalBackfillChunk {
        summary: LogicalBackfillChunkSummary {
            id: LogicalBackfillChunkId::new("chunk-block").unwrap(),
            domain: LogicalBackfillDomain::ItemRecords,
            record_count: 1,
            checksum: LogicalBackfillChecksum::new("checksum-block").unwrap(),
        },
        records: vec![LogicalBackfillRecord::PresentItem {
            table_name: "users".to_string(),
            key_json: r#"{"pk":{"S":"u#blocked"}}"#.to_string(),
            item_json: r#"{"pk":{"S":"u#blocked"}}"#.to_string(),
            item_stream_version: ItemStreamVersion::new(4),
        }],
    };
    SyncRaftSnapshotPayload::new(manifest, vec![chunk])
        .into_snapshot(SnapshotMeta {
            last_log_id: Some(log_id),
            last_membership: StoredMembership::default(),
            snapshot_id: "snapshot-block".to_string(),
        })
        .expect("snapshot")
}

#[test]
fn raft_snapshot_payload_roundtrips_manifest_for_restart_inspection() {
    let log_id = LogId {
        leader_id: LeaderId::new(5, 9),
        index: 12,
    };
    let snapshot = logical_snapshot(log_id);
    let bytes = snapshot.snapshot.get_ref().clone();
    let payload =
        SyncRaftSnapshotPayload::from_snapshot_bytes(&bytes).expect("snapshot payload bytes");

    assert_eq!(payload.manifest.id.as_str(), "snapshot-block");
    assert_eq!(
        payload.manifest.caller,
        LogicalBackfillCaller::SyncLearnerCatchup
    );
    assert_eq!(payload.chunks.len(), 1);
    assert_eq!(snapshot.meta.last_log_id, Some(log_id));
}
