use std::{io::Cursor, sync::Arc};

use openraft::{
    EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError as RaftStorageError, StorageIOError,
    StoredMembership,
    storage::{RaftSnapshotBuilder, RaftStateMachine},
};
use storage_backfill::{LogicalBackfillImport, LogicalBackfillManifest};
use storage_types::TimestampMillis;

use crate::{
    SyncApply, SyncCommitMetadata, SyncLogId, SyncRaftResponse, SyncRaftSnapshotPayload,
    SyncSnapshotInstallPhase, SyncTypeConfig,
};

pub struct SyncRaftStateMachine<A> {
    apply: Arc<A>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, openraft::BasicNode>,
    current_snapshot: Option<Snapshot<SyncTypeConfig>>,
    current_snapshot_manifest: Option<LogicalBackfillManifest>,
    snapshot_install_phase: SyncSnapshotInstallPhase,
}

// Snapshot install is treated as a barrier: logical state must be durable at
// the snapshot boundary before any post-boundary log entry can apply. This
// keeps OpenRaft log compaction compatible with portable logical catchup.
impl<A> SyncRaftStateMachine<A> {
    #[must_use]
    pub fn new(apply: Arc<A>) -> Self {
        Self {
            apply,
            last_applied: None,
            last_membership: StoredMembership::default(),
            current_snapshot: None,
            current_snapshot_manifest: None,
            snapshot_install_phase: SyncSnapshotInstallPhase::Idle,
        }
    }

    #[must_use]
    pub const fn snapshot_install_phase(&self) -> SyncSnapshotInstallPhase {
        self.snapshot_install_phase
    }

    #[must_use]
    pub const fn current_snapshot_manifest(&self) -> Option<&LogicalBackfillManifest> {
        self.current_snapshot_manifest.as_ref()
    }

    pub fn discard_incomplete_snapshot_install(&mut self) {
        if self.snapshot_install_phase == SyncSnapshotInstallPhase::RaftRecovering {
            self.snapshot_install_phase = SyncSnapshotInstallPhase::Idle;
        }
    }
}

impl<A> RaftStateMachine<SyncTypeConfig> for SyncRaftStateMachine<A>
where A: SyncApply + LogicalBackfillImport + 'static
{
    type SnapshotBuilder = SyncRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, openraft::BasicNode>,
        ),
        RaftStorageError<u64>,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<SyncRaftResponse>, RaftStorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<SyncTypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut responses = Vec::new();
        for entry in entries {
            if self.snapshot_install_phase == SyncSnapshotInstallPhase::RaftRecovering {
                let io_error = std::io::Error::other(
                    "sync snapshot install is still recovering; post-boundary apply is blocked",
                );
                return Err(RaftStorageError::IO {
                    source: StorageIOError::apply(entry.log_id, &io_error),
                });
            }
            self.last_applied = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => responses.push(SyncRaftResponse::new(Vec::new())),
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(SyncRaftResponse::new(Vec::new()));
                }
                EntryPayload::Normal(request) => {
                    let metadata = SyncCommitMetadata {
                        log_id: SyncLogId::new(entry.log_id.leader_id.term, entry.log_id.index),
                        committed_at: TimestampMillis::now(),
                        leader_node_id: entry.log_id.leader_id.node_id.to_string(),
                    };
                    let response = self
                        .apply
                        .apply_resolved_sync_mutations(metadata, request.batch)
                        .await
                        .map(SyncRaftResponse::new)
                        .map_err(|error| {
                            let io_error = std::io::Error::other(error.to_string());
                            RaftStorageError::IO {
                                source: StorageIOError::apply(entry.log_id, &io_error),
                            }
                        })?;
                    responses.push(response);
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        SyncRaftSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            current_snapshot: self.current_snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, RaftStorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), RaftStorageError<u64>> {
        self.snapshot_install_phase = SyncSnapshotInstallPhase::RaftRecovering;
        let bytes = snapshot.get_ref().clone();
        let payload = SyncRaftSnapshotPayload::from_snapshot_bytes(&bytes).map_err(|error| {
            let io_error = std::io::Error::other(error.to_string());
            RaftStorageError::IO {
                source: StorageIOError::read_snapshot(Some(meta.signature()), &io_error),
            }
        })?;
        for chunk in payload.chunks {
            let manifest = payload.manifest.clone();
            self.apply
                .import_logical_chunk(&manifest, chunk)
                .await
                .map_err(|error| {
                    let io_error = std::io::Error::other(error.to_string());
                    RaftStorageError::IO {
                        source: StorageIOError::apply(
                            meta.last_log_id.unwrap_or_else(|| LogId {
                                leader_id: openraft::LeaderId::new(0, 0),
                                index: 0,
                            }),
                            &io_error,
                        ),
                    }
                })?;
        }
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        self.current_snapshot_manifest = Some(payload.manifest);
        self.current_snapshot = Some(Snapshot {
            meta: meta.clone(),
            snapshot: Box::new(Cursor::new(bytes)),
        });
        self.snapshot_install_phase = SyncSnapshotInstallPhase::Installed;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<SyncTypeConfig>>, RaftStorageError<u64>> {
        Ok(self.current_snapshot.clone())
    }
}

pub struct SyncRaftSnapshotBuilder {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, openraft::BasicNode>,
    current_snapshot: Option<Snapshot<SyncTypeConfig>>,
}

impl RaftSnapshotBuilder<SyncTypeConfig> for SyncRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<SyncTypeConfig>, RaftStorageError<u64>> {
        if let Some(snapshot) = self.current_snapshot.clone() {
            return Ok(snapshot);
        }
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: self.last_applied,
                last_membership: self.last_membership.clone(),
                snapshot_id: "sync-empty-snapshot".to_string(),
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}
