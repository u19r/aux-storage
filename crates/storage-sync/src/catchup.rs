use serde::{Deserialize, Serialize};
use storage_backfill::{
    LogicalBackfillActivationGate, LogicalBackfillCaller, LogicalBackfillChunk,
    LogicalBackfillChunkId, LogicalBackfillChunkSummary, LogicalBackfillDomain,
    LogicalBackfillExport, LogicalBackfillId, LogicalBackfillImport, LogicalBackfillManifest,
    LogicalBackfillPolicy, LogicalExportRequest, SyncLearnerCatchupPolicy,
    validate_logical_chunk_for_manifest,
};
use storage_types::{StorageError, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncLearnerCatchupRequirement<P = SyncLearnerCatchupPolicy> {
    policy: P,
}

impl Default for SyncLearnerCatchupRequirement<SyncLearnerCatchupPolicy> {
    fn default() -> Self {
        Self {
            policy: SyncLearnerCatchupPolicy,
        }
    }
}

impl<P> SyncLearnerCatchupRequirement<P>
where P: LogicalBackfillPolicy
{
    #[must_use]
    pub const fn new(policy: P) -> Self {
        Self { policy }
    }

    #[must_use]
    pub fn caller(&self) -> LogicalBackfillCaller {
        self.policy.caller()
    }

    #[must_use]
    pub fn activation_gate(&self) -> LogicalBackfillActivationGate {
        self.policy.activation_gate()
    }

    #[must_use]
    pub const fn policy(&self) -> &P {
        &self.policy
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncLearnerCatchupGate {
    LogicalBackfillComplete,
    PromotionDecisionReached,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLearnerCatchupConfig {
    pub page_limit: u32,
    pub domains: Vec<LogicalBackfillDomain>,
}

impl Default for SyncLearnerCatchupConfig {
    fn default() -> Self {
        Self {
            page_limit: 500,
            domains: sync_learner_catchup_domains(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncLearnerCatchupReport {
    pub manifest: LogicalBackfillManifest,
    pub chunks_imported: u64,
    pub records_imported: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncLearnerCatchupCheckpoint {
    pub manifest: LogicalBackfillManifest,
    pub domain_index: usize,
    pub cursor: Option<String>,
    pub page: u64,
    pub chunks_imported: u64,
    pub records_imported: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SyncLearnerCatchupStep {
    pub checkpoint: SyncLearnerCatchupCheckpoint,
    pub imported_chunk: Option<LogicalBackfillChunkSummary>,
    pub complete: bool,
}

pub struct SyncLearnerCatchupExecutor<P = SyncLearnerCatchupPolicy> {
    requirement: SyncLearnerCatchupRequirement<P>,
    config: SyncLearnerCatchupConfig,
}

impl Default for SyncLearnerCatchupExecutor<SyncLearnerCatchupPolicy> {
    fn default() -> Self {
        Self::new(
            SyncLearnerCatchupRequirement::default(),
            SyncLearnerCatchupConfig::default(),
        )
    }
}

impl<P> SyncLearnerCatchupExecutor<P>
where P: LogicalBackfillPolicy
{
    #[must_use]
    pub const fn new(
        requirement: SyncLearnerCatchupRequirement<P>,
        config: SyncLearnerCatchupConfig,
    ) -> Self {
        Self {
            requirement,
            config,
        }
    }

    pub async fn run<S, D>(
        &self,
        source: &S,
        destination: &D,
        manifest_id: LogicalBackfillId,
        source_backend: impl Into<String>,
        destination_backend: impl Into<String>,
    ) -> StorageResult<SyncLearnerCatchupReport>
    where
        S: LogicalBackfillExport + Sync,
        D: LogicalBackfillImport + Sync,
    {
        let mut checkpoint =
            self.start_checkpoint(manifest_id, source_backend, destination_backend);
        loop {
            let step = self
                .transfer_next_chunk(source, destination, checkpoint)
                .await?;
            checkpoint = step.checkpoint;
            if step.complete {
                break;
            }
        }

        Ok(SyncLearnerCatchupReport {
            manifest: checkpoint.manifest,
            chunks_imported: checkpoint.chunks_imported,
            records_imported: checkpoint.records_imported,
        })
    }

    #[must_use]
    pub fn start_checkpoint(
        &self,
        manifest_id: LogicalBackfillId,
        source_backend: impl Into<String>,
        destination_backend: impl Into<String>,
    ) -> SyncLearnerCatchupCheckpoint {
        SyncLearnerCatchupCheckpoint {
            manifest: LogicalBackfillManifest::for_policy(
                manifest_id,
                self.requirement.policy(),
                source_backend,
                destination_backend,
                self.config.domains.clone(),
            ),
            domain_index: 0,
            cursor: None,
            page: 0,
            chunks_imported: 0,
            records_imported: 0,
        }
    }

    pub async fn transfer_next_chunk<S, D>(
        &self,
        source: &S,
        destination: &D,
        mut checkpoint: SyncLearnerCatchupCheckpoint,
    ) -> StorageResult<SyncLearnerCatchupStep>
    where
        S: LogicalBackfillExport + Sync,
        D: LogicalBackfillImport + Sync,
    {
        let Some(domain) = self.config.domains.get(checkpoint.domain_index).copied() else {
            return Ok(SyncLearnerCatchupStep {
                checkpoint,
                imported_chunk: None,
                complete: true,
            });
        };
        let export = source
            .export_logical_page(LogicalExportRequest {
                manifest_id: checkpoint.manifest.id.clone(),
                domain,
                table_name: None,
                cursor: checkpoint.cursor.clone(),
                limit: self.config.page_limit,
            })
            .await?;
        let chunk = LogicalBackfillChunk {
            summary: LogicalBackfillChunkSummary {
                id: chunk_id(&checkpoint.manifest.id, domain, checkpoint.page)?,
                domain: export.domain,
                record_count: export.records.len().try_into().map_err(|_| {
                    StorageError::internal("logical catchup page record count overflow")
                })?,
                checksum: export.checksum,
            },
            records: export.records,
        };
        validate_logical_chunk_for_manifest(&checkpoint.manifest, &chunk)
            .map_err(|error| StorageError::validation(error.to_string()))?;
        let imported_chunk = chunk.summary.clone();
        let record_count = chunk.summary.record_count;
        checkpoint.manifest.chunks.push(imported_chunk.clone());
        destination
            .import_logical_chunk(&checkpoint.manifest, chunk)
            .await?;
        checkpoint.chunks_imported = checkpoint.chunks_imported.saturating_add(1);
        checkpoint.records_imported = checkpoint.records_imported.saturating_add(record_count);
        if let Some(next_cursor) = export.next_cursor {
            checkpoint.cursor = Some(next_cursor);
            checkpoint.page = checkpoint.page.saturating_add(1);
        } else {
            checkpoint.domain_index = checkpoint.domain_index.saturating_add(1);
            checkpoint.cursor = None;
            checkpoint.page = 0;
        }
        let complete = checkpoint.domain_index >= self.config.domains.len();
        Ok(SyncLearnerCatchupStep {
            checkpoint,
            imported_chunk: Some(imported_chunk),
            complete,
        })
    }
}

#[must_use]
pub fn sync_learner_catchup_domains() -> Vec<LogicalBackfillDomain> {
    vec![
        LogicalBackfillDomain::TableMetadata,
        LogicalBackfillDomain::ItemRecords,
        LogicalBackfillDomain::Tombstones,
        LogicalBackfillDomain::DurableRevisions,
        LogicalBackfillDomain::StreamRecords,
        LogicalBackfillDomain::TtlRecords,
        LogicalBackfillDomain::GsiRecords,
        LogicalBackfillDomain::StorageControlPlane,
        LogicalBackfillDomain::BackgroundJobs,
        LogicalBackfillDomain::SyncControlPlane,
    ]
}

fn chunk_id(
    manifest_id: &LogicalBackfillId,
    domain: LogicalBackfillDomain,
    page: u64,
) -> StorageResult<LogicalBackfillChunkId> {
    LogicalBackfillChunkId::new(format!("{}#{domain:?}#{page}", manifest_id.as_str()))
        .map_err(|error| StorageError::validation(error.to_string()))
}
