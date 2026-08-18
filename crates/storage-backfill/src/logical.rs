use serde::{Deserialize, Serialize};
use storage_types::{ItemStreamVersion, StorageError};
use thiserror::Error;

pub const LOGICAL_BACKFILL_PROTOCOL_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum LogicalBackfillError {
    #[error(
        "logical backfill protocol version {actual} is incompatible with required version \
         {expected}"
    )]
    IncompatibleProtocolVersion { expected: u16, actual: u16 },
    #[error("logical backfill identifier cannot be empty")]
    EmptyId,
    #[error("logical backfill checksum cannot be empty")]
    EmptyChecksum,
    #[error("logical backfill chunk domain {domain:?} is not in manifest")]
    DomainNotInManifest { domain: LogicalBackfillDomain },
    #[error("logical backfill record count mismatch: expected {expected}, got {actual}")]
    RecordCountMismatch { expected: u64, actual: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LogicalBackfillId(String);

impl LogicalBackfillId {
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalBackfillError> {
        let value = value.into();
        if value.is_empty() {
            return Err(LogicalBackfillError::EmptyId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<LogicalBackfillId> for String {
    fn from(value: LogicalBackfillId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LogicalBackfillChunkId(String);

impl LogicalBackfillChunkId {
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalBackfillError> {
        let value = value.into();
        if value.is_empty() {
            return Err(LogicalBackfillError::EmptyId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<LogicalBackfillChunkId> for String {
    fn from(value: LogicalBackfillChunkId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalBackfillCheckpoint {
    pub manifest_id: LogicalBackfillId,
    pub last_imported_chunk: Option<LogicalBackfillChunkId>,
    pub protected_stream_cursor: Option<String>,
    pub source_log_boundary: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillCaller {
    SyncLearnerCatchup,
    MultiRegionBootstrap,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillActivationGate {
    RaftPromotionReadiness,
    ReplicaActivationCursor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillConflictPolicy {
    ItemStreamVersionOnly,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillTombstoneCleanup {
    AfterFinalCatchupDrain,
}

pub trait LogicalBackfillPolicy {
    fn caller(&self) -> LogicalBackfillCaller;
    fn activation_gate(&self) -> LogicalBackfillActivationGate;

    fn conflict_policy(&self) -> LogicalBackfillConflictPolicy {
        LogicalBackfillConflictPolicy::ItemStreamVersionOnly
    }

    fn tombstone_cleanup(&self) -> LogicalBackfillTombstoneCleanup {
        LogicalBackfillTombstoneCleanup::AfterFinalCatchupDrain
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncLearnerCatchupPolicy;

impl LogicalBackfillPolicy for SyncLearnerCatchupPolicy {
    fn caller(&self) -> LogicalBackfillCaller {
        LogicalBackfillCaller::SyncLearnerCatchup
    }

    fn activation_gate(&self) -> LogicalBackfillActivationGate {
        LogicalBackfillActivationGate::RaftPromotionReadiness
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MultiRegionBootstrapPolicy;

impl LogicalBackfillPolicy for MultiRegionBootstrapPolicy {
    fn caller(&self) -> LogicalBackfillCaller {
        LogicalBackfillCaller::MultiRegionBootstrap
    }

    fn activation_gate(&self) -> LogicalBackfillActivationGate {
        LogicalBackfillActivationGate::ReplicaActivationCursor
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillDomain {
    TableMetadata,
    ItemRecords,
    Tombstones,
    DurableRevisions,
    StreamRecords,
    TtlRecords,
    GsiRecords,
    StorageControlPlane,
    BackgroundJobs,
    SyncControlPlane,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalBackfillManifest {
    pub protocol_version: u16,
    pub id: LogicalBackfillId,
    pub caller: LogicalBackfillCaller,
    pub activation_gate: LogicalBackfillActivationGate,
    pub conflict_policy: LogicalBackfillConflictPolicy,
    pub tombstone_cleanup: LogicalBackfillTombstoneCleanup,
    pub source_backend: String,
    pub destination_backend: String,
    pub domains: Vec<LogicalBackfillDomain>,
    pub protected_stream_cursor: Option<String>,
    pub source_log_boundary: Option<String>,
    pub chunks: Vec<LogicalBackfillChunkSummary>,
}

impl LogicalBackfillManifest {
    #[must_use]
    pub fn for_policy<P: LogicalBackfillPolicy>(
        id: LogicalBackfillId,
        policy: &P,
        source_backend: impl Into<String>,
        destination_backend: impl Into<String>,
        domains: Vec<LogicalBackfillDomain>,
    ) -> Self {
        Self {
            protocol_version: LOGICAL_BACKFILL_PROTOCOL_VERSION,
            id,
            caller: policy.caller(),
            activation_gate: policy.activation_gate(),
            conflict_policy: policy.conflict_policy(),
            tombstone_cleanup: policy.tombstone_cleanup(),
            source_backend: source_backend.into(),
            destination_backend: destination_backend.into(),
            domains,
            protected_stream_cursor: None,
            source_log_boundary: None,
            chunks: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalBackfillChunkSummary {
    pub id: LogicalBackfillChunkId,
    pub domain: LogicalBackfillDomain,
    pub record_count: u64,
    pub checksum: LogicalBackfillChecksum,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalBackfillChunk {
    pub summary: LogicalBackfillChunkSummary,
    pub records: Vec<LogicalBackfillRecord>,
}

pub fn validate_logical_chunk_for_manifest(
    manifest: &LogicalBackfillManifest,
    chunk: &LogicalBackfillChunk,
) -> Result<(), LogicalBackfillError> {
    if manifest.protocol_version != LOGICAL_BACKFILL_PROTOCOL_VERSION {
        return Err(LogicalBackfillError::IncompatibleProtocolVersion {
            expected: LOGICAL_BACKFILL_PROTOCOL_VERSION,
            actual: manifest.protocol_version,
        });
    }
    if !manifest.domains.contains(&chunk.summary.domain) {
        return Err(LogicalBackfillError::DomainNotInManifest {
            domain: chunk.summary.domain,
        });
    }
    let actual_count = u64::try_from(chunk.records.len()).map_err(|_| {
        LogicalBackfillError::RecordCountMismatch {
            expected: chunk.summary.record_count,
            actual: u64::MAX,
        }
    })?;
    if chunk.summary.record_count != actual_count {
        return Err(LogicalBackfillError::RecordCountMismatch {
            expected: chunk.summary.record_count,
            actual: actual_count,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalBackfillRecord {
    PresentItem {
        table_name: String,
        key_json: String,
        item_json: String,
        indexers: Vec<String>,
        item_stream_version: ItemStreamVersion,
    },
    Tombstone(LogicalBackfillTombstone),
    StreamRecord {
        stream_name: String,
        record_id: String,
        payload_json: String,
        item_stream_version: Option<ItemStreamVersion>,
    },
    DomainRecord {
        domain: LogicalBackfillDomain,
        record_key_json: String,
        payload_json: String,
    },
}

impl LogicalBackfillRecord {
    #[must_use]
    pub const fn item_stream_version(&self) -> Option<ItemStreamVersion> {
        match self {
            Self::PresentItem {
                item_stream_version,
                ..
            }
            | Self::Tombstone(LogicalBackfillTombstone {
                item_stream_version,
                ..
            }) => Some(*item_stream_version),
            Self::StreamRecord {
                item_stream_version,
                ..
            } => *item_stream_version,
            Self::DomainRecord { .. } => None,
        }
    }

    #[must_use]
    pub const fn domain(&self) -> LogicalBackfillDomain {
        match self {
            Self::PresentItem { .. } => LogicalBackfillDomain::ItemRecords,
            Self::Tombstone(_) => LogicalBackfillDomain::Tombstones,
            Self::StreamRecord { .. } => LogicalBackfillDomain::StreamRecords,
            Self::DomainRecord { domain, .. } => *domain,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalBackfillTombstone {
    pub table_name: String,
    pub key_json: String,
    pub item_stream_version: ItemStreamVersion,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct LogicalBackfillChecksum(String);

impl LogicalBackfillChecksum {
    pub fn new(value: impl Into<String>) -> Result<Self, LogicalBackfillError> {
        let value = value.into();
        if value.is_empty() {
            return Err(LogicalBackfillError::EmptyChecksum);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<LogicalBackfillChecksum> for String {
    fn from(value: LogicalBackfillChecksum) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LogicalBackfillCommand {
    StartSession {
        manifest: LogicalBackfillManifest,
    },
    ImportChunk {
        chunk: LogicalBackfillChunk,
    },
    PersistCheckpoint {
        checkpoint: LogicalBackfillCheckpoint,
    },
    DrainStreams {
        target_cursor: String,
    },
    ActivateDestination {
        manifest_id: LogicalBackfillId,
    },
    CleanupTombstones {
        manifest_id: LogicalBackfillId,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LogicalBackfillResult {
    Started,
    ChunkImported,
    DuplicateChunkIgnored,
    StaleManifestRejected,
    CheckpointPersisted,
    StreamsDrained,
    DestinationActivated,
    TombstonesCleaned,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalExportRequest {
    pub manifest_id: LogicalBackfillId,
    pub domain: LogicalBackfillDomain,
    pub table_name: Option<String>,
    pub cursor: Option<String>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LogicalExportPage {
    pub domain: LogicalBackfillDomain,
    pub records: Vec<LogicalBackfillRecord>,
    pub next_cursor: Option<String>,
    pub checksum: LogicalBackfillChecksum,
}

#[async_trait::async_trait]
pub trait LogicalBackfillExport {
    async fn export_logical_page(
        &self,
        request: LogicalExportRequest,
    ) -> Result<LogicalExportPage, StorageError>;
}

#[async_trait::async_trait]
pub trait LogicalBackfillImport {
    async fn import_logical_chunk(
        &self,
        manifest: &LogicalBackfillManifest,
        chunk: LogicalBackfillChunk,
    ) -> Result<LogicalBackfillResult, StorageError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalImportRecordKind {
    PresentItem,
    Tombstone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalImportApplyCase {
    pub current_version: Option<ItemStreamVersion>,
    pub incoming_version: ItemStreamVersion,
    pub incoming_kind: LogicalImportRecordKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalImportApplyDecision {
    ApplyPresentItem,
    ApplyTombstone,
    IgnoreDuplicate,
    IgnoreStale,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogicalBootstrapPreflightCase {
    pub destination_empty: bool,
    pub preflight_marker_present: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalBootstrapPreflightDecision {
    AllowEmptyDestination,
    AllowRetryAfterPreflight,
    RejectNonEmptyDestination,
}

impl LogicalImportApplyCase {
    #[must_use]
    pub const fn new(
        current_version: Option<ItemStreamVersion>,
        incoming_version: ItemStreamVersion,
        incoming_kind: LogicalImportRecordKind,
    ) -> Self {
        Self {
            current_version,
            incoming_version,
            incoming_kind,
        }
    }
}

#[must_use]
pub fn plan_logical_import_apply(case: LogicalImportApplyCase) -> LogicalImportApplyDecision {
    match case.current_version {
        Some(current) if case.incoming_version < current => LogicalImportApplyDecision::IgnoreStale,
        Some(current) if case.incoming_version == current => {
            LogicalImportApplyDecision::IgnoreDuplicate
        }
        _ => match case.incoming_kind {
            LogicalImportRecordKind::PresentItem => LogicalImportApplyDecision::ApplyPresentItem,
            LogicalImportRecordKind::Tombstone => LogicalImportApplyDecision::ApplyTombstone,
        },
    }
}

#[must_use]
pub const fn plan_logical_bootstrap_preflight(
    case: LogicalBootstrapPreflightCase,
) -> LogicalBootstrapPreflightDecision {
    if case.preflight_marker_present {
        LogicalBootstrapPreflightDecision::AllowRetryAfterPreflight
    } else if case.destination_empty {
        LogicalBootstrapPreflightDecision::AllowEmptyDestination
    } else {
        LogicalBootstrapPreflightDecision::RejectNonEmptyDestination
    }
}
