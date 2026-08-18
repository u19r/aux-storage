use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};
use storage_types::{ItemStreamVersion, TableName};

use crate::metadata::SyncReadSet;

pub const SYNC_PROTOCOL_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncMutationId(String);

impl SyncMutationId {
    pub fn new(value: impl Into<String>) -> Result<Self, SyncMutationError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SyncMutationError::EmptyMutationId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncProposalId(String);

impl SyncProposalId {
    pub fn new(value: impl Into<String>) -> Result<Self, SyncMutationError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(SyncMutationError::EmptyProposalId);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResolvedSyncMutation {
    Put(SyncPutMutation),
    Delete(SyncDeleteMutation),
    CreateTable(SyncCreateTableMutation),
    UpdateTable(SyncUpdateTableMutation),
    DeleteTable(SyncDeleteTableMutation),
    UpdateTimeToLive(SyncUpdateTimeToLiveMutation),
}

impl ResolvedSyncMutation {
    #[must_use]
    pub fn mutation_id(&self) -> &SyncMutationId {
        match self {
            Self::Put(mutation) => &mutation.mutation_id,
            Self::Delete(mutation) => &mutation.mutation_id,
            Self::CreateTable(mutation) => &mutation.mutation_id,
            Self::UpdateTable(mutation) => &mutation.mutation_id,
            Self::DeleteTable(mutation) => &mutation.mutation_id,
            Self::UpdateTimeToLive(mutation) => &mutation.mutation_id,
        }
    }

    #[must_use]
    pub fn table_name(&self) -> &TableName {
        match self {
            Self::Put(mutation) => &mutation.table_name,
            Self::Delete(mutation) => &mutation.table_name,
            Self::CreateTable(mutation) => &mutation.table_name,
            Self::UpdateTable(mutation) => &mutation.table_name,
            Self::DeleteTable(mutation) => &mutation.table_name,
            Self::UpdateTimeToLive(mutation) => &mutation.table_name,
        }
    }

    #[must_use]
    pub fn target_item_stream_version(&self) -> ItemStreamVersion {
        match self {
            Self::Put(mutation) => mutation.target_item_stream_version,
            Self::Delete(mutation) => mutation.target_item_stream_version,
            Self::CreateTable(_)
            | Self::UpdateTable(_)
            | Self::DeleteTable(_)
            | Self::UpdateTimeToLive(_) => ItemStreamVersion::new(0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPutMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub key_json: String,
    pub item_json: String,
    pub indexers: Vec<String>,
    pub old_item_json: Option<String>,
    pub old_indexers: Option<Vec<String>>,
    pub target_item_stream_version: ItemStreamVersion,
    pub response: SyncMutationResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDeleteMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub key_json: String,
    pub old_item_json: Option<String>,
    pub old_indexers: Option<Vec<String>>,
    pub target_item_stream_version: ItemStreamVersion,
    pub response: SyncMutationResponse,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncCreateTableMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub request_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncUpdateTableMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub request_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncDeleteTableMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub request_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncUpdateTimeToLiveMutation {
    pub mutation_id: SyncMutationId,
    pub table_name: TableName,
    pub request_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedSyncMutationBatch {
    pub protocol_version: u16,
    pub mutations: Vec<ResolvedSyncMutation>,
}

impl ResolvedSyncMutationBatch {
    #[must_use]
    pub fn new(mutations: Vec<ResolvedSyncMutation>) -> Self {
        Self {
            protocol_version: SYNC_PROTOCOL_VERSION,
            mutations,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    pub fn validate_protocol(&self) -> Result<(), SyncMutationError> {
        if self.protocol_version == SYNC_PROTOCOL_VERSION {
            Ok(())
        } else {
            Err(SyncMutationError::IncompatibleProtocolVersion {
                expected: SYNC_PROTOCOL_VERSION,
                actual: self.protocol_version,
            })
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncProposalBatch {
    pub proposal_id: SyncProposalId,
    pub batch: ResolvedSyncMutationBatch,
    pub read_set: SyncReadSet,
}

impl SyncProposalBatch {
    #[must_use]
    pub fn new(proposal_id: SyncProposalId, batch: ResolvedSyncMutationBatch) -> Self {
        Self {
            proposal_id,
            batch,
            read_set: SyncReadSet::default(),
        }
    }

    #[must_use]
    pub fn with_read_set(mut self, read_set: SyncReadSet) -> Self {
        self.read_set = read_set;
        self
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.batch.is_empty()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncProposalResponse {
    pub proposal_id: SyncProposalId,
    pub responses: Vec<SyncMutationResponse>,
}

impl SyncProposalResponse {
    #[must_use]
    pub fn new(proposal_id: SyncProposalId, responses: Vec<SyncMutationResponse>) -> Self {
        Self {
            proposal_id,
            responses,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncMutationResponse {
    pub response_json: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMutationError {
    EmptyMutationId,
    EmptyProposalId,
    IncompatibleProtocolVersion { expected: u16, actual: u16 },
}

impl Display for SyncMutationError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyMutationId => formatter.write_str("sync mutation id must not be empty"),
            Self::EmptyProposalId => formatter.write_str("sync proposal id must not be empty"),
            Self::IncompatibleProtocolVersion { expected, actual } => write!(
                formatter,
                "sync protocol version {actual} is incompatible with required version {expected}"
            ),
        }
    }
}

impl std::error::Error for SyncMutationError {}
