use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::{StorageResult, StoredTableInfo, TimeToLiveStatus};

use crate::native_record::corrupt_record;

/// Logical policies frozen at the capture barrier. Native maintenance cursors
/// and source row counts do not describe a new target database.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CaptureSchema {
    pub table: StoredTableInfo,
    pub ttl: Option<CaptureTtlPolicy>,
}

impl CaptureSchema {
    pub(crate) fn require_table(&self, table: &StoredTableInfo) -> StorageResult<()> {
        if table.table_name != self.table.table_name
            || table.table_status != storage_types::TableStatus::Active
            || logical_table_content(table)? != logical_table_content(&self.table)?
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn validate(&self, name: &storage_types::TableName) -> StorageResult<()> {
        let table = &self.table;
        if &table.table_name != name
            || !storage_provider::is_public_table_name(name)
            || table.table_status != storage_types::TableStatus::Active
            || table.item_count != 0
            || table.table_size_bytes != 0
            || table.created_at.timestamp_millis() <= 0
            || self
                .ttl
                .as_ref()
                .is_some_and(|policy| policy.attribute.trim().is_empty())
        {
            return Err(corrupt_record());
        }
        self.validate_ttl_index()?;
        let mut request = storage_types::CreateTableRequest::new(
            table.table_name.clone(),
            table.attribute_definitions.clone(),
            table.key_schema.clone(),
            storage_types::BillingMode::PayPerRequest,
        );
        request.global_secondary_indexes = table
            .global_secondary_indexes
            .as_ref()
            .map(|indexes| {
                indexes
                    .iter()
                    .filter(|index| !index.index_name.is_ttl_index())
                    .map(|index| storage_types::CreateGlobalSecondaryIndex {
                        index_name: index.index_name.clone(),
                        key_schema: index.key_schema.clone(),
                        projection: index.projection.clone(),
                        provisioned_throughput: None,
                    })
                    .collect::<Vec<_>>()
            })
            .filter(|indexes| !indexes.is_empty());
        request.max_indexers = table.max_indexers;
        request.validate_for_storage()
    }

    fn validate_ttl_index(&self) -> StorageResult<()> {
        let indexes = self
            .table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|index| index.index_name.is_ttl_index())
            .collect::<Vec<_>>();
        let policy = self.ttl.as_ref().filter(|policy| {
            matches!(
                policy.status,
                TimeToLiveStatus::Enabling | TimeToLiveStatus::Enabled
            )
        });
        let Some(policy) = policy else {
            return if indexes.is_empty() {
                Ok(())
            } else {
                Err(corrupt_record())
            };
        };
        let [index] = indexes.as_slice() else {
            return Err(corrupt_record());
        };
        let expected = storage_common::TtlConfigRecord::try_new_at(
            policy.attribute.clone(),
            &self.table.table_name,
            policy.status.clone(),
            self.table.created_at,
        )?;
        if index.index_name != expected.gsi_name
            || index.key_schema.len() != 1
            || index.key_schema[0].key_type != storage_types::KeyType::Range
            || index.key_schema[0].attribute_name != policy.attribute
            || index.projection.projection_type.is_some()
            || index.projection.non_key_attributes.is_some()
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct SourceSchemaObservation {
    pub(crate) source: replication_types::configuration::installed::InstalledTableState,
    pub(crate) schema: CaptureSchema,
}

impl SourceSchemaObservation {
    pub fn source(&self) -> &replication_types::configuration::installed::InstalledTableState {
        &self.source
    }
    pub fn table(&self) -> &StoredTableInfo {
        &self.schema.table
    }
    pub fn ttl(&self) -> Option<&CaptureTtlPolicy> {
        self.schema.ttl.as_ref()
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaptureTtlPolicy {
    pub attribute: String,
    pub status: TimeToLiveStatus,
}

/// A content-bound definition from a durably ready source capture. This is
/// input to hidden target preparation, never an activation certificate.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCaptureDefinition {
    proposal: ControlProposal,
    schema: CaptureSchema,
    vector: CanonicalDigest,
    content: CanonicalDigest,
}

impl SourceCaptureDefinition {
    pub(crate) fn validate(&self) -> StorageResult<()> {
        replication_provider::control::source_capture_state(&self.proposal)
            .map_err(|_| corrupt_record())?;
        self.schema.validate(
            &self
                .proposal
                .participant(&self.proposal.operation().home_region)
                .map_err(|_| corrupt_record())?
                .proposed
                .name,
        )?;
        let serialized_digest =
            CanonicalDigest::of_json(&(&self.proposal, &self.schema, self.vector))
                .map_err(|_| corrupt_record())?;
        if self.content != serialized_digest {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn from_ready(
        proposal: ControlProposal,
        schema: CaptureSchema,
        vector: CanonicalDigest,
    ) -> StorageResult<Self> {
        schema.validate(
            &proposal
                .participant(&proposal.operation().home_region)
                .map_err(|_| corrupt_record())?
                .proposed
                .name,
        )?;
        let serialized_digest = CanonicalDigest::of_json(&(&proposal, &schema, vector))
            .map_err(|_| corrupt_record())?;
        Ok(Self {
            proposal,
            schema,
            vector,
            content: serialized_digest,
        })
    }

    pub(crate) fn require_baseline_scope(
        &self,
        baseline: &crate::journal::BaselineAuthority,
    ) -> StorageResult<()> {
        let source = self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?;
        let item = baseline.item();
        if item.source_table != self.proposal.operation().table
            || item.schema != source.proposed.configuration.schema
            || item.lineage != self.proposal.operation().lineage
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub fn proposal(&self) -> &ControlProposal {
        &self.proposal
    }
    pub fn table(&self) -> &StoredTableInfo {
        &self.schema.table
    }
    pub fn ttl(&self) -> Option<&CaptureTtlPolicy> {
        self.schema.ttl.as_ref()
    }
    pub(crate) fn vector_seed(
        proposal: &ControlProposal,
        schema: &CaptureSchema,
    ) -> StorageResult<CanonicalDigest> {
        let serialized_digest =
            CanonicalDigest::of_json(&(proposal, schema)).map_err(|_| corrupt_record())?;
        Ok(serialized_digest)
    }

    pub(crate) fn initialize_vector(&self) -> StorageResult<CanonicalDigest> {
        Self::vector_seed(&self.proposal, &self.schema)
    }
    pub(crate) fn vector(&self) -> CanonicalDigest {
        self.vector
    }

    pub fn content(&self) -> CanonicalDigest {
        self.content
    }

    pub(crate) fn schema(&self) -> &CaptureSchema {
        &self.schema
    }
}

pub(crate) fn logical_table_content(table: &StoredTableInfo) -> StorageResult<CanonicalDigest> {
    // Each target owns its native creation time and mutable statistics.
    // Schema and logical policies remain bound to the capture definition.
    let serialized_digest = CanonicalDigest::of_json(&(
        &table.attribute_definitions,
        &table.key_schema,
        table.max_indexers,
        &table.global_secondary_indexes,
        &table.stream_specification,
        table.table_stream_duration,
        table.default_item_stream_duration,
        table.deletion_protection_enabled,
    ))
    .map_err(|_| corrupt_record())?;
    Ok(serialized_digest)
}
