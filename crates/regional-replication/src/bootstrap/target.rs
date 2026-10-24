use replication_types::{
    configuration::{
        control::ControlOperation,
        installed::{InstalledTableState, RegionalTablePhase},
        joining::JoiningTableDefinition,
    },
    identity::resource::ControlOperationId,
    protocol::{CanonicalBytes, CanonicalDigest},
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{CaptureSchema, SourceSchemaObservation};
use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HiddenTargetRequest {
    pub(crate) operation: ControlOperation,
    pub(crate) source: InstalledTableState,
    pub(crate) target: JoiningTableDefinition,
    pub(crate) schema: CaptureSchema,
}

impl HiddenTargetRequest {
    pub fn bind(
        mut operation: ControlOperation,
        target: JoiningTableDefinition,
        source: &SourceSchemaObservation,
    ) -> Self {
        operation.content = [0; 32].into();
        Self {
            operation,
            source: source.source.clone(),
            target,
            schema: source.schema.clone(),
        }
    }

    pub(crate) fn content(&self) -> StorageResult<CanonicalDigest> {
        let serialized_digest = CanonicalDigest::of_json(self).map_err(|_| corrupt_record())?;
        Ok(serialized_digest)
    }

    pub(crate) fn matches_definition(
        &self,
        definition: &super::SourceCaptureDefinition,
    ) -> StorageResult<bool> {
        self.matches_capture(definition.proposal(), definition.schema())
    }

    pub(crate) fn matches_capture(
        &self,
        proposal: &replication_types::configuration::proposal::ControlProposal,
        schema: &CaptureSchema,
    ) -> StorageResult<bool> {
        let mut operation = proposal.operation().clone();
        operation.content = [0; 32].into();
        let source = proposal
            .participant(&operation.home_region)
            .map_err(|_| corrupt_record())?;
        Ok(operation == self.operation
            && source.before.as_ref() == Some(&self.source)
            && serde_json::to_vec(schema).map_err(|_| corrupt_record())?
                == serde_json::to_vec(&self.schema).map_err(|_| corrupt_record())?)
    }
}

/// Allocated native incarnation after acknowledged commit or fresh durable
/// observation. Reservation creates no grant or public table.
pub struct HiddenTargetReservation {
    pub(crate) operation: ControlOperationId,
    pub(crate) state: InstalledTableState,
    pub(crate) content: CanonicalDigest,
}

impl HiddenTargetReservation {
    pub fn operation(&self) -> ControlOperationId {
        self.operation
    }
    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
    pub fn proposed(&self) -> InstalledTableState {
        let mut state = self.state.clone();
        state.phase = RegionalTablePhase::Active;
        state
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineCopyPage {
    pub sequence: u64,
    pub items: Vec<CanonicalBytes>,
    pub complete: bool,
}

/// Known committed page progress, not proof of tail coverage or projections.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommittedBaselinePage {
    pub sequence: u64,
    pub content: CanonicalDigest,
    pub baselines_complete: bool,
}

pub(crate) fn baseline_page_content(
    definition: CanonicalDigest,
    page: &BaselineCopyPage,
) -> StorageResult<CanonicalDigest> {
    let serialized_digest =
        CanonicalDigest::of_json(&(definition, page)).map_err(|_| corrupt_record())?;
    Ok(serialized_digest)
}
