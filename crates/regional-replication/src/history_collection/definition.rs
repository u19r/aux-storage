use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::HistoryCollectionRecord;
use crate::{
    bootstrap::SourceCaptureDefinition,
    history_capture::{HistoryCaptureDefinition, RetainedBaselineDefinition},
    native_record::corrupt_record,
};

/// Complete collected source declarations. Hidden native verification still
/// owns activation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryAdoptionDefinition {
    current: HistoryCaptureDefinition,
    initial: SourceCaptureDefinition,
    collection: CanonicalDigest,
    retained: Option<AdoptedRetainedInventory>,
    content: CanonicalDigest,
}

impl HistoryAdoptionDefinition {
    pub(crate) fn from_native(
        root: &HistoryCollectionRecord,
        current: HistoryCaptureDefinition,
    ) -> StorageResult<Self> {
        root.validate()?;
        if !root.is_complete()
            || current.request().proposal != root.proposal
            || current.policy_digest()? != root.policy
        {
            return Err(corrupt_record());
        }
        let initial = root.initial.clone().ok_or_else(corrupt_record)?;
        let mut definition = Self {
            current,
            initial,
            collection: root.content,
            retained: root
                .retained
                .clone()
                .map(|source| AdoptedRetainedInventory {
                    source,
                    inventory: root.vector,
                }),
            content: [0; 32].into(),
        };
        definition.content = definition.expected_content()?;
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.current.validate()?;
        let proposal = &self.current.request().proposal;
        replication_provider::control::validate_addition(proposal).map_err(|_| corrupt_record())?;
        if self.current.request().region != proposal.operation().home_region
            || self.content != self.expected_content()?
        {
            return Err(corrupt_record());
        }
        if let Some(retained) = &self.retained {
            crate::bootstrap::ReenableHistoryCustody {
                definition: retained.source.clone(),
                ordinal: 0,
            }
            .require_custodian(proposal)?;
        }
        RetainedBaselineDefinition::from_native(
            self.current.request().clone(),
            self.initial.clone(),
        )?;
        Ok(())
    }

    pub(crate) fn require_collection(
        &self,
        collection: &HistoryCollectionRecord,
    ) -> StorageResult<()> {
        self.validate()?;
        collection.validate()?;
        if collection.proposal != *self.proposal()
            || collection.policy != self.current.policy_digest()?
        {
            return Err(corrupt_record());
        }
        if collection.is_complete()
            && (collection.content != self.collection
                || collection.initial.as_ref().map(|initial| initial.content())
                    != Some(self.initial.content()))
        {
            return Err(corrupt_record());
        }
        if let Some(source) = &collection.retained {
            let expected = self.retained.as_ref().ok_or_else(corrupt_record)?;
            source.require_native_inventory(&expected.source)?;
        }
        if collection.is_complete() {
            let retained = collection
                .retained
                .clone()
                .map(|source| AdoptedRetainedInventory {
                    source,
                    inventory: collection.vector,
                });
            if CanonicalDigest::of_json(&retained).map_err(|_| corrupt_record())?
                != CanonicalDigest::of_json(&self.retained).map_err(|_| corrupt_record())?
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn current(&self) -> &HistoryCaptureDefinition {
        &self.current
    }

    pub fn proposal(&self) -> &ControlProposal {
        &self.current.request().proposal
    }
    pub fn initial(&self) -> &SourceCaptureDefinition {
        &self.initial
    }
    pub fn table(&self) -> &storage_types::StoredTableInfo {
        self.current.table()
    }
    pub fn ttl(&self) -> Option<&crate::bootstrap::CaptureTtlPolicy> {
        self.current.ttl()
    }
    pub fn collection(&self) -> CanonicalDigest {
        self.collection
    }
    pub fn content(&self) -> CanonicalDigest {
        self.content
    }

    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(
            &self.current,
            &self.initial,
            self.collection,
            &self.retained,
        ))
        .map_err(|_| corrupt_record())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdoptedRetainedInventory {
    source: crate::bootstrap::RetainedHistoryInventory,
    inventory: CanonicalDigest,
}

impl HistoryAdoptionDefinition {
    pub(crate) fn retained_inventory(
        &self,
    ) -> StorageResult<Option<crate::bootstrap::RetainedHistoryInventory>> {
        self.validate()?;
        Ok(self
            .retained
            .as_ref()
            .map(|retained| crate::bootstrap::RetainedHistoryInventory {
                proposal: self.proposal().clone(),
                bootstrap: self.content,
                cuts: retained.source.cuts,
                inventory: retained.inventory,
            }))
    }
}
