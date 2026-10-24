use replication_types::{configuration::proposal::ControlProposal, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{CaptureSchema, SourceCaptureDefinition};
use crate::history_collection::HistoryAdoptionDefinition;

/// Frozen target input. Neither alternative grants membership or activation.
#[derive(Clone, Serialize, Deserialize)]
pub enum TargetBootstrapDefinition {
    Initial(Box<SourceCaptureDefinition>),
    Addition(Box<HistoryAdoptionDefinition>),
    Reenable(Box<super::ReenableHistoryDefinition>),
}

impl TargetBootstrapDefinition {
    pub(crate) fn validate(&self) -> StorageResult<()> {
        match self {
            Self::Initial(definition) => {
                definition.validate()?;
                let proposal = definition.proposal();
                let before = proposal
                    .participant(&proposal.operation().home_region)
                    .map_err(|_| crate::native_record::corrupt_record())?
                    .before
                    .as_ref()
                    .ok_or_else(crate::native_record::corrupt_record)?;
                if u64::from(before.configuration.config) != 0 {
                    return Err(crate::native_record::corrupt_record());
                }
                Ok(())
            }
            Self::Addition(definition) => definition.validate(),
            Self::Reenable(definition) => definition.validate(),
        }
    }

    pub fn proposal(&self) -> &ControlProposal {
        match self {
            Self::Initial(definition) => definition.proposal(),
            Self::Addition(definition) => definition.proposal(),
            Self::Reenable(definition) => definition.initial().proposal(),
        }
    }

    pub fn content(&self) -> CanonicalDigest {
        match self {
            Self::Initial(definition) => definition.content(),
            Self::Addition(definition) => definition.content(),
            Self::Reenable(definition) => definition.content(),
        }
    }

    pub(crate) fn initial(&self) -> &SourceCaptureDefinition {
        match self {
            Self::Initial(definition) => definition,
            Self::Addition(definition) => definition.initial(),
            Self::Reenable(definition) => definition.initial(),
        }
    }

    pub(crate) fn schema(&self) -> &CaptureSchema {
        match self {
            Self::Initial(definition) => definition.schema(),
            Self::Addition(definition) => definition.current().schema(),
            Self::Reenable(definition) => definition.initial().schema(),
        }
    }

    pub(crate) fn verification_progress(&self) -> StorageResult<super::VerificationProgress> {
        let (vector, stage) = match self {
            Self::Initial(definition) => (
                definition.initialize_vector()?,
                super::VerificationStage::Cuts(0),
            ),
            Self::Addition(definition) => match definition.retained_inventory()? {
                Some(inventory) => (
                    inventory.inventory_seed()?,
                    super::VerificationStage::RetainedCuts(0),
                ),
                None => (
                    definition.collection(),
                    super::VerificationStage::AdoptedCuts(None),
                ),
            },
            Self::Reenable(definition) => (
                definition.inventory_seed()?,
                super::VerificationStage::RetainedCuts(0),
            ),
        };
        Ok(super::VerificationProgress {
            vector,
            history: self.content(),
            winners: self.content(),
            stage,
            projection_counts: Vec::new(),
            ttl_count: 0,
            cut_count: 0,
        })
    }

    pub(crate) fn vector(&self) -> CanonicalDigest {
        match self {
            Self::Initial(definition) => definition.vector(),
            Self::Addition(definition) => definition.collection(),
            Self::Reenable(definition) => definition.initial().vector(),
        }
    }
}

impl From<SourceCaptureDefinition> for TargetBootstrapDefinition {
    fn from(definition: SourceCaptureDefinition) -> Self {
        Self::Initial(Box::new(definition))
    }
}

impl From<HistoryAdoptionDefinition> for TargetBootstrapDefinition {
    fn from(definition: HistoryAdoptionDefinition) -> Self {
        Self::Addition(Box::new(definition))
    }
}

impl TargetBootstrapDefinition {
    pub(crate) fn retained_inventory(
        &self,
    ) -> StorageResult<Option<super::RetainedHistoryInventory>> {
        self.validate()?;
        match self {
            Self::Initial(_) => Ok(None),
            Self::Addition(definition) => definition.retained_inventory(),
            Self::Reenable(definition) => {
                super::RetainedHistoryInventory::try_from(definition.as_ref()).map(Some)
            }
        }
    }
}
