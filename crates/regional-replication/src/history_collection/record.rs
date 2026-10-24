use infrastructure_provider::record::DurableRecordKey;
use replication_types::{
    configuration::{proposal::ControlProposal, table::RegionalMode},
    identity::replica::RegionId,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use crate::{
    history_capture::{HistoryCaptureDefinition, HistoryCaptureRequest, HistoryCaptureVectorPage},
    native_record::corrupt_record,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryCollectionRecord {
    pub proposal: ControlProposal,
    pub ancestry: AncestryCollectionStage,
    pub remaining_retired: Vec<RegionId>,
    pub initial: Option<crate::bootstrap::SourceCaptureDefinition>,
    pub retained: Option<crate::bootstrap::RetainedHistoryInventory>,
    pub source_index: usize,
    pub current: Option<HistoryCaptureDefinition>,
    pub next_partition: u32,
    pub vector: CanonicalDigest,
    pub policy: CanonicalDigest,
    pub content: CanonicalDigest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoryCollectionObservation {
    pub revision: Revision,
    pub record: HistoryCollectionRecord,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum HistoryCollectionChange {
    Definition(HistoryCaptureDefinition),
    Page(HistoryCaptureVectorPage),
    Catalog(crate::history_capture::RetiredHistoryCatalogPage),
    Retired(crate::history_capture::RetiredHistoryPage),
    Baseline(crate::history_capture::RetainedBaselineDefinition),
    RetainedDefinition(Option<crate::bootstrap::RetainedHistoryInventory>),
    RetainedPage(Box<crate::bootstrap::ReenableHistoryPage>),
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum AncestryCollectionStage {
    Active,
    Catalog {
        after: Option<DurableRecordKey>,
    },
    Retired {
        definition: Box<crate::history_capture::RetiredHistoryDefinition>,
        next: u32,
        vector: CanonicalDigest,
        after: Option<DurableRecordKey>,
    },
    Baseline,
    RetainedDefinition,
    RetainedPages {
        next: u64,
        vector: CanonicalDigest,
    },
    Complete,
}

impl HistoryCollectionRecord {
    pub(crate) fn from_primary_definition(
        definition: &HistoryCaptureDefinition,
    ) -> StorageResult<Self> {
        definition.validate()?;
        let request = definition.request();
        if request.region != request.proposal.operation().home_region {
            return Err(corrupt_record());
        }
        let mut record = Self {
            proposal: request.proposal.clone(),
            ancestry: AncestryCollectionStage::Active,
            remaining_retired: Vec::new(),
            initial: None,
            retained: None,
            source_index: 0,
            current: None,
            next_partition: 0,
            vector: definition.vector_seed()?,
            policy: definition.policy_digest()?,
            content: request.content()?,
        };
        record.remaining_retired = record.required_retired_regions()?;
        record.validate()?;
        Ok(record)
    }

    pub(crate) fn sources(&self) -> StorageResult<&[RegionId]> {
        let before = self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        let RegionalMode::Global { members, .. } = &before.configuration.regional else {
            return Err(corrupt_record());
        };
        Ok(members)
    }

    pub(crate) fn request(&self) -> StorageResult<Option<HistoryCaptureRequest>> {
        Ok(self
            .sources()?
            .get(self.source_index)
            .map(|region| HistoryCaptureRequest {
                proposal: self.proposal.clone(),
                region: region.clone(),
            }))
    }

    pub(crate) fn required_retired_regions(&self) -> StorageResult<Vec<RegionId>> {
        let before = self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        let members = self.sources()?;
        Ok(before
            .sources
            .iter()
            .filter(|source| !members.contains(&source.region))
            .map(|source| source.region.clone())
            .collect())
    }

    pub(crate) fn validate(&self) -> StorageResult<()> {
        replication_provider::control::validate_addition(&self.proposal)
            .map_err(|_| corrupt_record())?;
        if let Some(initial) = &self.initial {
            initial.validate()?;
        }
        let required = self.required_retired_regions()?;
        if self
            .remaining_retired
            .iter()
            .any(|region| !required.contains(region))
            || self
                .remaining_retired
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(corrupt_record());
        }
        if self.source_index == self.sources()?.len()
            && (self.initial.is_none()
                || self.current.is_some()
                || !self.remaining_retired.is_empty()
                || !matches!(
                    self.ancestry,
                    AncestryCollectionStage::RetainedDefinition
                        | AncestryCollectionStage::RetainedPages { .. }
                        | AncestryCollectionStage::Complete
                ))
        {
            return Err(corrupt_record());
        }
        self.require_retained_progress()?;
        if self.source_index > self.sources()?.len() {
            return Err(corrupt_record());
        }
        if !matches!(self.ancestry, AncestryCollectionStage::Active) && self.current.is_some() {
            return Err(corrupt_record());
        }
        if let AncestryCollectionStage::Retired {
            definition, next, ..
        } = &self.ancestry
        {
            definition.require_capture_request(&self.request()?.ok_or_else(corrupt_record)?)?;
            if *next >= u32::from(definition.source()?.partitions) {
                return Err(corrupt_record());
            }
        }
        if let Some(definition) = &self.current {
            definition.validate()?;
            let source = replication_provider::control::borrow_addition_source(
                &self.proposal,
                &definition.request().region,
            )
            .map_err(|_| corrupt_record())?;
            let binding = source
                .sources
                .iter()
                .find(|binding| binding.region == definition.request().region)
                .ok_or_else(corrupt_record)?;
            if self.next_partition >= u32::from(binding.partitions) {
                return Err(corrupt_record());
            }
            if Some(definition.request().content()?)
                != self
                    .request()?
                    .map(|request| request.content())
                    .transpose()?
                || definition.policy_digest()? != self.policy
            {
                return Err(corrupt_record());
            }
        } else if self.next_partition != 0 {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl HistoryCollectionRecord {
    pub(crate) fn is_complete(&self) -> bool {
        matches!(self.ancestry, AncestryCollectionStage::Complete)
    }

    pub(crate) fn work_request(&self) -> StorageResult<Option<HistoryCaptureRequest>> {
        if self.is_complete() {
            return Ok(None);
        }
        Ok(Some(self.request()?.unwrap_or_else(|| {
            HistoryCaptureRequest {
                proposal: self.proposal.clone(),
                region: self.proposal.operation().home_region.clone(),
            }
        })))
    }

    pub(crate) fn retained_seed(&self) -> StorageResult<CanonicalDigest> {
        let before = self
            .proposal
            .participant(&self.proposal.operation().home_region)
            .map_err(|_| corrupt_record())?
            .before
            .as_ref()
            .ok_or_else(corrupt_record)?;
        CanonicalDigest::of_json(&(self.proposal.operation().content, before.physical))
            .map_err(|_| corrupt_record())
    }

    fn require_retained_progress(&self) -> StorageResult<()> {
        let is_inventory = matches!(
            self.ancestry,
            AncestryCollectionStage::RetainedDefinition
                | AncestryCollectionStage::RetainedPages { .. }
                | AncestryCollectionStage::Complete
        );
        if is_inventory != (self.source_index == self.sources()?.len()) {
            return Err(corrupt_record());
        }
        if let Some(definition) = &self.retained {
            crate::bootstrap::ReenableHistoryCustody {
                definition: definition.clone(),
                ordinal: 0,
            }
            .require_custodian(&self.proposal)?;
            if !matches!(
                self.ancestry,
                AncestryCollectionStage::RetainedPages { .. } | AncestryCollectionStage::Complete
            ) {
                return Err(corrupt_record());
            }
        }
        if let AncestryCollectionStage::RetainedPages { next, vector } = self.ancestry {
            let definition = self.retained.as_ref().ok_or_else(corrupt_record)?;
            if next >= definition.cuts
                || (next == 0
                    && (vector != definition.inventory_seed()?
                        || self.vector != self.retained_seed()?))
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }
}
