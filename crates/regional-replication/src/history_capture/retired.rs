use replication_types::{
    configuration::{installed::RegionalSourceBinding, proposal::ControlProposal},
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::HistoryCaptureRequest;
use crate::{
    lifecycle::{SealRequest, SealedOriginCut},
    native_record::corrupt_record,
};

/// Complete native survivor facts, never a membership grant or an imported
/// local control decision.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredHistoryDefinition {
    request: HistoryCaptureRequest,
    removal: SealRequest,
    vector: CanonicalDigest,
    custody: RetiredHistoryCustody,
    content: CanonicalDigest,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum RetiredHistoryCustody {
    RemovalSurvivor,
    VerifiedAddition {
        proposal: Box<ControlProposal>,
        bootstrap: CanonicalDigest,
    },
}

impl RetiredHistoryDefinition {
    pub(crate) fn from_native(
        request: HistoryCaptureRequest,
        removal: SealRequest,
        vector: CanonicalDigest,
    ) -> StorageResult<Self> {
        let mut definition = Self {
            request,
            removal,
            vector,
            custody: RetiredHistoryCustody::RemovalSurvivor,
            content: [0; 32].into(),
        };
        definition.content = definition.expected_content()?;
        definition.validate()?;
        Ok(definition)
    }

    pub(crate) fn from_adoption(
        request: HistoryCaptureRequest,
        original: &Self,
        proposal: ControlProposal,
        bootstrap: CanonicalDigest,
    ) -> StorageResult<Self> {
        original.validate()?;
        let mut definition = Self {
            request,
            removal: original.removal.clone(),
            vector: original.vector,
            custody: RetiredHistoryCustody::VerifiedAddition {
                proposal: Box::new(proposal),
                bootstrap,
            },
            content: [0; 32].into(),
        };
        definition.content = definition.expected_content()?;
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.request.validate()?;
        self.removal.validate()?;
        let current = replication_provider::control::borrow_addition_source(
            &self.request.proposal,
            &self.request.region,
        )
        .map_err(|_| corrupt_record())?;
        let sealed = self.removal.source()?;
        let is_current = current.sources.iter().any(|source| {
            source.region == sealed.region
                && source.database_group == sealed.database_group
                && source.generation == sealed.generation
                && source.table == sealed.table
        });
        if (is_current
            && matches!(&current.configuration.regional, replication_types::configuration::table::RegionalMode::Global { members, .. } if members.contains(&sealed.region)))
            || self.removal.before()?.configuration.lineage != current.configuration.lineage
            || self.removal.before()?.configuration.tenant != current.configuration.tenant
        {
            return Err(corrupt_record());
        }
        self.require_custody(current)?;
        if self.content != self.expected_content()? {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn require_custody(
        &self,
        current: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<()> {
        let custodian = match &self.custody {
            RetiredHistoryCustody::RemovalSurvivor => {
                if !self.removal.survivors()?.contains(&self.request.region) {
                    return Err(corrupt_record());
                }
                self.removal
                    .proposal
                    .participant(&self.request.region)
                    .map_err(|_| corrupt_record())?
            }
            RetiredHistoryCustody::VerifiedAddition { proposal, .. } => {
                replication_provider::control::validate_addition(proposal)
                    .map_err(|_| corrupt_record())?;
                if !matches!(&proposal.operation().change, replication_types::configuration::control::ConfigurationChange::AddRegion { region } if region == &self.request.region)
                {
                    return Err(corrupt_record());
                }
                let target = proposal
                    .participant(&self.request.region)
                    .map_err(|_| corrupt_record())?;
                if target.before.is_some()
                    || target.proposed.configuration.lineage != current.configuration.lineage
                    || target.proposed.membership > current.membership
                {
                    return Err(corrupt_record());
                }
                target
            }
        };
        if custodian.proposed.configuration.table != current.configuration.table
            || custodian.proposed.physical != current.physical
            || custodian.proposed.configuration.tenant != current.configuration.tenant
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(&self.request, &self.removal, self.vector, &self.custody))
            .map_err(|_| corrupt_record())
    }

    pub(crate) fn removal_scope(&self) -> &SealRequest {
        &self.removal
    }

    pub(crate) fn into_original_seal(self) -> SealRequest {
        self.removal
    }

    pub(crate) fn is_adopted(&self) -> bool {
        matches!(self.custody, RetiredHistoryCustody::VerifiedAddition { .. })
    }

    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
    pub fn vector(&self) -> CanonicalDigest {
        self.vector
    }
    pub(crate) fn request(&self) -> &HistoryCaptureRequest {
        &self.request
    }
    pub fn source(&self) -> StorageResult<&RegionalSourceBinding> {
        self.removal.source()
    }
    pub fn removal(&self) -> &ControlProposal {
        &self.removal.proposal
    }
    pub(crate) fn removal_content(&self) -> StorageResult<CanonicalDigest> {
        self.removal.content()
    }

    pub(crate) fn require_request(
        &self,
        request: &HistoryCaptureRequest,
        source: &RegionalSourceBinding,
    ) -> StorageResult<()> {
        self.require_capture_request(request)?;
        if self.source()? != source {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn require_capture_request(
        &self,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<()> {
        self.validate()?;
        if self.request.content()? != request.content()? {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn require_page(&self, page: &RetiredHistoryPage, after: u32) -> StorageResult<()> {
        self.validate()?;
        if page.definition.content != self.content {
            return Err(corrupt_record());
        }
        let width = u32::try_from(page.cuts.len()).map_err(|_| corrupt_record())?;
        let end = after.checked_add(width).ok_or_else(corrupt_record)?;
        let source = self.source()?;
        let count = u32::from(source.partitions);
        if width == 0
            || page.cuts.len() > crate::constants::HISTORY_CUT_PAGE_WIDTH
            || end > count
            || page.next_partition != (end < count).then_some(end)
        {
            return Err(corrupt_record());
        }
        let survivors = self.removal.survivors()?;
        for (index, cut) in page.cuts.iter().enumerate() {
            self.removal.require_history_cut(
                (&cut.origin, cut.through, &cut.receipts),
                after + index as u32,
                survivors,
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredHistoryPage {
    pub definition: RetiredHistoryDefinition,
    pub cuts: Vec<SealedOriginCut>,
    pub next_partition: Option<u32>,
}
