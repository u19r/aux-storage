//! Immutable old baseline dependencies; transfer never materializes their
//! images.
use replication_types::{
    configuration::installed::InstalledTableState,
    protocol::{CanonicalBytes, CanonicalDigest},
};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::SourceCaptureDefinition;
use crate::{journal::BaselineAuthority, native_record::corrupt_record};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableBaselineDefinition {
    pub(crate) initial: SourceCaptureDefinition,
    pub(crate) count: u64,
    pub(crate) vector: CanonicalDigest,
    pub(crate) content: CanonicalDigest,
}

impl ReenableBaselineDefinition {
    pub(crate) fn from_native(
        initial: SourceCaptureDefinition,
        inventory: (u64, CanonicalDigest),
    ) -> StorageResult<Self> {
        initial.validate()?;
        let content = CanonicalDigest::of_json(&(initial.content(), inventory))
            .map_err(|_| corrupt_record())?;
        Ok(Self {
            initial,
            count: inventory.0,
            vector: inventory.1,
            content,
        })
    }
    pub(crate) fn validate(&self) -> StorageResult<()> {
        self.initial.validate()?;
        if self.content
            != CanonicalDigest::of_json(&(self.initial.content(), (self.count, self.vector)))
                .map_err(|_| corrupt_record())?
            || (self.count == 0 && self.vector != self.seed()?)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
    pub(crate) fn require_lineage(&self, state: &InstalledTableState) -> StorageResult<()> {
        self.validate()?;
        let initial = self.initial.proposal();
        let home = &initial
            .participant(&initial.operation().home_region)
            .map_err(|_| corrupt_record())?
            .proposed;
        if initial.operation().lineage != state.configuration.lineage
            || home.configuration.tenant != state.configuration.tenant
            || !state.schemas.contains(&home.configuration.schema)
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
    pub(crate) fn seed(&self) -> StorageResult<CanonicalDigest> {
        Self::initial_seed(&self.initial)
    }
    pub(crate) fn initial_seed(
        initial: &SourceCaptureDefinition,
    ) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(initial.proposal().operation().lineage, initial.content()))
            .map_err(|_| corrupt_record())
    }
    pub(crate) fn fold_authority(
        &self,
        vector: CanonicalDigest,
        authority: &BaselineAuthority,
    ) -> StorageResult<CanonicalDigest> {
        self.initial.require_baseline_scope(authority)?;
        Self::fold_bytes(vector, authority.bytes())
    }
    pub(crate) fn fold_bytes(
        vector: CanonicalDigest,
        bytes: &[u8],
    ) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(vector, CanonicalDigest::of(bytes)))
            .map_err(|_| corrupt_record())
    }
    pub(crate) fn require_complete(
        &self,
        count: u64,
        vector: CanonicalDigest,
    ) -> StorageResult<()> {
        if count != self.count || vector != self.vector {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableBaselinePage {
    pub definition: CanonicalDigest,
    pub sequence: u64,
    pub items: Vec<CanonicalBytes>,
    pub complete: bool,
    pub content: CanonicalDigest,
}
impl ReenableBaselinePage {
    pub(crate) fn from_native(
        definition: CanonicalDigest,
        sequence: u64,
        items: Vec<CanonicalBytes>,
        complete: bool,
    ) -> StorageResult<Self> {
        let mut page = Self {
            definition,
            sequence,
            items,
            complete,
            content: [0; 32].into(),
        };
        page.content = page.expected_content()?;
        Ok(page)
    }
    pub(crate) fn require_content(
        &self,
        definition: &ReenableBaselineDefinition,
        budget: super::copy_scope::BaselineCopyBudget,
    ) -> StorageResult<()> {
        let bytes = self.items.iter().try_fold(0usize, |sum, item| {
            sum.checked_add(item.as_ref().len())
                .ok_or_else(corrupt_record)
        })?;
        if self.definition != definition.content
            || self.content != self.expected_content()?
            || self.items.len() > budget.records.get()
            || bytes > budget.bytes.get()
            || (!self.complete && self.items.is_empty())
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(self.definition, self.sequence, &self.items, self.complete))
            .map_err(|_| corrupt_record())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReenableBaselineReceipt {
    pub definition: CanonicalDigest,
    pub sequence: u64,
    pub content: CanonicalDigest,
    pub complete: bool,
}
impl ReenableBaselineReceipt {
    pub(crate) fn require_page(&self, page: &ReenableBaselinePage) -> StorageResult<()> {
        if self.definition != page.definition
            || self.sequence != page.sequence
            || self.content != page.content
            || self.complete != page.complete
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BaselineDependencyProgress {
    pub sequence: u64,
    pub count: u64,
    pub vector: CanonicalDigest,
    pub complete: bool,
}
impl BaselineDependencyProgress {
    pub(crate) fn from_definition(definition: &ReenableBaselineDefinition) -> StorageResult<Self> {
        Ok(Self {
            sequence: 0,
            count: 0,
            vector: definition.seed()?,
            complete: false,
        })
    }
    pub(crate) fn require_definition(
        &self,
        definition: &ReenableBaselineDefinition,
    ) -> StorageResult<()> {
        if self.count > definition.count
            || (self.sequence == 0
                && (self.count != 0 || self.vector != definition.seed()? || self.complete))
        {
            return Err(corrupt_record());
        }
        if self.complete {
            definition.require_complete(self.count, self.vector)?;
        }
        Ok(())
    }
    pub(crate) fn accept_page(
        &mut self,
        definition: &ReenableBaselineDefinition,
        input: (
            &ReenableBaselinePage,
            &replication_provider::canonical_event_codec::CanonicalJsonCodec,
            super::copy_scope::BaselineCopyBudget,
        ),
    ) -> StorageResult<Vec<BaselineAuthority>> {
        self.require_definition(definition)?;
        let (page, codec, budget) = input;
        page.require_content(definition, budget)?;
        if self.complete || page.sequence != self.sequence {
            return Err(corrupt_record());
        }
        let mut authorities = Vec::with_capacity(page.items.len());
        for bytes in &page.items {
            let authority = BaselineAuthority::decode(std::sync::Arc::from(bytes.as_ref()), codec)?;
            self.vector = definition.fold_authority(self.vector, &authority)?;
            self.count = self.count.checked_add(1).ok_or_else(corrupt_record)?;
            authorities.push(authority);
        }
        if self.count > definition.count {
            return Err(corrupt_record());
        }
        self.sequence = self.sequence.checked_add(1).ok_or_else(corrupt_record)?;
        self.complete = page.complete;
        self.require_definition(definition)?;
        Ok(authorities)
    }
}
