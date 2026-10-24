use infrastructure_provider::record::DurableRecordKey;
use replication_types::protocol::{CanonicalBytes, CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::HistoryCaptureRequest;
use crate::{bootstrap::SourceCaptureDefinition, native_record::corrupt_record};

/// Immutable original baseline authority selected from a certified initial
/// enable.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBaselineDefinition {
    request: HistoryCaptureRequest,
    initial: SourceCaptureDefinition,
    content: CanonicalDigest,
}

impl RetainedBaselineDefinition {
    pub(crate) fn from_native(
        request: HistoryCaptureRequest,
        initial: SourceCaptureDefinition,
    ) -> StorageResult<Self> {
        let content =
            CanonicalDigest::of_json(&(&request, &initial)).map_err(|_| corrupt_record())?;
        let definition = Self {
            request,
            initial,
            content,
        };
        definition.validate()?;
        Ok(definition)
    }

    pub fn validate(&self) -> StorageResult<()> {
        self.request.validate()?;
        self.initial.validate()?;
        let current = replication_provider::control::borrow_addition_source(
            &self.request.proposal,
            &self.request.region,
        )
        .map_err(|_| corrupt_record())?;
        let initial = self
            .initial
            .proposal()
            .participant(&self.initial.proposal().operation().home_region)
            .map_err(|_| corrupt_record())?;
        if self.initial.proposal().operation().lineage != current.configuration.lineage
            || initial.proposed.configuration.tenant != current.configuration.tenant
            || !current.schemas.contains(
                &self
                    .initial
                    .proposal()
                    .participant(&self.initial.proposal().operation().home_region)
                    .map_err(|_| corrupt_record())?
                    .proposed
                    .configuration
                    .schema,
            )
        {
            return Err(corrupt_record());
        }
        let serialized_digest = CanonicalDigest::of_json(&(&self.request, &self.initial))
            .map_err(|_| corrupt_record())?;
        if self.content != serialized_digest {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub(crate) fn request(&self) -> &HistoryCaptureRequest {
        &self.request
    }

    pub fn content(&self) -> CanonicalDigest {
        self.content
    }
    pub fn initial(&self) -> &SourceCaptureDefinition {
        &self.initial
    }
    pub(crate) fn require_request(&self, request: &HistoryCaptureRequest) -> StorageResult<()> {
        self.validate()?;
        if self.request.content()? != request.content()? {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBaselineScope {
    pub after: Option<DurableRecordKey>,
    pub max_records: std::num::NonZeroUsize,
    pub max_bytes: std::num::NonZeroUsize,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedBaselinePage {
    pub definition: RetainedBaselineDefinition,
    pub after: Option<DurableRecordKey>,
    pub items: Vec<CanonicalBytes>,
    pub continuation: Option<DurableRecordKey>,
    pub content: CanonicalDigest,
}

impl RetainedBaselinePage {
    pub(crate) fn from_native(
        definition: RetainedBaselineDefinition,
        after: Option<DurableRecordKey>,
        page: crate::journal::BaselineAuthorityPage,
        limit: std::num::NonZeroUsize,
    ) -> StorageResult<Self> {
        let items = page
            .authorities
            .into_iter()
            .map(|authority| {
                CanonicalBytes::try_new(authority.into_bytes(), limit).map_err(|_| corrupt_record())
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let mut page = Self {
            definition,
            after,
            items,
            continuation: page.continuation,
            content: [0; 32].into(),
        };
        page.content = page.expected_content()?;
        Ok(page)
    }

    pub(crate) fn require_scope(
        &self,
        request: &HistoryCaptureRequest,
        scope: &RetainedBaselineScope,
    ) -> StorageResult<()> {
        self.definition.require_request(request)?;
        if self.after != scope.after
            || self.items.len() > scope.max_records.get()
            || self.content != self.expected_content()?
        {
            return Err(corrupt_record());
        }
        let bytes = self.items.iter().try_fold(0usize, |total, item| {
            total
                .checked_add(item.as_ref().len())
                .ok_or_else(corrupt_record)
        })?;
        if bytes > scope.max_bytes.get() {
            return Err(corrupt_record());
        }
        if self.items.is_empty() && self.continuation.is_some() {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(
            self.definition.content(),
            &self.after,
            &self.items,
            &self.continuation,
        ))
        .map_err(|_| corrupt_record())
    }
}
