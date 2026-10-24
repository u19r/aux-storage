use infrastructure_provider::record::DurableRecordKey;
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::{HistoryCaptureRequest, RetiredHistoryDefinition};
use crate::native_record::corrupt_record;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryCatalogScope {
    pub after: Option<DurableRecordKey>,
    pub max_records: std::num::NonZeroUsize,
    pub max_bytes: std::num::NonZeroUsize,
}

/// Exact retired generations retained by this survivor; no membership
/// authority.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredHistoryCatalogPage {
    pub request: CanonicalDigest,
    pub after: Option<DurableRecordKey>,
    pub entries: Vec<RetiredHistoryDefinition>,
    pub continuation: Option<DurableRecordKey>,
    pub content: CanonicalDigest,
}

impl RetiredHistoryCatalogPage {
    pub(crate) fn from_native(
        request: &HistoryCaptureRequest,
        scope: &HistoryCatalogScope,
        entries: Vec<RetiredHistoryDefinition>,
        continuation: Option<DurableRecordKey>,
    ) -> StorageResult<Self> {
        let mut page = Self {
            request: request.content()?,
            after: scope.after.clone(),
            entries,
            continuation,
            content: [0; 32].into(),
        };
        page.content = page.expected_content()?;
        page.require_scope(request, scope)?;
        Ok(page)
    }

    pub(crate) fn require_scope(
        &self,
        request: &HistoryCaptureRequest,
        scope: &HistoryCatalogScope,
    ) -> StorageResult<()> {
        if self.request != request.content()?
            || self.after != scope.after
            || self.entries.len() > scope.max_records.get()
            || self.content != self.expected_content()?
            || (self.entries.is_empty() && self.continuation.is_some())
            || self
                .continuation
                .as_ref()
                .is_some_and(|next| self.after.as_ref().is_some_and(|after| next <= after))
        {
            return Err(corrupt_record());
        }
        for entry in &self.entries {
            entry.require_capture_request(request)?;
        }
        CanonicalDigest::of_json_bounded(self, scope.max_bytes).map_err(|error| {
            if error.io_error_kind() == Some(std::io::ErrorKind::FileTooLarge) {
                storage_types::InfrastructureResultKind::InvalidBound {
                    context: storage_types::InfrastructureContext::RecordPage,
                }
                .into()
            } else {
                corrupt_record()
            }
        })?;
        Ok(())
    }

    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(self.request, &self.after, &self.entries, &self.continuation))
            .map_err(|_| corrupt_record())
    }
}
