use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::{StorageResult, TableHistoryId};

use super::VerificationProgress;
use crate::native_record::corrupt_record;

/// Durable descriptive evidence. Only a fresh protected native observation can
/// turn this record into control authority; a wire value grants no activation.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BootstrapVerification {
    request: CanonicalDigest,
    definition: CanonicalDigest,
    physical: TableHistoryId,
    vector: CanonicalDigest,
    cuts: u64,
    history: CanonicalDigest,
    winners: CanonicalDigest,
    projections: CanonicalDigest,
    interpretation: u32,
    content: CanonicalDigest,
}

impl BootstrapVerification {
    pub(crate) fn complete(
        binding: (CanonicalDigest, CanonicalDigest, TableHistoryId),
        progress: &VerificationProgress,
    ) -> StorageResult<Self> {
        let projections = CanonicalDigest::of_json(&(
            &progress.winners,
            &progress.projection_counts,
            progress.ttl_count,
        ))
        .map_err(|_| corrupt_record())?;
        let mut evidence = Self {
            request: binding.0,
            definition: binding.1,
            physical: binding.2,
            vector: progress.vector,
            cuts: progress.cut_count,
            history: progress.history,
            winners: progress.winners,
            projections,
            interpretation: 2,
            content: binding.0,
        };
        evidence.content = evidence.expected_content()?;
        Ok(evidence)
    }

    pub(crate) fn require_binding(
        &self,
        binding: (CanonicalDigest, CanonicalDigest, TableHistoryId),
        progress: &VerificationProgress,
    ) -> StorageResult<()> {
        let expected = Self::complete(binding, progress)?;
        if self.content != self.expected_content()? || self.content != expected.content {
            return Err(corrupt_record());
        }
        Ok(())
    }

    fn expected_content(&self) -> StorageResult<CanonicalDigest> {
        let serialized_digest = CanonicalDigest::of_json(&(
            self.request,
            self.definition,
            self.physical,
            self.vector,
            self.cuts,
            self.history,
            self.winners,
            self.projections,
            self.interpretation,
        ))
        .map_err(|_| corrupt_record())?;
        Ok(serialized_digest)
    }
}
