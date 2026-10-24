//! Terminal descriptions bind the complete ordered native installation set.
use replication_types::{identity::replica::RegionId, protocol::CanonicalDigest};
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use super::commit::TrimCommitCertificate;
use crate::native_record::corrupt_record;

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimInstalledParticipant {
    pub(crate) region: RegionId,
    pub(crate) certificate: CanonicalDigest,
    pub(crate) grant: CanonicalDigest,
    pub(crate) installation: CanonicalDigest,
}

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TrimCompletionCertificate {
    pub(crate) certificate: TrimCommitCertificate,
    pub(crate) installations: Vec<TrimInstalledParticipant>,
}

impl TrimCompletionCertificate {
    pub(crate) fn require_integrity(&self, digest: CanonicalDigest) -> StorageResult<()> {
        self.certificate.require_integrity()?;
        if self.installations.len() != self.certificate.proposal.participants.len() {
            return Err(corrupt_record());
        }
        for (installed, participant) in self
            .installations
            .iter()
            .zip(&self.certificate.proposal.participants)
        {
            if installed.region != participant.region
                || installed.certificate != digest
                || installed.grant != self.certificate.grant_for(&participant.region)?
            {
                return Err(corrupt_record());
            }
        }
        Ok(())
    }

    pub(crate) fn require_local(&self, installed: &TrimInstalledParticipant) -> StorageResult<()> {
        if !self.installations.iter().any(|actual| actual == installed) {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

/// Only the validated configured-primary boundary can request participant
/// release.
pub struct PrimaryTrimCompletion {
    pub(crate) completion: TrimCompletionCertificate,
}

impl PrimaryTrimCompletion {
    pub(crate) fn try_from_primary(
        completion: TrimCompletionCertificate,
        primary: &RegionId,
    ) -> StorageResult<Self> {
        completion.certificate.require_integrity()?;
        if completion.certificate.proposal.primary != *primary {
            return Err(corrupt_record());
        }
        Ok(Self { completion })
    }
}
