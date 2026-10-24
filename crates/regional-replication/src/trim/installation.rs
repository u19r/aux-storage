//! Configured primary transport creates installation intent, never native
//! custody.
use replication_types::identity::replica::RegionId;
use storage_types::StorageResult;

use super::commit::TrimCommitCertificate;

/// Process-only intent from the validated primary peer; the native owner still
/// proves its grant.
pub struct PrimaryTrimInstallation {
    pub(crate) certificate: TrimCommitCertificate,
}

impl PrimaryTrimInstallation {
    pub(crate) fn try_from_primary(
        certificate: TrimCommitCertificate,
        primary: &RegionId,
    ) -> StorageResult<Self> {
        certificate.require_integrity()?;
        if certificate.proposal.primary != *primary {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(Self { certificate })
    }
}
