//! A prospective selection owns progress, never custody or installed authority.
use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};
use storage_types::StorageResult;

use crate::trim::{original::OriginalPrefixWitness, reservation::ReservationRevision};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum ArchiveAdmissionPhase {
    Classify,
    Original { witness: OriginalPrefixWitness },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ArchiveAdmissionScan {
    selection: CanonicalDigest,
    barrier: ReservationRevision,
    pub(crate) scope: Option<CanonicalDigest>,
    pub(crate) phase: ArchiveAdmissionPhase,
}

impl ArchiveAdmissionScan {
    pub(crate) fn new(barrier: ReservationRevision, selection: CanonicalDigest) -> Self {
        Self {
            selection,
            barrier,
            scope: None,
            phase: ArchiveAdmissionPhase::Classify,
        }
    }

    pub(crate) fn require_authority(
        &mut self,
        observed: (ReservationRevision, CanonicalDigest),
    ) -> StorageResult<()> {
        if self.barrier > observed.0 {
            return Err(crate::native_record::corrupt_record());
        }
        if self.selection != observed.1 {
            return Err(crate::native_record::corrupt_record());
        }
        if self.scope.is_none() {
            self.require_initial_progress()?;
        }
        if self.barrier != observed.0 {
            *self = Self::new(observed.0, observed.1);
        }
        Ok(())
    }

    pub(crate) fn require_initial_progress(&self) -> StorageResult<()> {
        if self.scope.is_some() || !matches!(self.phase, ArchiveAdmissionPhase::Classify) {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ArchiveAdmissionWork {
    Qualified,
    Advanced,
    Waiting,
}
