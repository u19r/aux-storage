use replication_types::configuration::proposal::ControlProposal;
use serde::{Deserialize, Serialize};
use storage_backfill::ScanProgress;
use storage_types::KeyAttributes;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CaptureRecord {
    pub proposal: ControlProposal,
    pub schema: crate::bootstrap::CaptureSchema,
    pub vector: replication_types::protocol::CanonicalDigest,
    pub phase: CapturePhase,
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) enum CapturePhase {
    Holding(u32),
    Live(ScanProgress<(), String>),
    Deleted(ScanProgress<(), KeyAttributes>),
    High(u32),
    History,
    BaselineReady,
    Releasing(u32),
    Released,
    Cancelling(u32),
    Cancelled,
}

impl CaptureRecord {
    pub(super) fn require_partition(
        &self,
        partitions: replication_types::identity::journal_partition::JournalPartitionCount,
    ) -> storage_types::StorageResult<()> {
        let next = match self.phase {
            CapturePhase::Holding(next)
            | CapturePhase::High(next)
            | CapturePhase::Releasing(next)
            | CapturePhase::Cancelling(next) => next,
            _ => return Ok(()),
        };
        if next >= u32::from(partitions) {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}
