use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

use crate::{
    bootstrap::CaptureSchema,
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryCaptureRecord {
    pub request: HistoryCaptureRequest,
    pub schema: CaptureSchema,
    pub vector: CanonicalDigest,
    pub phase: HistoryCapturePhase,
    pub retained: Option<crate::bootstrap::TargetBootstrapDefinition>,
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub(super) enum HistoryCapturePhase {
    Admitting {
        barrier: crate::trim::reservation::ReservationRevision,
        next: u64,
        vector: CanonicalDigest,
    },
    Holding(u32),
    High(u32),
    Ready,
    Releasing(u32),
    Released,
    Cancelling(u32),
    Cancelled,
}

pub(super) fn capture_status(phase: HistoryCapturePhase) -> HistoryCaptureStatus {
    match phase {
        HistoryCapturePhase::Admitting { .. } | HistoryCapturePhase::Holding(_) => {
            HistoryCaptureStatus::Holding
        }
        HistoryCapturePhase::High(_) => HistoryCaptureStatus::CapturingHigh,
        HistoryCapturePhase::Ready => HistoryCaptureStatus::Ready,
        HistoryCapturePhase::Releasing(_) => HistoryCaptureStatus::Releasing,
        HistoryCapturePhase::Released => HistoryCaptureStatus::Released,
        HistoryCapturePhase::Cancelling(_) => HistoryCaptureStatus::Cancelling,
        HistoryCapturePhase::Cancelled => HistoryCaptureStatus::Cancelled,
    }
}

impl HistoryCaptureRecord {
    pub(super) fn require_local(
        &self,
        region: &replication_types::identity::replica::RegionId,
        partitions: replication_types::identity::journal_partition::JournalPartitionCount,
    ) -> storage_types::StorageResult<()> {
        self.request.validate()?;
        if &self.request.region != region {
            return Err(crate::native_record::corrupt_record());
        }
        let source =
            replication_provider::control::borrow_addition_source(&self.request.proposal, region)
                .map_err(|_| crate::native_record::corrupt_record())?;
        self.schema.validate(&source.name)?;
        let is_primary = self.request.proposal.operation().home_region == *region;
        if self.retained.is_some() != is_primary {
            return Err(crate::native_record::corrupt_record());
        }
        if let Some(definition) = &self.retained {
            definition.validate()?;
            let local = &definition
                .proposal()
                .participant(region)
                .map_err(|_| crate::native_record::corrupt_record())?
                .proposed;
            if local.physical != source.physical
                || local.configuration.table != source.configuration.table
                || local.configuration.tenant != source.configuration.tenant
                || local.name != source.name
                || local.configuration.lineage != source.configuration.lineage
                || definition.initial().proposal().operation().lineage
                    != source.configuration.lineage
                || definition.proposal().operation().id == self.request.proposal.operation().id
            {
                return Err(crate::native_record::corrupt_record());
            }
        }
        if let HistoryCapturePhase::Admitting { next, vector, .. } = self.phase {
            let inventory = self
                .retained
                .as_ref()
                .ok_or_else(crate::native_record::corrupt_record)?
                .retained_inventory()?
                .ok_or_else(crate::native_record::corrupt_record)?;
            inventory.validate()?;
            if next >= inventory.cuts || (next == 0 && vector != inventory.inventory_seed()?) {
                return Err(crate::native_record::corrupt_record());
            }
        }
        let next = match self.phase {
            HistoryCapturePhase::Holding(next)
            | HistoryCapturePhase::High(next)
            | HistoryCapturePhase::Releasing(next)
            | HistoryCapturePhase::Cancelling(next) => Some(next),
            _ => None,
        };
        if next.is_some_and(|next| next >= u32::from(partitions)) {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}
