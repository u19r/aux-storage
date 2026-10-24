//! One protected page per turn proves frozen custody disjoint before creating a
//! hold.
use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{identity::resource::ControlOperationId, protocol::CanonicalDigest};
use storage_types::{ReadProtection, StorageResult};

use super::record::{HistoryCapturePhase, HistoryCaptureRecord};
use crate::{
    history::source_retention::SourceRetentionRange,
    journal::{JournalStore, trim_reservation::ReservationAdmission},
    native_record::corrupt_record,
};

/// Issued and consumed inside one native admission turn; never stored or
/// deserialized.
pub(super) struct AdoptionAdmission {
    operation: ControlOperationId,
    inventory: Option<CanonicalDigest>,
}

impl AdoptionAdmission {
    pub(super) fn require_capture(self, record: &HistoryCaptureRecord) -> StorageResult<()> {
        let inventory = record
            .retained
            .as_ref()
            .ok_or_else(corrupt_record)?
            .retained_inventory()?;
        let content = inventory
            .as_ref()
            .map(CanonicalDigest::of_json)
            .transpose()
            .map_err(|_| corrupt_record())?;
        if record.request.proposal.operation().id != self.operation
            || content != self.inventory
            || !matches!(record.phase, HistoryCapturePhase::Holding(0))
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl JournalStore {
    pub(super) async fn begin_adoption_admission<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut HistoryCaptureRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(definition) = &record.retained else {
                return Ok(());
            };
            if let Some(inventory) = definition.retained_inventory()? {
                inventory.validate()?;
                let (barrier, _) = self
                    .load_reservation_barrier(attempt, ReadProtection::Conflict)
                    .await?
                    .reservation_state();
                record.phase = HistoryCapturePhase::Admitting {
                    barrier,
                    next: 0,
                    vector: inventory.inventory_seed()?,
                };
                return Ok(());
            }
            self.require_reservation_admission(attempt, ReservationAdmission::Sources(&[]))
                .await?;
            let admission = AdoptionAdmission {
                operation: record.request.proposal.operation().id,
                inventory: None,
            };
            self.retain_adoption_inventory(attempt, record, admission)
                .await
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn advance_adoption_admission<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut HistoryCaptureRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let HistoryCapturePhase::Admitting {
                barrier,
                next,
                vector,
            } = record.phase
            else {
                return Err(corrupt_record());
            };
            let inventory = record
                .retained
                .as_ref()
                .ok_or_else(corrupt_record)?
                .retained_inventory()?
                .ok_or_else(corrupt_record)?;
            let (current, count) = self
                .load_reservation_barrier(attempt, ReadProtection::Conflict)
                .await?
                .reservation_state();
            if barrier > current {
                return Err(corrupt_record());
            }
            if current != barrier {
                record.phase = HistoryCapturePhase::Admitting {
                    barrier: current,
                    next: 0,
                    vector: inventory.inventory_seed()?,
                };
                return Ok(());
            }
            let custody = crate::bootstrap::ReenableHistoryCustody {
                definition: inventory.clone(),
                ordinal: next,
            };
            let page = self
                .load_retained_custody_page(
                    attempt,
                    &record.request.proposal,
                    &custody,
                    ReadProtection::Shared,
                )
                .await?;
            let vector = inventory.require_page(&page, (next, vector))?;
            let overlap =
                self.load_trim_reservation_inventory(attempt, count, ReadProtection::Conflict)
                    .await?
                    .is_some_and(|reservation| {
                        reservation.proposal.candidate.sources.iter().any(|cut| {
                            SourceRetentionRange::Source(&page.cut.cut).overlaps_cut(cut)
                        })
                    });
            if overlap {
                return Err(storage_types::StorageError::unsupported(
                    "History custody conflicts with a live trim reservation",
                ));
            }
            if let Some(next) = page.next_ordinal {
                record.phase = HistoryCapturePhase::Admitting {
                    barrier,
                    next,
                    vector,
                };
                return Ok(());
            }
            let admission = AdoptionAdmission {
                operation: record.request.proposal.operation().id,
                inventory: Some(
                    CanonicalDigest::of_json(&inventory).map_err(|_| corrupt_record())?,
                ),
            };
            record.phase = HistoryCapturePhase::Holding(0);
            self.retain_adoption_inventory(attempt, record, admission)
                .await
        }
        .await;
        settlement.complete(result)
    }
}
