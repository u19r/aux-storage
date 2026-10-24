use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{SealPhase, SealRecord};
use crate::{delivery::DeliveryScope, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn advance_table_seal_drain<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut SealRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let SealPhase::Draining(next) = record.phase else {
                return Err(corrupt_record());
            };
            let (row, mut cut) = self
                .load_table_seal_cut(attempt, &record.request, next)
                .await?;
            if cut.drained {
                return Err(corrupt_record());
            }
            let before = record.request.before()?;
            let mut receipts = Vec::new();
            for survivor in record.request.survivors()? {
                let destination = before
                    .sources
                    .iter()
                    .find(|source| &source.region == survivor)
                    .ok_or_else(corrupt_record)?
                    .clone();
                let scope = DeliveryScope {
                    origin: cut.origin.clone(),
                    table: before.configuration.table,
                    local_table: before.configuration.table,
                    lineage: before.configuration.lineage,
                    membership: before.membership,
                    destination,
                };
                let (coverage, _) = self
                    .observe_delivery_progress(attempt, &scope, ReadProtection::Shared)
                    .await?;
                if coverage.through() < cut.through {
                    return Ok(());
                }
                receipts.push((survivor.clone(), coverage.through()));
            }
            cut.drained = true;
            cut.receipts = receipts;
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(row.key, Some(row.revision), &cut)?],
                )
                .await?;
            let next = next.checked_add(1).ok_or_else(corrupt_record)?;
            record.phase = if next == u32::from(self.partitions) {
                SealPhase::Ready
            } else {
                SealPhase::Draining(next)
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
