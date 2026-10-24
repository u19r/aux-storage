use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::SealPhase;
use crate::{
    journal::JournalStore,
    lifecycle::{SealRequest, SealVectorPage, SealedOriginCut},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_sealed_table_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        after: u32,
    ) -> StorageResult<SealVectorPage> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (_, record) = self
                .load_table_seal(attempt, request, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                SealPhase::Ready | SealPhase::Removed | SealPhase::Retired
            ) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into());
            }
            let count = u32::from(self.partitions);
            if after >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .saturating_sub(1)
                    .clamp(1, crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            let end = after.saturating_add(width).min(count);
            let origins = (after..end)
                .map(|partition| {
                    self.origin(
                        u16::try_from(partition)
                            .map_err(|_| corrupt_record())?
                            .into(),
                    )
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let keys = origins
                .iter()
                .map(|origin| self.table_seal_cut_key(request, origin))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Snapshot)
                .await?;
            let mut cuts = Vec::with_capacity(rows.len());
            for (row, origin) in rows.into_iter().zip(origins) {
                let cut = self.decode_table_seal_cut(
                    row.as_ref().ok_or_else(corrupt_record)?,
                    request,
                    &origin,
                )?;
                if !cut.drained {
                    return Err(corrupt_record());
                }
                cuts.push(SealedOriginCut {
                    origin,
                    through: cut.through,
                    receipts: cut.receipts,
                });
            }
            Ok(SealVectorPage {
                content: record.content,
                vector: record.vector,
                cuts,
                next_partition: (end < count).then_some(end),
            })
        }
        .await;
        settlement.complete(result)
    }
}
