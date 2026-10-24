//! Mutable descriptor reads qualify exact receipt content before publication.

use storage_types::StorageResult;

use super::{RetainedOwnerRecord, custody_record::CustodyRecordCodec, error::invalid_image};

impl RetainedOwnerRecord {
    pub async fn require_current_batch<A: super::RetainedOwnershipAttempt>(
        attempt: &mut A,
        receipts: &[Self],
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let protection = attempt.read_protection().shared();
            for wave in receipts.chunks(crate::constants::IMAGE_OWNER_READ_RECORDS) {
                let limits = Self::receipt_limits(wave.iter())?;
                let keys = wave
                    .iter()
                    .map(Self::key)
                    .collect::<StorageResult<Vec<_>>>()?;
                let limits = limits.project_read_keys(&keys)?;
                let rows = attempt.load_records(&keys, protection, limits).await?;
                limits.validate_observations(&keys, &rows)?;
                for (receipt, row) in wave.iter().zip(rows) {
                    let row = row.ok_or_else(invalid_image)?;
                    if !receipt.kind.is_mutable()
                        && row.revision != storage_types::Revision::INITIAL
                    {
                        return Err(invalid_image());
                    }
                    let observed: Self =
                        CustodyRecordCodec::decode(&row.bytes, limits.max_record_bytes().get())?;
                    if observed != *receipt {
                        return Err(invalid_image());
                    }
                }
                Self::require_graph_wave(attempt, wave).await?;
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
