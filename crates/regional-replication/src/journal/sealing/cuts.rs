use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecordKey};
use replication_types::{identity::origin::OriginId, protocol::CanonicalDigest};
use storage_types::{ReadProtection, Revision, StorageResult};

use super::record::{SealCut, SealPhase, SealRecord};
use crate::{
    constants::TABLE_SEAL_CUT_SUFFIX, journal::JournalStore, lifecycle::SealRequest,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn table_seal_cut_key(
        &self,
        request: &SealRequest,
        origin: &OriginId,
    ) -> StorageResult<DurableRecordKey> {
        self.identity_key(
            TABLE_SEAL_CUT_SUFFIX,
            &(request.proposal.operation().id, origin),
        )
    }

    pub(super) async fn record_table_seal_cuts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &mut SealRecord,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let SealPhase::RecordingCuts(next) = record.phase else {
                return Err(corrupt_record());
            };
            let count = u32::from(self.partitions);
            if next >= count {
                return Err(corrupt_record());
            }
            let width = u32::try_from(
                self.records
                    .limits
                    .max_records()
                    .get()
                    .saturating_sub(1)
                    .saturating_div(2)
                    .clamp(1, crate::constants::HISTORY_CUT_PAGE_WIDTH),
            )
            .map_err(|_| corrupt_record())?;
            let end = next.saturating_add(width).min(count);
            let partitions = (next..end)
                .map(|partition| {
                    u16::try_from(partition)
                        .map(Into::into)
                        .map_err(|_| corrupt_record())
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let heads = self
                .load_source_heads(attempt, &partitions, ReadProtection::Conflict)
                .await?;
            let before = record.request.before()?;
            let table = (before.configuration.table, before.configuration.lineage);
            let activities = partitions
                .iter()
                .copied()
                .zip(heads.iter().copied())
                .map(|high| (table, high));
            let mut changes = self
                .prepare_local_delivery_activities(attempt, activities)
                .await?;
            for (partition, through) in partitions.into_iter().zip(heads) {
                let cut = SealCut {
                    content: record.content,
                    origin: self.origin(partition)?,
                    through,
                    drained: false,
                    receipts: Vec::new(),
                };
                let key = self.table_seal_cut_key(&record.request, &cut.origin)?;
                record.vector = CanonicalDigest::of_json(&(record.vector, &cut.origin, through))
                    .map_err(|_| corrupt_record())?;
                changes.push(self.records.prepare(key, None, &cut)?);
            }
            self.records.store(attempt, &changes).await?;
            record.phase = if end == count {
                SealPhase::Draining(0)
            } else {
                SealPhase::RecordingCuts(end)
            };
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn load_table_seal_cut<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        partition: u32,
    ) -> StorageResult<(infrastructure_provider::record::DurableRecord, SealCut)> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let origin = self.origin(
                u16::try_from(partition)
                    .map_err(|_| corrupt_record())?
                    .into(),
            )?;
            let key = self.table_seal_cut_key(request, &origin)?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Conflict)
                .await?;
            let row = rows
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(corrupt_record)?;
            let cut = self.decode_table_seal_cut(&row, request, &origin)?;
            Ok((row, cut))
        }
        .await;
        settlement.complete(result)
    }

    pub(super) fn decode_table_seal_cut(
        &self,
        row: &infrastructure_provider::record::DurableRecord,
        request: &SealRequest,
        origin: &OriginId,
    ) -> StorageResult<SealCut> {
        let cut: SealCut = self.records.decode(row)?;
        let revision = if cut.drained {
            Revision::INITIAL.checked_next()?
        } else {
            Revision::INITIAL
        };
        if row.key != self.table_seal_cut_key(request, origin)?
            || row.revision != revision
            || cut.origin != *origin
            || cut.content != request.content()?
        {
            return Err(corrupt_record());
        }
        if !cut.drained {
            if !cut.receipts.is_empty() {
                return Err(corrupt_record());
            }
            return Ok(cut);
        }
        request.require_history_cut(
            (&cut.origin, cut.through, &cut.receipts),
            u32::from(u16::from(origin.journal_partition)),
            request.survivors()?,
        )?;
        Ok(cut)
    }
}
