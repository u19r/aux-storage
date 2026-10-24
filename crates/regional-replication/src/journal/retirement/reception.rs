use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::RecordChange};
use replication_types::protocol::CanonicalDigest;
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{ReceivedSeal, RetiredOrigin};
use crate::{
    constants::{RECEIVED_SEAL_SUFFIX, RETIRED_ORIGIN_SUFFIX},
    journal::JournalStore,
    lifecycle::{SealReceptionProgress, SealRequest, SealVectorPage},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn receive_sealed_table_cuts<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        page: &SealVectorPage,
    ) -> StorageResult<SealReceptionProgress> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let end = self.validate_received_seal_page(request, page)?;
            let start = u32::from(u16::from(page.cuts[0].origin.journal_partition));
            let key = self.identity_key(
                RECEIVED_SEAL_SUFFIX,
                &(request.proposal.operation().id, &request.region),
            )?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let mut record: ReceivedSeal = rows[0]
                .as_ref()
                .map(|row| self.records.decode(row))
                .transpose()?
                .unwrap_or(ReceivedSeal {
                    request: request.clone(),
                    vector: page.vector,
                    folded: request.content()?,
                    next: 0,
                });
            if record.request.content()? != request.content()?
                || record.vector != page.vector
                || start > record.next
                || (start < record.next && end > record.next)
            {
                return Err(corrupt_record());
            }
            let count = u32::from(request.source()?.partitions);
            if record.next > count || (record.next == count && record.folded != record.vector) {
                return Err(corrupt_record());
            }
            let replay = end <= record.next;
            if !replay {
                self.require_seal_reception_control(attempt, request)
                    .await?;
            }
            let mut changes = self
                .prepare_received_retired_origins(attempt, request, (&page.cuts, replay))
                .await?;
            if replay {
                return Ok(record.progress(count));
            }
            for cut in &page.cuts {
                record.folded =
                    CanonicalDigest::of_json(&(record.folded, &cut.origin, cut.through))
                        .map_err(|_| corrupt_record())?;
            }
            record.next = end;
            if end == count && record.folded != record.vector {
                return Err(corrupt_record());
            }
            changes.push(self.records.prepare(
                key,
                rows[0].as_ref().map(|row| row.revision),
                &record,
            )?);
            self.records.store(attempt, &changes).await?;
            Ok(record.progress(count))
        }
        .await;
        settlement.complete(result)
    }

    async fn require_seal_reception_control<A: PhysicalRecordAttempt + CatalogAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<()> {
        self.require_control_revision(
            attempt,
            &request.proposal,
            storage_types::ReadProtection::Shared,
        )
        .await?;
        let row = self
            .load_control(attempt, &request.proposal, ReadProtection::Shared)
            .await?
            .ok_or_else(corrupt_record)?;
        if self.decode_control(&row, &request.proposal)?.decision
            != crate::control::ControlDecision::Prepared
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    async fn prepare_received_retired_origins<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
        input: (&[crate::lifecycle::SealedOriginCut], bool),
    ) -> StorageResult<Vec<RecordChange>> {
        let (cuts, replay) = input;
        let local = request
            .proposal
            .participant(&self.region)
            .map_err(|_| corrupt_record())?
            .proposed
            .configuration
            .table;
        let before = request.before()?;
        let content = request.content()?;
        let keys = cuts
            .iter()
            .map(|cut| {
                self.identity_key(
                    RETIRED_ORIGIN_SUFFIX,
                    &(
                        local,
                        before.configuration.table,
                        before.configuration.lineage,
                        &cut.origin,
                    ),
                )
            })
            .collect::<StorageResult<Vec<_>>>()?;
        let rows = self
            .records
            .load(attempt, &keys, ReadProtection::Conflict)
            .await?;
        let mut pending = Vec::with_capacity(cuts.len());
        for ((cut, key), row) in cuts.iter().zip(keys).zip(rows) {
            if let Some(row) = &row {
                let old: RetiredOrigin = self.records.decode(row)?;
                if old.is_seal_cut(request, cut, content) {
                    continue;
                }
                if replay {
                    return Err(corrupt_record());
                }
                self.require_cancelled_retired_origin(attempt, &old).await?;
            }
            if replay {
                return Err(corrupt_record());
            }
            pending.push((cut, key, row.as_ref().map(|row| row.revision)));
        }
        let origins = pending
            .iter()
            .map(|(cut, _, _)| cut.origin.clone())
            .collect::<Vec<_>>();
        let coverages = self
            .load_received_coverages(
                attempt,
                (
                    local,
                    before.configuration.table,
                    before.configuration.lineage,
                ),
                &origins,
                ReadProtection::Shared,
            )
            .await?;
        let mut changes = Vec::with_capacity(pending.len());
        for ((cut, key, revision), coverage) in pending.into_iter().zip(coverages) {
            if coverage.through() < cut.through {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into());
            }
            let retired = RetiredOrigin {
                operation: request.proposal.operation().id,
                region: request.region.clone(),
                content,
                cut: cut.clone(),
            };
            changes.push(self.records.prepare(key, revision, &retired)?);
        }
        Ok(changes)
    }
}
