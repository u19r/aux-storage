use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::ReceivedSeal;
use crate::{
    constants::RECEIVED_SEAL_SUFFIX,
    journal::JournalStore,
    lifecycle::{SealReceptionProgress, SealRequest, SealVectorPage},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(super) fn require_seal_survivor(&self, request: &SealRequest) -> StorageResult<()> {
        request.validate()?;
        if !request.survivors()?.contains(&self.region) {
            return Err(corrupt_record());
        }
        self.require_local_source(
            &request
                .proposal
                .participant(&self.region)
                .map_err(|_| corrupt_record())?
                .proposed,
        )
    }

    pub(super) fn validate_received_seal_page(
        &self,
        request: &SealRequest,
        page: &SealVectorPage,
    ) -> StorageResult<u32> {
        self.require_seal_survivor(request)?;
        if page.content != request.content()? || page.cuts.is_empty() {
            return Err(corrupt_record());
        }
        let start = u32::from(u16::from(page.cuts[0].origin.journal_partition));
        let end = start
            .checked_add(u32::try_from(page.cuts.len()).map_err(|_| corrupt_record())?)
            .ok_or_else(corrupt_record)?;
        if end > u32::from(request.source()?.partitions)
            || page.next_partition != (end < u32::from(request.source()?.partitions)).then_some(end)
        {
            return Err(corrupt_record());
        }
        let survivors = request.survivors()?;
        for (index, cut) in page.cuts.iter().enumerate() {
            request.require_history_cut(
                (&cut.origin, cut.through, &cut.receipts),
                start + index as u32,
                survivors,
            )?;
        }
        Ok(end)
    }

    pub(crate) async fn observe_received_seal<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &SealRequest,
    ) -> StorageResult<SealReceptionProgress> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            self.require_seal_survivor(request)?;
            let key = self.identity_key(
                RECEIVED_SEAL_SUFFIX,
                &(request.proposal.operation().id, &request.region),
            )?;
            let rows = self
                .records
                .load(attempt, &[key], ReadProtection::Snapshot)
                .await?;
            let Some(row) = &rows[0] else {
                return Ok(SealReceptionProgress {
                    next_partition: 0,
                    complete: false,
                });
            };
            let record: ReceivedSeal = self.records.decode(row)?;
            if record.request.content()? != request.content()?
                || record.next > u32::from(request.source()?.partitions)
                || (record.next == u32::from(request.source()?.partitions)
                    && record.folded != record.vector)
            {
                return Err(corrupt_record());
            }
            Ok(record.progress(u32::from(request.source()?.partitions)))
        }
        .await;
        settlement.complete(result)
    }

    pub(in crate::journal) async fn require_received_removal_seals<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &replication_types::configuration::proposal::ControlProposal,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if !matches!(
                proposal.operation().change,
                replication_types::configuration::control::ConfigurationChange::RemoveRegion { .. }
                    | replication_types::configuration::control::ConfigurationChange::DisableGlobal
            ) {
                return Ok(());
            }
            for region in replication_provider::control::retiring_regions(proposal)
                .map_err(|_| corrupt_record())?
            {
                let request = SealRequest::try_new(proposal.clone(), region)?;
                if request.survivors()?.contains(&self.region)
                    && !self
                        .observe_received_seal(attempt, &request)
                        .await?
                        .complete
                {
                    return Err(storage_types::InfrastructureResultKind::Unavailable {
                        context: storage_types::InfrastructureContext::RegionalSource,
                    }
                    .into());
                }
            }
            Ok(())
        }
        .await;
        settlement.complete(result)
    }
}
