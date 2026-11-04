mod begin;
mod cancel;
mod completion;
mod cuts;
mod record;
mod scan;

use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use record::{CapturePhase, CaptureRecord};
use replication_types::configuration::proposal::ControlProposal;
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    bootstrap::SourceCaptureStatus, constants::SOURCE_CAPTURE_SUFFIX, native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn load_source_capture_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<crate::bootstrap::SourceCaptureDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_source_capture(attempt, proposal, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                CapturePhase::BaselineReady | CapturePhase::Releasing(_) | CapturePhase::Released
            ) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::PublicationHistory,
                }
                .into());
            }
            crate::bootstrap::SourceCaptureDefinition::from_ready(
                record.proposal,
                record.schema,
                record.vector,
            )
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn observe_source_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
    ) -> StorageResult<Option<SourceCaptureStatus>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            Ok(self
                .load_source_capture(attempt, proposal, ReadProtection::Snapshot)
                .await?
                .map(|(_, record)| status(&record.phase)))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_source_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proposal: &ControlProposal,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, CaptureRecord)>> {
        proposal.validate().map_err(|_| corrupt_record())?;
        if proposal.operation().home_region != self.region {
            return Err(corrupt_record());
        }
        let key = self.identity_key(SOURCE_CAPTURE_SUFFIX, &proposal.operation().id)?;
        let mut rows = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?;
        let Some(row) = rows.pop().flatten() else {
            return Ok(None);
        };
        let record = self.decode_source_capture(&row)?;
        if record.proposal != *proposal {
            return Err(corrupt_record());
        }
        Ok(Some((row, record)))
    }

    fn decode_source_capture(&self, row: &DurableRecord) -> StorageResult<CaptureRecord> {
        let record: CaptureRecord = self.records.decode(row)?;
        record.proposal.validate().map_err(|_| corrupt_record())?;
        if record.proposal.operation().home_region != self.region
            || row.key
                != self.identity_key(SOURCE_CAPTURE_SUFFIX, &record.proposal.operation().id)?
        {
            return Err(corrupt_record());
        }
        let source = replication_provider::control::source_capture_state(&record.proposal)
            .map_err(|_| corrupt_record())?;
        self.require_local_source(&source)?;
        record.schema.validate(&source.name)?;
        record.require_partition(self.partitions)?;
        Ok(record)
    }

    pub(super) async fn reclaim_source_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        row: &DurableRecord,
        history: storage_types::TableHistoryId,
    ) -> StorageResult<()> {
        let record = self.decode_source_capture(row)?;
        let source = replication_provider::control::source_capture_state(&record.proposal)
            .map_err(|_| corrupt_record())?;
        if source.physical != history {
            return Ok(());
        }
        self.records
            .store(
                attempt,
                &[infrastructure_provider::record::RecordChange::Delete {
                    key: row.key.clone(),
                    expected: row.revision,
                }],
            )
            .await
    }

    async fn store_source_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &CaptureRecord,
        revision: Option<storage_types::Revision>,
    ) -> StorageResult<SourceCaptureStatus> {
        record.require_partition(self.partitions)?;
        let key = self.identity_key(SOURCE_CAPTURE_SUFFIX, &record.proposal.operation().id)?;
        let change = self.records.prepare(key, revision, record)?;
        self.records.store(attempt, &[change]).await?;
        Ok(status(&record.phase))
    }
}

fn status(phase: &CapturePhase) -> SourceCaptureStatus {
    match phase {
        CapturePhase::Holding(_) => SourceCaptureStatus::Holding,
        CapturePhase::Live(_) | CapturePhase::Deleted(_) => SourceCaptureStatus::Scanning,
        CapturePhase::High(_) => SourceCaptureStatus::CapturingHigh,
        CapturePhase::History => SourceCaptureStatus::RetainingHistory,
        CapturePhase::BaselineReady => SourceCaptureStatus::BaselineReady,
        CapturePhase::Releasing(_) => SourceCaptureStatus::Releasing,
        CapturePhase::Released => SourceCaptureStatus::Released,
        CapturePhase::Cancelling(_) => SourceCaptureStatus::Cancelling,
        CapturePhase::Cancelled => SourceCaptureStatus::Cancelled,
    }
}

mod trim_authority;
