mod admission;
mod completion;
mod cuts;
mod preparation;
mod record;
mod retained;

use infrastructure_provider::{physical_attempt::PhysicalRecordAttempt, record::DurableRecord};
use record::{HistoryCaptureRecord, capture_status};
use storage_types::{ReadProtection, StorageResult};

use super::JournalStore;
use crate::{
    constants::HISTORY_CAPTURE_SUFFIX,
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn require_ready_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<crate::history_capture::HistoryCaptureDefinition> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (_, record) = self
                .load_history_capture(attempt, request, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(record.phase, record::HistoryCapturePhase::Ready) {
                return Err(corrupt_record());
            }
            self.require_history_slot(attempt, request, protection)
                .await?;
            crate::history_capture::HistoryCaptureDefinition::from_ready(
                record.request,
                record.schema,
                record.vector,
            )
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn observe_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<Option<HistoryCaptureStatus>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            Ok(self
                .load_history_capture(attempt, request, ReadProtection::Snapshot)
                .await?
                .map(|(_, record)| capture_status(record.phase)))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn load_history_capture_definition<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<crate::history_capture::HistoryCaptureDefinition> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let (_, record) = self
                .load_history_capture(attempt, request, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            if !matches!(
                record.phase,
                record::HistoryCapturePhase::Ready
                    | record::HistoryCapturePhase::Releasing(_)
                    | record::HistoryCapturePhase::Released
            ) {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::PublicationHistory,
                }
                .into());
            }
            crate::history_capture::HistoryCaptureDefinition::from_ready(
                record.request,
                record.schema,
                record.vector,
            )
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
        protection: ReadProtection,
    ) -> StorageResult<Option<(DurableRecord, HistoryCaptureRecord)>> {
        request.validate()?;
        if request.region != self.region {
            return Err(corrupt_record());
        }
        let key = self.identity_key(HISTORY_CAPTURE_SUFFIX, &request.proposal.operation().id)?;
        let mut rows = self
            .records
            .load(attempt, std::slice::from_ref(&key), protection)
            .await?;
        let Some(row) = rows.pop().flatten() else {
            return Ok(None);
        };
        let record: HistoryCaptureRecord = self.records.decode(&row)?;
        if row.key != key || record.request.content()? != request.content()? {
            return Err(corrupt_record());
        }
        record.require_local(&self.region, self.partitions)?;
        self.require_adoption_inventory(attempt, &record, protection)
            .await?;
        Ok(Some((row, record)))
    }

    async fn store_history_capture<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &HistoryCaptureRecord,
        revision: Option<storage_types::Revision>,
    ) -> StorageResult<HistoryCaptureStatus> {
        record.require_local(&self.region, self.partitions)?;
        let key = self.identity_key(
            HISTORY_CAPTURE_SUFFIX,
            &record.request.proposal.operation().id,
        )?;
        self.records
            .store(attempt, &[self.records.prepare(key, revision, record)?])
            .await?;
        Ok(capture_status(record.phase))
    }
}

mod baseline;

mod baseline_authority;
mod grant;
