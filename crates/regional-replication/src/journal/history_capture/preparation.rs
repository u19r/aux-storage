use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use super::record::{HistoryCapturePhase, HistoryCaptureRecord, capture_status};
use crate::{
    history_capture::{HistoryCaptureRequest, HistoryCaptureStatus},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn advance_history_capture<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<HistoryCaptureStatus> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some((row, mut record)) = self
                .load_history_capture(attempt, request, ReadProtection::Conflict)
                .await?
            else {
                return self.begin_history_capture(attempt, request).await;
            };
            if matches!(
                record.phase,
                HistoryCapturePhase::Ready
                    | HistoryCapturePhase::Released
                    | HistoryCapturePhase::Cancelled
            ) {
                return Ok(capture_status(record.phase));
            }
            if matches!(
                record.phase,
                HistoryCapturePhase::Releasing(_) | HistoryCapturePhase::Cancelling(_)
            ) {
                return self
                    .advance_history_cleanup(attempt, (row.revision, record))
                    .await;
            }
            self.require_control_revision(
                attempt,
                &request.proposal,
                storage_types::ReadProtection::Shared,
            )
            .await?;
            self.require_history_slot(attempt, request, storage_types::ReadProtection::Conflict)
                .await?;
            if matches!(record.phase, HistoryCapturePhase::Admitting { .. }) {
                self.advance_adoption_admission(attempt, &mut record)
                    .await?;
            } else {
                self.advance_history_cuts(attempt, &mut record).await?;
            }
            self.store_history_capture(attempt, &record, Some(row.revision))
                .await
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn begin_history_capture<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        request: &HistoryCaptureRequest,
    ) -> StorageResult<HistoryCaptureStatus> {
        request.validate()?;
        if request.region != self.region {
            return Err(corrupt_record());
        }
        let source =
            replication_provider::control::borrow_addition_source(&request.proposal, &self.region)
                .map_err(|_| corrupt_record())?;
        self.require_local_source(source)?;
        if self
            .load_control(attempt, &request.proposal, ReadProtection::Conflict)
            .await?
            .is_some()
        {
            return Err(corrupt_record());
        }
        let capture = self.identity_key(
            crate::constants::SOURCE_CAPTURE_SUFFIX,
            &request.proposal.operation().id,
        )?;
        if self
            .records
            .load(attempt, &[capture], ReadProtection::Conflict)
            .await?[0]
            .is_some()
        {
            return Err(corrupt_record());
        }
        self.require_control_revision(
            attempt,
            &request.proposal,
            storage_types::ReadProtection::Shared,
        )
        .await?;
        let slot = self.reserve_control(attempt, &request.proposal).await?;
        let retained = if request.proposal.operation().home_region == self.region {
            Some(
                self.load_lineage_bootstrap_definition(
                    attempt,
                    source,
                    storage_types::ReadProtection::Shared,
                )
                .await?,
            )
        } else {
            None
        };
        let schema = self.freeze_capture_schema(attempt, source).await?;
        let digest = replication_types::protocol::CanonicalDigest::of_json(&schema)
            .map_err(|_| corrupt_record())?;
        let vector =
            replication_types::protocol::CanonicalDigest::of_json(&(request.content()?, digest))
                .map_err(|_| corrupt_record())?;
        let mut record = HistoryCaptureRecord {
            request: request.clone(),
            schema,
            vector,
            phase: HistoryCapturePhase::Holding(0),
            retained,
        };
        self.begin_adoption_admission(attempt, &mut record).await?;
        self.records.store(attempt, &[slot]).await?;
        self.store_history_capture(attempt, &record, None).await
    }
}
