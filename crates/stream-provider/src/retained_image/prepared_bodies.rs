//! One complete immutable body inventory owns its native storage path.

use infrastructure_provider::{
    batch::RecordBatch,
    limits::InfrastructureLimits,
    physical_attempt::PhysicalRecordAttempt,
    record::{ConditionalWriteResult, DurableRecord, RecordChange, RecordExpectation},
};
use storage_types::StorageResult;

use super::{
    error::{capacity_rejected, invalid_image},
    preparation::RetainedImageBodyWork,
};

pub(super) struct PreparedImageBodies {
    records: Vec<DurableRecord>,
    limits: InfrastructureLimits,
    pub(super) work: Vec<RetainedImageBodyWork>,
}

impl PreparedImageBodies {
    pub(super) fn new(
        records: Vec<DurableRecord>,
        work: Vec<RetainedImageBodyWork>,
        limits: InfrastructureLimits,
    ) -> Self {
        Self {
            records,
            work,
            limits,
        }
    }

    pub(super) fn select_work(&self, slots: &[usize]) -> StorageResult<Vec<RetainedImageBodyWork>> {
        let mut work = Vec::new();
        work.try_reserve(slots.len())
            .map_err(|_| capacity_rejected())?;
        for slot in slots {
            work.push(self.work.get(*slot).cloned().ok_or_else(invalid_image)?);
        }
        Ok(work)
    }

    pub(super) async fn store<A: PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let mut changes = Vec::new();
            changes
                .try_reserve(self.records.len())
                .map_err(|_| capacity_rejected())?;
            changes.extend(self.records.into_iter().map(|record| RecordChange::Store {
                record,
                expected: RecordExpectation::Absent,
            }));
            let [waves] = RecordBatch::plan_waves([changes.as_slice()], self.limits)?;
            for batch in waves {
                if !matches!(
                    attempt.store_records(&batch).await?,
                    ConditionalWriteResult::Applied
                ) {
                    return Err(storage_types::StorageEnum::TransactionConflict {
                        message: "Retained image body changed during the native attempt".into(),
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
