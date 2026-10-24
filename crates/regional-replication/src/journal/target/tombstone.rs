use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    bootstrap::HiddenTargetRequest, constants::TARGET_CANCELLATION_SUFFIX, journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) async fn load_target_tombstone<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
        protection: ReadProtection,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let key = self.identity_key(TARGET_CANCELLATION_SUFFIX, &request.operation.id)?;
            let rows = self.records.load(attempt, &[key], protection).await?;
            let Some(row) = &rows[0] else {
                return Ok(false);
            };
            let content: replication_types::protocol::CanonicalDigest = self.records.decode(row)?;
            if content != request.content()? {
                return Err(corrupt_record());
            }
            Ok(true)
        }
        .await;
        settlement.complete(result)
    }

    pub(super) async fn persist_target_tombstone<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        request: &HiddenTargetRequest,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if self
                .load_target_tombstone(attempt, request, ReadProtection::Conflict)
                .await?
            {
                return Ok(());
            }
            let key = self.identity_key(TARGET_CANCELLATION_SUFFIX, &request.operation.id)?;
            let change = self.records.prepare(key, None, &request.content()?)?;
            self.records.store(attempt, &[change]).await
        }
        .await;
        settlement.complete(result)
    }
}
