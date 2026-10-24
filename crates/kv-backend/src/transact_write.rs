//! One KV token key in the caller's protected transaction.

use storage_provider::DynamoTransactWriteAttempt;
use storage_types::StorageResult;

use crate::{
    KvAttempt, KvMutation, ReadProtection, catalog::KvCatalogAttempt,
    constants::TRANSACT_RECEIPT_PREFIX,
};

impl<A: KvAttempt> DynamoTransactWriteAttempt for KvCatalogAttempt<'_, A> {
    async fn read_scheduled_jobs(
        &mut self,
        keys: &[scheduled_bg_jobs::store::StoreKey],
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = settlement.as_mut();
        let result = async {
            let physical = crate::job_store::data_keys(
                keys.iter()
                    .map(scheduled_bg_jobs::store::StoreKey::as_bytes),
            )?;
            catalog
                .attempt
                .get_batch_admitted(
                    physical,
                    ReadProtection::Conflict,
                    crate::job_store::point_allowance(keys.len())?,
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn read_transact_receipt(&mut self, token: &str) -> StorageResult<Option<Vec<u8>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let result = async {
            let maximum = storage_provider::TRANSACT_RECEIPT_READ_BYTES.get();
            settlement
                .as_mut()
                .attempt
                .get_admitted(
                    &receipt_key(token)?,
                    ReadProtection::Conflict,
                    crate::KvValueAllowance::new(maximum, maximum),
                )
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn write_transact_receipt(&mut self, token: &str, receipt: Vec<u8>) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let result = (|| {
            let mut changes = Vec::new();
            changes
                .try_reserve_exact(1)
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes.push(KvMutation::Put {
                key: receipt_key(token)?,
                value: receipt,
            });
            settlement.as_mut().attempt.stage(changes)
        })();
        settlement.complete(result)
    }
}

fn receipt_key(token: &str) -> StorageResult<Vec<u8>> {
    let count = TRANSACT_RECEIPT_PREFIX
        .len()
        .checked_add(token.len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut key = Vec::new();
    key.try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    key.extend_from_slice(TRANSACT_RECEIPT_PREFIX);
    key.extend_from_slice(token.as_bytes());
    Ok(key)
}
