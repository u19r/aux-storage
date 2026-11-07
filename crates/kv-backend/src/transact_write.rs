//! One KV token key in the caller's protected transaction.

use storage_provider::{DynamoTransactWriteAttempt, maintenance::TransactReceiptRow};
use storage_types::{StorageError, StorageResult};

use crate::{
    KvAttempt, KvMutation, KvRange, ReadProtection, catalog::KvCatalogAttempt,
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

/// Ordered receipt page after `after`. Receipts live under one prefix, so the
/// range starts just past the last token already scanned.
pub(crate) async fn scan_receipts<A: KvAttempt>(
    attempt: &A,
    after: Option<&str>,
    limit: std::num::NonZeroU32,
) -> StorageResult<Vec<TransactReceiptRow>> {
    let mut begin = match after {
        Some(token) => receipt_key(token)?,
        None => TRANSACT_RECEIPT_PREFIX.to_vec(),
    };
    if after.is_some() {
        begin.push(0);
    }
    let mut end = TRANSACT_RECEIPT_PREFIX.to_vec();
    if let Some(last) = end.last_mut() {
        *last += 1;
    }
    let range = KvRange {
        begin,
        end,
        reverse: false,
        limit,
        max_bytes: storage_provider::TRANSACT_RECEIPT_READ_BYTES,
        entry_bounds: None,
    };
    let [page]: [_; 1] = attempt
        .scan_batch(vec![range], ReadProtection::Conflict)
        .await?
        .try_into()
        .map_err(|_| StorageError::internal("KV transact receipt scan page count differs"))?;
    page.entries
        .into_iter()
        .map(|entry| {
            let token = entry
                .key
                .strip_prefix(TRANSACT_RECEIPT_PREFIX)
                .and_then(|token| std::str::from_utf8(token).ok())
                .ok_or_else(|| StorageError::internal("KV transact receipt key is invalid"))?
                .to_owned();
            Ok(TransactReceiptRow {
                token,
                receipt: entry.value,
            })
        })
        .collect()
}

/// Re-read each key under conflict protection and delete only receipts whose
/// bytes still equal the scanned bytes.
pub(crate) async fn delete_receipts<A: KvAttempt>(
    attempt: &mut A,
    receipts: &[TransactReceiptRow],
) -> StorageResult<()> {
    let keys = receipts
        .iter()
        .map(|row| receipt_key(&row.token))
        .collect::<StorageResult<Vec<_>>>()?;
    let current = attempt
        .get_batch(keys.clone(), ReadProtection::Conflict)
        .await?;
    let changes = keys
        .into_iter()
        .zip(current)
        .zip(receipts)
        .filter(|((_, stored), row)| stored.as_deref() == Some(row.receipt.as_slice()))
        .map(|((key, _), _)| KvMutation::Delete { key })
        .collect::<Vec<_>>();
    if changes.is_empty() {
        return Ok(());
    }
    attempt.stage(changes)
}
