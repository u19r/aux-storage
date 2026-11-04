//! Operation IDs are recovery lookup keys; response owners govern erasure.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use storage_types::{StorageResult, TableHistoryId};

use super::JournalStore;
use crate::{
    constants::OPERATION_SUFFIX, native_record::corrupt_record, origin::OriginOperationReceipt,
};

impl JournalStore {
    pub(super) async fn reclaim_operation_receipts<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        after: Option<DurableRecordKey>,
    ) -> StorageResult<Option<DurableRecordKey>> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let page = self
                .scan_reclaim_family(operation.as_mut(), OPERATION_SUFFIX, after)
                .await?;
            let changes = page
                .records
                .iter()
                .map(|row| self.prepare_response_removal(row, history))
                .collect::<StorageResult<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            self.records.store(operation.as_mut(), &changes).await?;
            Ok(page.continuation)
        }
        .await;
        operation.complete(result)
    }

    fn prepare_response_removal(
        &self,
        row: &DurableRecord,
        history: TableHistoryId,
    ) -> StorageResult<Option<RecordChange>> {
        let receipt: OriginOperationReceipt<&serde_json::value::RawValue> =
            self.records.decode(row)?;
        if row.key != self.identity_key(OPERATION_SUFFIX, &receipt.operation)? {
            return Err(corrupt_record());
        }
        if !receipt.owners.iter().any(|owner| owner.physical == history) {
            return Ok(None);
        }
        if receipt.owners.iter().all(|owner| owner.physical == history) {
            return Ok(Some(RecordChange::Delete {
                key: row.key.clone(),
                expected: row.revision,
            }));
        }
        let remaining = receipt.remove_table_data(history)?;
        self.records
            .prepare(row.key.clone(), Some(row.revision), &remaining)
            .map(Some)
    }
}
