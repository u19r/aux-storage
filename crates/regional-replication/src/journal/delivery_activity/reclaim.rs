//! Remove discovery rows without stranding the shared delivery cursor.
use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, RecordChange},
};
use storage_types::{ReadProtection, StorageResult, TableHistoryId};

use super::record::{DeliveryActivity, DeliveryCursor};
use crate::{constants, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(in crate::journal) async fn prepare_table_delivery_activity_removals<
        A: PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
        history: TableHistoryId,
        rows: &[DurableRecord],
    ) -> StorageResult<Vec<RecordChange>> {
        for row in rows {
            let activity: DeliveryActivity = self.records.decode(row)?;
            if activity.physical != history || row.key != self.delivery_activity_key(&activity)? {
                return Err(corrupt_record());
            }
        }
        let key = self.record_key(constants::DELIVERY_RECOVERY_SUFFIX.to_vec())?;
        let Some(row) = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&key),
                ReadProtection::Conflict,
            )
            .await?
            .pop()
            .flatten()
        else {
            return Ok(Vec::new());
        };
        let mut cursor: DeliveryCursor = self.records.decode(&row)?;
        let first = self.record_key(constants::DELIVERY_ACTIVITY_SUFFIX.to_vec())?;
        cursor.require_range(&self.record_prefix_range(&first)?)?;
        if cursor
            .current
            .as_ref()
            .is_none_or(|key| !rows.iter().any(|row| row.key == *key))
        {
            return Ok(Vec::new());
        }
        cursor.after = cursor.current.take();
        cursor.next_destination = 0;
        Ok(vec![self.records.prepare(
            key,
            Some(row.revision),
            &cursor,
        )?])
    }
}
