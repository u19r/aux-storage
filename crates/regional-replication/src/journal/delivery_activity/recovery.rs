use std::num::NonZeroUsize;

use infrastructure_provider::{
    page::{BoundedRecordRange, RecordPageRequest},
    physical_attempt::PhysicalRecordAttempt,
    record::DurableRecord,
};
use storage_types::{ReadProtection, StorageResult};

use super::record::{DeliveryActivity, DeliveryCursor, DeliverySelection};
use crate::{
    constants::{DELIVERY_ACTIVITY_SUFFIX, DELIVERY_RECOVERY_SUFFIX},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn select_pending_delivery<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
    ) -> StorageResult<DeliverySelection> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let key = self.record_key(DELIVERY_RECOVERY_SUFFIX.to_vec())?;
            let rows = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&key),
                    ReadProtection::Conflict,
                )
                .await?;
            let mut cursor: DeliveryCursor = rows[0]
                .as_ref()
                .map(|row| self.records.decode(row))
                .transpose()?
                .unwrap_or_default();
            let row = self.load_delivery_activity(attempt, &cursor).await?;
            let wrapped = row.is_none();
            let scope = if let Some(row) = row {
                let activity: DeliveryActivity = self.records.decode(&row)?;
                if row.key != self.delivery_activity_key(&activity)? {
                    return Err(corrupt_record());
                }
                cursor.current = Some(row.key.clone());
                let previous = cursor.next_destination;
                let scope = self
                    .select_activity_destination(attempt, &activity, &mut cursor)
                    .await?;
                if previous == cursor.next_destination {
                    cursor.after = Some(row.key);
                    cursor.current = None;
                    cursor.next_destination = 0;
                }
                scope
            } else {
                cursor = DeliveryCursor::default();
                None
            };
            self.records
                .store(
                    attempt,
                    &[self.records.prepare(
                        key,
                        rows[0].as_ref().map(|row| row.revision),
                        &cursor,
                    )?],
                )
                .await?;
            Ok(DeliverySelection { scope, wrapped })
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn load_delivery_activity<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        cursor: &DeliveryCursor,
    ) -> StorageResult<Option<DurableRecord>> {
        let first = self.record_key(DELIVERY_ACTIVITY_SUFFIX.to_vec())?;
        let range = self.record_prefix_range(&first)?;
        cursor.require_range(&range)?;
        if let Some(current) = &cursor.current {
            let row = self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(current),
                    ReadProtection::Shared,
                )
                .await?
                .into_iter()
                .next()
                .flatten()
                .ok_or_else(corrupt_record)?;
            return Ok(Some(row));
        }
        let range = BoundedRecordRange::try_new(
            range.bounds().0.clone(),
            range.bounds().1.clone(),
            NonZeroUsize::MIN,
        )?;
        let page = self
            .records
            .scan(
                attempt,
                RecordPageRequest {
                    range,
                    after: cursor.after.clone(),
                    max_bytes: self.records.limits.max_bytes(),
                    protection: ReadProtection::Shared,
                },
            )
            .await?;
        Ok(page.records.into_iter().next())
    }
}
