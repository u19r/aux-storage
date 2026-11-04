use infrastructure_provider::{
    physical_attempt::PhysicalRecordAttempt,
    record::{DurableRecord, DurableRecordKey, RecordChange},
};
use replication_types::identity::origin::OriginPosition;
use storage_types::{ReadProtection, StorageResult};

use super::record::{DeliveryActivity, DeliveryActivityObservation};
use crate::{
    constants::DELIVERY_ACTIVITY_SUFFIX, journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(in crate::journal) fn delivery_activity_key(
        &self,
        activity: &DeliveryActivity,
    ) -> StorageResult<DurableRecordKey> {
        self.table_identity_key(
            DELIVERY_ACTIVITY_SUFFIX,
            activity.physical,
            &(
                activity.table,
                activity.source_table,
                activity.lineage,
                &activity.origin,
            ),
        )
    }

    pub(in crate::journal) fn observe_delivery_activity(
        &self,
        expected: DeliveryActivity,
        row: Option<&DurableRecord>,
    ) -> StorageResult<DeliveryActivityObservation> {
        let key = self.delivery_activity_key(&expected)?;
        let activity = match row {
            Some(row) => {
                let activity: DeliveryActivity = self.records.decode(row)?;
                if row.key != key
                    || activity.physical != expected.physical
                    || activity.table != expected.table
                    || activity.lineage != expected.lineage
                    || activity.source_table != expected.source_table
                    || activity.origin != expected.origin
                {
                    return Err(corrupt_record());
                }
                activity
            }
            None => expected,
        };
        Ok(DeliveryActivityObservation {
            key,
            revision: row.map(|row| row.revision),
            activity,
        })
    }

    pub(in crate::journal) fn prepare_delivery_activity(
        &self,
        mut observed: DeliveryActivityObservation,
        through: OriginPosition,
    ) -> StorageResult<Option<RecordChange>> {
        if observed.revision.is_some() && observed.activity.through >= through {
            return Ok(None);
        }
        observed.activity.through = through;
        self.records
            .prepare(observed.key, observed.revision, &observed.activity)
            .map(Some)
    }

    /// Prepare monotone discovery markers against the original protected rows.
    /// The caller publishes the complete inventory with its source or cut
    /// records.
    pub(in crate::journal) fn prepare_local_delivery_activities<
        'attempt,
        A: PhysicalRecordAttempt,
    >(
        &'attempt self,
        attempt: &'attempt mut A,
        activities: impl Iterator<Item = DeliveryActivity>,
    ) -> impl Future<Output = StorageResult<Vec<RecordChange>>> + Send + 'attempt {
        let inventory = self.prepare_local_delivery_inventory(activities);
        self.prepare_local_delivery_changes(attempt, inventory)
    }

    async fn prepare_local_delivery_changes<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        inventory: StorageResult<std::collections::BTreeMap<DurableRecordKey, DeliveryActivity>>,
    ) -> StorageResult<Vec<RecordChange>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let inventory = inventory?;
            let keys = inventory.keys().cloned().collect::<Vec<_>>();
            let rows = self
                .records
                .load(attempt, &keys, ReadProtection::Conflict)
                .await?;
            let mut changes = Vec::with_capacity(keys.len());
            for (activity, row) in inventory.into_values().zip(rows) {
                let through = activity.through;
                let observed = self.observe_delivery_activity(activity, row.as_ref())?;
                if let Some(change) = self.prepare_delivery_activity(observed, through)? {
                    changes.push(change);
                }
            }
            Ok(changes)
        }
        .await;
        settlement.complete(result)
    }

    fn prepare_local_delivery_inventory(
        &self,
        activities: impl Iterator<Item = DeliveryActivity>,
    ) -> StorageResult<std::collections::BTreeMap<DurableRecordKey, DeliveryActivity>> {
        let mut inventory = std::collections::BTreeMap::<DurableRecordKey, DeliveryActivity>::new();
        for activity in activities {
            let key = self.delivery_activity_key(&activity)?;
            match inventory.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(activity);
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let current = entry.get_mut();
                    if current.physical != activity.physical
                        || current.table != activity.table
                        || current.source_table != activity.source_table
                        || current.lineage != activity.lineage
                        || current.origin != activity.origin
                    {
                        return Err(corrupt_record());
                    }
                    current.through = current.through.max(activity.through);
                }
            }
        }
        Ok(inventory)
    }
}
