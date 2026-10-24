use infrastructure_provider::record::DurableRecordKey;
use replication_types::identity::{
    origin::{OriginId, OriginPosition},
    resource::{TableIdentity, TableLineage},
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::journal) struct DeliveryActivity {
    pub table: TableIdentity,
    pub lineage: TableLineage,
    pub source_table: TableIdentity,
    pub origin: OriginId,
    pub through: OriginPosition,
}

pub(in crate::journal) struct DeliveryActivityObservation {
    pub key: DurableRecordKey,
    pub revision: Option<storage_types::Revision>,
    pub activity: DeliveryActivity,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DeliveryCursor {
    pub after: Option<DurableRecordKey>,
    pub current: Option<DurableRecordKey>,
    pub next_destination: u32,
}

impl DeliveryCursor {
    pub(super) fn require_range(
        &self,
        range: &infrastructure_provider::page::BoundedRecordRange,
    ) -> storage_types::StorageResult<()> {
        let (first, last, _) = range.bounds();
        let contains = |key: &DurableRecordKey| key >= first && key < last;
        if self.after.as_ref().is_some_and(|key| !contains(key))
            || self.current.as_ref().is_some_and(|key| !contains(key))
            || match &self.current {
                Some(current) => {
                    self.next_destination == 0
                        || self.after.as_ref().is_some_and(|after| current <= after)
                }
                None => self.next_destination != 0,
            }
        {
            return Err(crate::native_record::corrupt_record());
        }
        Ok(())
    }
}

pub(crate) struct DeliverySelection {
    pub scope: Option<crate::delivery::DeliveryScope>,
    pub wrapped: bool,
}
