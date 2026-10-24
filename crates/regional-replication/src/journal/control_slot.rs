//! The table-bound native ownership record shared by configuration workflows.
use infrastructure_provider::record::{DurableRecord, RecordChange};
use replication_types::identity::resource::{ControlOperationId, TableIdentity};
use serde::{Deserialize, Serialize};
use storage_types::{Revision, StorageResult};

use super::JournalStore;
use crate::native_record::corrupt_record;

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(in crate::journal) enum ControlSlotOwner {
    Idle,
    Configuration(ControlOperationId),
    Trim(ControlOperationId),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlSlotRecord {
    table: TableIdentity,
    owner: ControlSlotOwner,
}

impl ControlSlotOwner {
    pub(in crate::journal) fn require_configuration(self) -> StorageResult<ControlOperationId> {
        match self {
            Self::Configuration(operation) => Ok(operation),
            Self::Idle | Self::Trim(_) => Err(corrupt_record()),
        }
    }
}

impl JournalStore {
    pub(in crate::journal) fn decode_control_slot(
        &self,
        row: &DurableRecord,
        table: TableIdentity,
    ) -> StorageResult<ControlSlotOwner> {
        let record: ControlSlotRecord = self.records.decode(row)?;
        if record.table != table
            || row.key != self.identity_key(crate::constants::CONTROL_SLOT_SUFFIX, &table)?
        {
            return Err(corrupt_record());
        }
        Ok(record.owner)
    }

    pub(in crate::journal) fn prepare_control_slot(
        &self,
        table: TableIdentity,
        revision: Option<Revision>,
        owner: ControlSlotOwner,
    ) -> StorageResult<RecordChange> {
        let key = self.identity_key(crate::constants::CONTROL_SLOT_SUFFIX, &table)?;
        self.records
            .prepare(key, revision, &ControlSlotRecord { table, owner })
    }
}
