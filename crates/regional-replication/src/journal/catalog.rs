use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::installed::InstalledTableState, identity::resource::TableIdentity,
};
use storage_provider::CatalogAttempt;
use storage_types::{BoundTableInfo, ReadProtection, Revision, StorageResult};

use super::JournalStore;
use crate::{
    constants::{CAPTURE_SUFFIX, TABLE_STATE_SUFFIX},
    native_record::corrupt_record,
};

pub struct TableStateObservation {
    state: InstalledTableState,
    pub(super) revision: Revision,
}

impl TableStateObservation {
    pub fn state(&self) -> &InstalledTableState {
        &self.state
    }
}

impl JournalStore {
    pub(crate) fn require_local_source(&self, state: &InstalledTableState) -> StorageResult<()> {
        if state.configuration.tenant != self.tenant
            || !state.sources.iter().any(|source| {
                source.region == self.region
                    && source.database_group == self.group
                    && source.generation == self.generation
                    && source.partitions == self.partitions
                    && source.table == state.configuration.table
                    && source.config == state.configuration.config
            })
        {
            return Err(corrupt_record());
        }
        Ok(())
    }

    pub async fn load_table_state<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: TableIdentity,
    ) -> StorageResult<Option<TableStateObservation>> {
        self.observe_table_state(attempt, table, ReadProtection::Shared)
            .await
    }

    pub(super) async fn observe_table_state<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: TableIdentity,
        protection: ReadProtection,
    ) -> StorageResult<Option<TableStateObservation>> {
        self.observe_table_states(attempt, &[table], protection)
            .await?
            .pop()
            .ok_or_else(corrupt_record)
    }

    pub(crate) async fn observe_table_states<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        tables: &[TableIdentity],
        protection: ReadProtection,
    ) -> StorageResult<Vec<Option<TableStateObservation>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = tables
                .iter()
                .map(|table| self.identity_key(TABLE_STATE_SUFFIX, table))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self.records.load(attempt, &keys, protection).await?;
            rows.iter()
                .zip(tables)
                .map(|(row, table)| {
                    row.as_ref()
                        .map(|record| self.decode_table_state(record, *table))
                        .transpose()
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }

    fn decode_table_state(
        &self,
        record: &infrastructure_provider::record::DurableRecord,
        table: TableIdentity,
    ) -> StorageResult<TableStateObservation> {
        let state: InstalledTableState = self.records.decode(record)?;
        state.validate().map_err(|_| corrupt_record())?;
        if state.configuration.table != table {
            return Err(corrupt_record());
        }
        Ok(TableStateObservation {
            state,
            revision: record.revision,
        })
    }

    /// Select capture under the actual protected catalog incarnation, not a
    /// preliminary ordinary/global routing read.
    pub async fn load_capture_state<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<Option<TableStateObservation>> {
        self.observe_capture_state(attempt, table, ReadProtection::Shared)
            .await
    }

    pub(crate) async fn load_capture_states<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        tables: &[&BoundTableInfo],
    ) -> StorageResult<Vec<Option<TableStateObservation>>> {
        self.observe_capture_states(attempt, tables, ReadProtection::Shared)
            .await
    }

    pub(super) async fn observe_capture_state<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        protection: ReadProtection,
    ) -> StorageResult<Option<TableStateObservation>> {
        self.observe_capture_states(attempt, &[table], protection)
            .await?
            .pop()
            .ok_or_else(corrupt_record)
    }

    async fn observe_capture_states<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        tables: &[&BoundTableInfo],
        protection: ReadProtection,
    ) -> StorageResult<Vec<Option<TableStateObservation>>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let keys = tables
                .iter()
                .map(|table| self.identity_key(CAPTURE_SUFFIX, &table.history_id))
                .collect::<StorageResult<Vec<_>>>()?;
            let rows = self.records.load(attempt, &keys, protection).await?;
            let identities = rows
                .iter()
                .map(|row| {
                    row.as_ref()
                        .map(|record| self.records.decode::<TableIdentity>(record))
                        .transpose()
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let selected = identities.iter().flatten().copied().collect::<Vec<_>>();
            let mut states = self
                .observe_table_states(attempt, &selected, protection)
                .await?
                .into_iter();
            tables
                .iter()
                .zip(identities)
                .map(|(table, identity)| {
                    if identity.is_none() {
                        return Ok(None);
                    }
                    let state = states.next().flatten().ok_or_else(corrupt_record)?;
                    if state.state.physical != table.history_id
                        || state.state.name != table.table_name
                    {
                        return Err(corrupt_record());
                    }
                    Ok(Some(state))
                })
                .collect()
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn persist_table_state<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        input: (Option<TableStateObservation>, InstalledTableState),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (observed, next) = input;
            next.validate().map_err(|_| corrupt_record())?;
            let stamp = attempt
                .read_catalog_stamp(&next.name)
                .await?
                .ok_or_else(corrupt_record)?;
            if stamp.history_id != next.physical
                || observed
                    .as_ref()
                    .is_some_and(|old| old.state.configuration.table != next.configuration.table)
            {
                return Err(corrupt_record());
            }
            self.prepare_table_trim_authority(
                attempt,
                (observed.as_ref().map(TableStateObservation::state), &next),
            )
            .await?;
            self.bind_capture_pointer(attempt, &next).await?;
            let key = self.identity_key(TABLE_STATE_SUFFIX, &next.configuration.table)?;
            let change = self
                .records
                .prepare(key, observed.map(|old| old.revision), &next)?;
            self.records.store(attempt, &[change]).await?;
            attempt.advance_catalog_stamp(&next.name, stamp).await?;
            Ok(())
        }
        .await;
        settlement.complete(result)
    }

    async fn bind_capture_pointer<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        next: &InstalledTableState,
    ) -> StorageResult<()> {
        let pointer = self.identity_key(CAPTURE_SUFFIX, &next.physical)?;
        let rows = self
            .records
            .load(
                attempt,
                std::slice::from_ref(&pointer),
                ReadProtection::Conflict,
            )
            .await?;
        if let Some(record) = &rows[0] {
            if self.records.decode::<TableIdentity>(record)? != next.configuration.table {
                return Err(corrupt_record());
            }
        } else {
            let change = self
                .records
                .prepare(pointer, None, &next.configuration.table)?;
            self.records.store(attempt, &[change]).await?;
        }
        Ok(())
    }

    pub fn region(&self) -> &replication_types::identity::replica::RegionId {
        &self.region
    }
}
