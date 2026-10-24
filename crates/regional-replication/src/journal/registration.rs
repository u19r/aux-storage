use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalSourceBinding, RegionalTablePhase},
        table::{RegionalMode, TableReplicationConfig},
    },
    identity::resource::TableIdentity,
};
use storage_provider::CatalogAttempt;
use storage_types::{BoundTableInfo, ReadProtection, StorageResult};

use crate::{
    constants::CONTROL_SLOT_SUFFIX,
    journal::{JournalStore, control_slot::ControlSlotOwner},
    native_record::corrupt_record,
};

impl JournalStore {
    /// Bind an ordinary incarnation only in the customer's enable attempt.
    /// The configured tenant is the native database owner, never a derived ID.
    pub(crate) async fn register_local_source<A: CatalogAttempt + PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        identity: TableIdentity,
    ) -> StorageResult<InstalledTableState> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            if let Some(observed) = self
                .observe_capture_state(attempt, table, ReadProtection::Conflict)
                .await?
            {
                self.require_local_source(observed.state())?;
                return Ok(observed.state().clone());
            }
            if self
                .observe_table_state(attempt, identity, ReadProtection::Conflict)
                .await?
                .is_some()
            {
                return Err(corrupt_record());
            }
            let slot = self.identity_key(CONTROL_SLOT_SUFFIX, &identity)?;
            if self
                .records
                .load(
                    attempt,
                    std::slice::from_ref(&slot),
                    ReadProtection::Conflict,
                )
                .await?[0]
                .is_some()
            {
                return Err(corrupt_record());
            }
            let config = 0.into();
            let state = InstalledTableState {
                name: table.table_name.clone(),
                physical: table.history_id,
                configuration: TableReplicationConfig {
                    table: identity,
                    lineage: <[u8; 16]>::from(identity)
                        .try_into()
                        .map_err(|_| corrupt_record())?,
                    tenant: self.tenant,
                    regional: RegionalMode::Local,
                    config,
                    schema: 1.into(),
                },
                membership: 0.into(),
                phase: RegionalTablePhase::Active,
                schemas: vec![1.into()],
                sources: vec![RegionalSourceBinding {
                    region: self.region.clone(),
                    database_group: self.group,
                    generation: self.generation,
                    partitions: self.partitions,
                    table: identity,
                    config,
                }],
            };
            self.persist_table_state(attempt, (None, state.clone()))
                .await?;
            let slot = self.prepare_control_slot(identity, None, ControlSlotOwner::Idle)?;
            self.records.store(attempt, &[slot]).await?;
            Ok(state)
        }
        .await;
        settlement.complete(result)
    }
}
