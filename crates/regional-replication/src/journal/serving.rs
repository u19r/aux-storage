use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::installed::RegionalTablePhase;
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use super::JournalStore;

pub(in crate::journal) struct HistoryTableObservation {
    pub(in crate::journal) state: super::TableStateObservation,
    pub(in crate::journal) authority: super::public_trim_authority::PublicTrimAuthority,
}

impl HistoryTableObservation {
    pub(in crate::journal) fn state(
        &self,
    ) -> &replication_types::configuration::installed::InstalledTableState {
        self.state.state()
    }
}

impl JournalStore {
    pub(in crate::journal) async fn observe_history_table<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        protection: storage_types::ReadProtection,
    ) -> StorageResult<HistoryTableObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let state = self
                .observe_capture_state(attempt, table, protection)
                .await?
                .ok_or_else(|| {
                    StorageError::unsupported(
                        "Canonical history requires an installed regional table",
                    )
                })?;
            self.require_readable_state(state.state())?;
            let authority = self
                .load_public_trim_head(
                    attempt,
                    (state.state(), state.state().configuration.lineage),
                    protection,
                )
                .await?;
            Ok(HistoryTableObservation { state, authority })
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) fn require_origin_admission(
        &self,
        state: Option<&super::TableStateObservation>,
    ) -> StorageResult<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let state = state.state();
        self.require_local_source(state)?;
        match state.phase {
            RegionalTablePhase::Joining | RegionalTablePhase::Removed => {
                return Err(StorageError::table_not_found(&state.name));
            }
            RegionalTablePhase::Sealing | RegionalTablePhase::Blocked => {
                return Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into());
            }
            _ => {}
        }
        match (&state.configuration.regional, state.phase) {
            (
                replication_types::configuration::table::RegionalMode::Local,
                RegionalTablePhase::Active | RegionalTablePhase::Capturing,
            ) => Ok(()),
            (
                replication_types::configuration::table::RegionalMode::Global { members, .. },
                RegionalTablePhase::Active,
            ) if members.contains(&self.region) => Ok(()),
            _ => Err(crate::native_record::corrupt_record()),
        }
    }

    pub(crate) async fn is_listed<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<bool> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self
                .observe_capture_state(attempt, table, storage_types::ReadProtection::Snapshot)
                .await?
            else {
                return Ok(true);
            };
            self.require_local_source(state.state())?;
            Ok(!matches!(
                state.state().phase,
                RegionalTablePhase::Joining | RegionalTablePhase::Removed
            ))
        }
        .await;
        settlement.complete(result)
    }

    /// Native snapshot admission is independent of WAN availability. Joining
    /// and removed incarnations stay hidden; blocked state fails closed.
    pub(crate) async fn require_readable<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(state) = self
                .observe_capture_state(attempt, table, storage_types::ReadProtection::Snapshot)
                .await?
            else {
                return Ok(());
            };
            self.require_readable_state(state.state())
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn require_range_readable<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        index: Option<&storage_types::IndexName>,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let Some(index) = index else {
                return self.require_readable(attempt, table).await;
            };
            let Some(state) = self
                .observe_capture_state(attempt, table, storage_types::ReadProtection::Snapshot)
                .await?
            else {
                return Ok(());
            };
            self.require_readable_state(state.state())?;
            self.require_shared_index_ready(attempt, state.state(), index)
                .await
        }
        .await;
        settlement.complete(result)
    }

    fn require_readable_state(
        &self,
        state: &replication_types::configuration::installed::InstalledTableState,
    ) -> StorageResult<()> {
        self.require_local_source(state)?;
        match state.phase {
            RegionalTablePhase::Active
            | RegionalTablePhase::Capturing
            | RegionalTablePhase::Sealing => Ok(()),
            RegionalTablePhase::Joining | RegionalTablePhase::Removed => {
                Err(StorageError::table_not_found(&state.name))
            }
            RegionalTablePhase::Blocked => {
                Err(storage_types::InfrastructureResultKind::Unavailable {
                    context: storage_types::InfrastructureContext::RegionalSource,
                }
                .into())
            }
        }
    }
}
