use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::installed::{InstalledTableState, RegionalTablePhase},
    event::identity::CanonicalEventId,
};
use storage_types::{ReadProtection, StorageResult};

use crate::{
    journal::{JournalStore, TableStateObservation},
    native_record::corrupt_record,
};

/// Member mapping retains exact borrowed states; one protected observation owns
/// each distinct installed configuration inside this caller's native attempt.
pub(super) struct OriginConfigurationParents<'state> {
    states: Vec<&'state InstalledTableState>,
    members: Vec<usize>,
}

impl<'state> OriginConfigurationParents<'state> {
    pub(super) fn try_new(tables: &[&'state TableStateObservation]) -> StorageResult<Self> {
        let mut inventory = Self {
            states: Vec::new(),
            members: Vec::with_capacity(tables.len()),
        };
        for table in tables {
            let state = table.state();
            if !matches!(
                state.phase,
                RegionalTablePhase::Capturing | RegionalTablePhase::Active
            ) {
                return Err(corrupt_record());
            }
            let position = inventory.states.iter().position(|previous| {
                previous.configuration.table == state.configuration.table
                    && previous.configuration.config == state.configuration.config
            });
            let position = match position {
                Some(position) => {
                    if inventory.states[position] != state {
                        return Err(corrupt_record());
                    }
                    position
                }
                None => {
                    inventory.states.push(state);
                    inventory.states.len() - 1
                }
            };
            inventory.members.push(position);
        }
        Ok(inventory)
    }

    pub(super) async fn load<A: PhysicalRecordAttempt>(
        self,
        journal: &JournalStore,
        attempt: &mut A,
    ) -> StorageResult<Vec<Option<CanonicalEventId>>> {
        let active = self
            .states
            .iter()
            .copied()
            .filter(|state| state.phase == RegionalTablePhase::Active)
            .collect::<Vec<_>>();
        let mut controls = journal
            .load_configuration_controls(attempt, &active, ReadProtection::Shared)
            .await?
            .into_iter();
        let parents = self
            .states
            .iter()
            .map(|state| {
                if state.phase == RegionalTablePhase::Capturing {
                    return Ok(None);
                }
                let control = controls.next().ok_or_else(corrupt_record)?;
                Ok(Some(CanonicalEventId::Control {
                    lineage: state.configuration.lineage,
                    operation: control.proposal.operation().id,
                }))
            })
            .collect::<StorageResult<Vec<_>>>()?;
        if controls.next().is_some() {
            return Err(corrupt_record());
        }
        self.members
            .into_iter()
            .map(|position| parents.get(position).cloned().ok_or_else(corrupt_record))
            .collect()
    }
}
