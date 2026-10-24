use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::configuration::{installed::RegionalTablePhase, table::RegionalMode};
use storage_types::{ReadProtection, StorageResult};

use super::record::{DeliveryActivity, DeliveryCursor};
use crate::{delivery::DeliveryScope, journal::JournalStore, native_record::corrupt_record};

impl JournalStore {
    pub(super) async fn select_activity_destination<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        activity: &DeliveryActivity,
        cursor: &mut DeliveryCursor,
    ) -> StorageResult<Option<DeliveryScope>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some(table) = self
                .observe_table_state(attempt, activity.table, ReadProtection::Shared)
                .await?
            else {
                return Err(corrupt_record());
            };
            let state = table.state();
            self.require_local_source(state)?;
            if state.configuration.lineage != activity.lineage
                || !matches!(
                    state.phase,
                    RegionalTablePhase::Active | RegionalTablePhase::Sealing
                )
            {
                return Ok(None);
            }
            let RegionalMode::Global { members, .. } = &state.configuration.regional else {
                return Ok(None);
            };
            let Some(source) = state.sources.iter().find(|source| {
                source.region == activity.origin.region
                    && source.database_group == activity.origin.database_group
                    && source.generation == activity.origin.journal_generation
                    && source.table == activity.source_table
            }) else {
                return Ok(None);
            };
            source
                .partitions
                .require_partition(activity.origin.journal_partition)
                .map_err(|_| corrupt_record())?;
            if !members.contains(&activity.origin.region) {
                return Ok(None);
            }
            let index = usize::try_from(cursor.next_destination).map_err(|_| corrupt_record())?;
            let Some(region) = members
                .iter()
                .filter(|region| *region != &self.region && *region != &activity.origin.region)
                .nth(index)
            else {
                return Ok(None);
            };
            cursor.next_destination = cursor
                .next_destination
                .checked_add(1)
                .ok_or_else(corrupt_record)?;
            let destination = state
                .sources
                .iter()
                .find(|source| &source.region == region)
                .ok_or_else(corrupt_record)?
                .clone();
            let scope = DeliveryScope {
                origin: activity.origin.clone(),
                table: activity.source_table,
                local_table: activity.table,
                lineage: activity.lineage,
                membership: state.membership,
                destination,
            };
            let (coverage, _) = self
                .observe_delivery_progress(attempt, &scope, ReadProtection::Shared)
                .await?;
            Ok((coverage.through() < activity.through).then_some(scope))
        }
        .await;
        settlement_guard.complete(result)
    }
}
