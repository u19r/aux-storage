use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{installed::RegionalTablePhase, table::RegionalMode},
    identity::resource::TableIdentity,
};
use storage_provider::DescribeTimeToLiveAttempt;
use storage_types::{ReadProtection, StorageResult};

use crate::{
    addition::AdditionSourceObservation, bootstrap::SourceSchemaObservation, journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    pub(crate) async fn observe_addition_source<
        A: PhysicalRecordAttempt
            + DescribeTimeToLiveAttempt
            + storage_provider::TableUpdateTransaction<Catalog = storage_types::BoundTableInfo>,
    >(
        &self,
        attempt: &mut A,
        table: TableIdentity,
    ) -> StorageResult<AdditionSourceObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let source = self
                .observe_table_state(attempt, table, ReadProtection::Snapshot)
                .await?
                .ok_or_else(corrupt_record)?;
            let source = source.state();
            self.require_local_source(source)?;
            let RegionalMode::Global { primary, .. } = &source.configuration.regional else {
                return Err(corrupt_record());
            };
            if primary != &self.region
                || source.phase != RegionalTablePhase::Active
                || !self
                    .is_local_control_idle(attempt, table, ReadProtection::Snapshot)
                    .await?
            {
                return Err(corrupt_record());
            }
            let current = self
                .load_configuration_control(attempt, source, ReadProtection::Snapshot)
                .await?;
            let schema = self.freeze_capture_schema(attempt, source).await?;
            Ok(AdditionSourceObservation {
                current: current.proposal,
                schema: SourceSchemaObservation {
                    source: source.clone(),
                    schema,
                },
            })
        }
        .await;
        settlement.complete(result)
    }
}
