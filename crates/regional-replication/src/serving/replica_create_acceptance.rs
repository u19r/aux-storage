use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::runtime_configuration::deployment::DeploymentConfiguration;
use storage_provider::{DynamoAttempt, describe_table_from_table};
use storage_types::{BoundTableInfo, StorageResult, UpdateTableResponse};

use super::replica_create_request::ReplicaCreateRequest;
use crate::{
    addition::AdditionChange, bootstrap::SourceSchemaObservation, enable::EnableChange,
    journal::JournalStore, native_record::corrupt_record,
};

impl JournalStore {
    pub(super) async fn accept_replica_create<A: PhysicalRecordAttempt + DynamoAttempt>(
        &self,
        attempt: &mut A,
        table: &BoundTableInfo,
        request: &ReplicaCreateRequest,
        configuration: &DeploymentConfiguration,
    ) -> StorageResult<UpdateTableResponse> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let current = self.load_capture_state(attempt, table).await?;
            if let Some(state) = current.as_ref().filter(|state| {
                matches!(
                    state.state().configuration.regional,
                    replication_types::configuration::table::RegionalMode::Global { .. }
                )
            }) {
                self.require_readable(attempt, table).await?;
                let observed = self
                    .observe_addition_source(attempt, state.state().configuration.table)
                    .await?;
                let plan = request.bind_addition_plan(&observed, configuration)?;
                plan.require_deployment(configuration)?;
                let record = self
                    .execute_addition(attempt, &plan, &AdditionChange::Observe)
                    .await?;
                let mut response = describe_table_from_table(attempt, table).await?;
                self.project_addition_description(&mut response.table, &record.plan, record.phase)?;
                return Ok(UpdateTableResponse {
                    table_description: response.table,
                });
            }
            self.require_local_metadata(attempt, table).await?;
            let source = self
                .register_local_source(attempt, table, request.source_identity())
                .await?;
            let schema = self.freeze_capture_schema(attempt, &source).await?;
            let observation = SourceSchemaObservation { source, schema };
            let plan = request.bind_enable_plan(&observation, configuration)?;
            plan.require_deployment(configuration)?;
            let record = self
                .execute_enable(attempt, &plan, &EnableChange::Observe)
                .await?;
            if record.content != plan.content()? {
                return Err(corrupt_record());
            }
            let mut response = describe_table_from_table(attempt, table).await?;
            self.project_enable_description(&mut response.table, &record.plan, record.phase)?;
            Ok(UpdateTableResponse {
                table_description: response.table,
            })
        }
        .await;
        settlement.complete(result)
    }
}
