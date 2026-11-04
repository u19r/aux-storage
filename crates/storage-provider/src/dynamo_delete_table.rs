//! One table-deletion decision on the caller's protected native attempt.

use std::future::Future;

use storage_types::{
    BoundTableInfo, DeleteTableResponse, StorageError, StorageResult, TableDescription, TableStatus,
};

use crate::{CatalogAttempt, SemanticDecision, TableReclaimPhase};

/// Lowerers remove the public binding and stage incarnation-scoped reclamation
/// in the same native attempt that protected the catalog observation.
pub trait DeleteTableAttempt:
    CatalogAttempt + infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn load_external_cleanup_requirement(
        &mut self,
        table: &BoundTableInfo,
    ) -> impl Future<Output = StorageResult<bool>> + Send;

    fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        phase: TableReclaimPhase,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

pub async fn delete_table_in_attempt<A: DeleteTableAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<SemanticDecision<DeleteTableResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        if table.info.deletion_protection_enabled {
            return Ok(SemanticDecision::Rejected(
                StorageError::deletion_protection_enabled(&table.info.table_name),
            ));
        }
        let description =
            TableDescription::from_bound_table_info(table.clone(), TableStatus::Deleting);
        retire_table_in_attempt(attempt, table).await?;
        Ok(SemanticDecision::Accepted(DeleteTableResponse {
            table_description: description,
        }))
    }
    .await;
    settlement.complete(result)
}

/// Capture the durable cleanup requirement before detaching this incarnation.
/// Internal retirement uses the same boundary as customer DeleteTable.
pub async fn retire_table_in_attempt<A: DeleteTableAttempt>(
    attempt: &mut A,
    table: &BoundTableInfo,
) -> StorageResult<()> {
    let phase = if attempt.load_external_cleanup_requirement(table).await? {
        TableReclaimPhase::ExternalData
    } else {
        TableReclaimPhase::MetadataCleanup
    };
    attempt.delete_table_layout(table, phase).await
}
