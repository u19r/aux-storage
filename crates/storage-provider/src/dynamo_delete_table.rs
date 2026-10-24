//! One table-deletion decision on the caller's protected native attempt.

use std::future::Future;

use storage_types::{
    BoundTableInfo, DeleteTableRequest, DeleteTableResponse, StorageError, StorageResult,
    TableDescription, TableStatus, TimestampMillis,
};

use crate::{
    CatalogAttempt, SemanticDecision, TableMetadataCache,
    dynamo_catalog::resolve_prepared_table_in_attempt,
};

/// Lowerers remove the public binding and stage incarnation-scoped reclamation
/// in the same native attempt that protected the catalog observation.
pub trait DeleteTableAttempt:
    CatalogAttempt + infrastructure_provider::physical_attempt::PhysicalRecordAttempt
{
    fn delete_table_layout(
        &mut self,
        table: &BoundTableInfo,
        captured_at: TimestampMillis,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

pub async fn delete_table_in_attempt<A: DeleteTableAttempt>(
    attempt: &mut A,
    cache: &TableMetadataCache,
    request: &DeleteTableRequest,
    captured_at: TimestampMillis,
) -> StorageResult<SemanticDecision<DeleteTableResponse>> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (prepared, _) =
            match resolve_prepared_table_in_attempt(attempt, cache, &request.table_name).await? {
                SemanticDecision::Accepted(table) => table,
                SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
            };
        let table = prepared.table;
        if table.info.deletion_protection_enabled {
            return Ok(SemanticDecision::Rejected(
                StorageError::deletion_protection_enabled(&request.table_name),
            ));
        }
        let description = TableDescription::from_stored_table_info(
            table.info.as_ref().clone(),
            TableStatus::Deleting,
        );
        attempt.delete_table_layout(&table, captured_at).await?;
        Ok(SemanticDecision::Accepted(DeleteTableResponse {
            table_description: description,
        }))
    }
    .await;
    settlement.complete(result)
}
