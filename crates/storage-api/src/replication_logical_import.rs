use storage::DatabaseManager;
use storage_backfill::LogicalBootstrapPreflightDecision;
use storage_types::{StorageError, StorageResult, TableName};

use crate::types::ReplicationLogicalBackfillImportRequest;

pub(crate) async fn enforce_logical_backfill_import_preflight(
    destination: &DatabaseManager,
    request: &ReplicationLogicalBackfillImportRequest,
) -> StorageResult<()> {
    if !request.require_empty_destination {
        return Ok(());
    }

    let table_name = request
        .table_name
        .as_deref()
        .map(TableName::new)
        .ok_or_else(|| {
            StorageError::validation("TableName is required when RequireEmptyDestination is true")
        })?;
    let decision = destination
        .ensure_logical_bootstrap_destination_preflight(
            &table_name,
            &request.source_region,
            &request.manifest.id,
        )
        .await?;
    if matches!(
        decision,
        LogicalBootstrapPreflightDecision::RejectNonEmptyDestination
    ) {
        return Err(StorageError::validation(
            "logical bootstrap destination is not empty",
        ));
    }
    Ok(())
}
