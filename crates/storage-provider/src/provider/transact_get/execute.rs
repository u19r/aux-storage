//! One-snapshot TransactGet execution after metadata binding.

use futures::future::{join_all, try_join_all};
use storage_types::{StorageResult, StoredTableInfo, TransactGetItemsResponse};

use super::plan::TransactGetPlan;
use crate::StorageProviderReadContext;

/// Read every member through one provider-owned transactional context and
/// finish the response in request order.
pub async fn execute_transact_get(
    plan: TransactGetPlan,
    metadata: Vec<StorageResult<StoredTableInfo>>,
    context: &dyn StorageProviderReadContext,
) -> StorageResult<TransactGetItemsResponse> {
    let metadata = if metadata.is_empty() {
        join_all(
            plan.table_names()
                .iter()
                .map(|table_name| context.get_table_info(table_name)),
        )
        .await
    } else {
        metadata
    };
    let prepared = plan.bind(metadata)?;
    let items = try_join_all(
        prepared
            .reads()
            .map(|(table, key)| context.get_item(table.table_name.clone(), key.clone(), true)),
    )
    .await?;
    prepared.finish(items)
}
