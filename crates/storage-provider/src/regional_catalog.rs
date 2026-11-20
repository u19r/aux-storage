//! The pair binding of one table: physical port and compare-and-set operation.

use std::{future::Future, sync::Arc};

use async_trait::async_trait;
use storage_types::{
    BoundTableInfo, PairId, RegionalBinding, StorageError, StorageResult, TableName,
};

use crate::{DynamoPort, TableUpdateTransaction};

/// Families store the binding in the catalog record and change the catalog
/// stamp.
pub trait RegionalCatalogAttempt: TableUpdateTransaction {
    /// `table` is the protected observation of this attempt
    /// (`observe_table_update`).
    fn store_regional_binding(
        &mut self,
        table: &BoundTableInfo,
        regional: Option<Arc<RegionalBinding>>,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}

#[async_trait]
pub trait RegionalBindingOperations: Send + Sync {
    /// Replace the binding when the current pair is `expected` (None:
    /// unpaired). Returns false and changes nothing when it differs. An unknown
    /// commit outcome is returned as the error; the caller reads the binding
    /// again and never repeats blindly.
    async fn replace_regional_binding(
        &self,
        table: &TableName,
        expected: Option<PairId>,
        next: Option<RegionalBinding>,
    ) -> StorageResult<bool>;
}

#[async_trait]
impl<P: DynamoPort> RegionalBindingOperations for P
where for<'native> P::Attempt<'native>: RegionalCatalogAttempt
{
    async fn replace_regional_binding(
        &self,
        table: &TableName,
        expected: Option<PairId>,
        next: Option<RegionalBinding>,
    ) -> StorageResult<bool> {
        let next = next.map(Arc::new);
        let replaced = storage_common::retry::retry_tries(
            self,
            self.attempt_deadlines(),
            crate::constants::SCHEMA_CHANGE,
            |attempt_request, _| {
                let name = table.clone();
                let next = next.clone();
                async move {
                    self.with_attempt(
                        attempt_request.scoped(storage_types::AttemptScope::Schema),
                        move |attempt| {
                            Box::pin(async move {
                                let current = attempt
                                    .observe_table_update(&name)
                                    .await?
                                    .ok_or_else(|| StorageError::table_not_found(&name))?;
                                if current
                                    .binding
                                    .regional
                                    .as_ref()
                                    .map(|binding| binding.pair)
                                    != expected
                                {
                                    return Ok(false);
                                }
                                attempt.store_regional_binding(&current, next).await?;
                                Ok(true)
                            })
                        },
                    )
                    .await
                }
            },
        )
        .await?;
        self.metadata_cache().invalidate(table);
        Ok(replaced)
    }
}
