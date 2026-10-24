//! Physical SQL DeleteItem effects on the caller's native write attempt.

use storage_common::TtlIndexWriteBatch;
use storage_provider::{AcceptedDeleteItem, DeleteItemAttempt, ItemPrimaryEffects};
use storage_types::StorageResult;

use crate::{
    SqlAttempt, SqlLayout,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    item_tombstone::sql_statements::primary_delete_statements,
    item_write::ttl_index_statements,
};

impl<A: SqlAttempt, L: SqlLayout> DeleteItemAttempt for SqlCatalogAttempt<'_, A, L> {
    fn stage_primary_delete(
        &mut self,
        item: &AcceptedDeleteItem,
        effects: &ItemPrimaryEffects,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        let result = (|| {
            let mut statements =
                primary_delete_statements(catalog.engine, &item.table, &item.key, effects)?;
            let ttl = ttl_index_statements(catalog.engine, &item.table, writes)?;
            statements
                .try_reserve(ttl.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            statements.extend(ttl);
            catalog.append_dynamo_statements(
                SqlDynamoWriteKind::Primary,
                statements
                    .into_iter()
                    .enumerate()
                    .map(|(index, statement)| {
                        (
                            statement,
                            if index == 1 {
                                SqlDynamoCount::ExactlyOne
                            } else {
                                SqlDynamoCount::AtMostOne
                            },
                        )
                    }),
            )?;
            Ok(())
        })();
        operation.complete(result)
    }
}
