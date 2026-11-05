//! Physical SQL DeleteItem effects on the caller's native write attempt.

use storage_common::TtlIndexWriteBatch;
use storage_provider::{AcceptedDeleteItem, DeleteItemAttempt, ItemPrimaryEffects};
use storage_types::StorageResult;

use crate::{
    SqlAttempt, SqlLayout,
    catalog::{SqlCatalogAttempt, SqlDynamoCount, SqlDynamoWriteKind},
    item_tombstone::sql_statements::tombstone_upsert_row,
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
            catalog.stage_primary_delete(crate::write_preparation::encode_primary_delete(
                catalog.engine,
                &item.table,
                &item.key,
            )?)?;
            catalog.stage_tombstone_upsert(tombstone_upsert_row(
                catalog.engine,
                &item.table,
                &item.key,
                effects,
            )?)?;
            let ttl = ttl_index_statements(catalog.engine, &item.table, writes)?;
            if !ttl.is_empty() {
                catalog.append_dynamo_statements(
                    SqlDynamoWriteKind::Primary,
                    ttl.into_iter()
                        .map(|statement| (statement, SqlDynamoCount::AtMostOne)),
                )?;
            }
            Ok(())
        })();
        operation.complete(result)
    }
}
