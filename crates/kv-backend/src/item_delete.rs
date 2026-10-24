//! Physical KV DeleteItem effects on the caller's native write attempt.

use storage_common::TtlIndexWriteBatch;
use storage_provider::{AcceptedDeleteItem, DeleteItemAttempt, ItemPrimaryEffects};
use storage_types::StorageResult;

use crate::{
    KvAttempt, catalog::KvCatalogAttempt, item_tombstone::write::primary_delete_mutations,
    item_write::ttl_index_mutations,
};

impl<A: KvAttempt> DeleteItemAttempt for KvCatalogAttempt<'_, A> {
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
            let identity = catalog.identity_for(&item.table.info.table_name)?;
            let mut changes = primary_delete_mutations(identity, &item.table, &item.key, effects)?;
            let ttl = ttl_index_mutations(identity.table_id, writes)?;
            changes
                .try_reserve(ttl.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            changes.extend(ttl);
            catalog.append_dynamo_mutations(changes)?;
            Ok(())
        })();
        operation.complete(result)
    }
}
