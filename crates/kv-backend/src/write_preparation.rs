//! KV new-image encoding before the native conflict window opens.

use storage_provider::{
    AcceptedUpsert, BoundPutItem, ItemPrimaryEffects, PreparedWriteAttempt, UpsertImage,
};
use storage_types::{ItemKey, KeyAttributes, StorageResult, TableHistoryId};

use crate::{
    KvAttempt, KvMutation,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    gsi::PreparedGsiWrite,
    item_codec::encode_primary_item,
    item_tombstone::primary_tombstone_key,
};

#[derive(Default)]
pub struct KvPreparedWrites {
    pub(crate) primary: Vec<PreparedPrimaryWrite>,
    pub(crate) gsi: Vec<PreparedGsiWrite>,
}

impl KvPreparedWrites {
    pub(crate) fn try_new(items: &[&BoundPutItem]) -> StorageResult<Self> {
        let mut prepared = Self::default();
        prepared
            .primary
            .try_reserve_exact(items.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let projections = items
            .iter()
            .try_fold(0usize, |count, item| {
                count.checked_add(item.gsi_puts().count())
            })
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        prepared
            .gsi
            .try_reserve_exact(projections)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for item in items {
            prepared
                .primary
                .push(PreparedPrimaryWrite::try_new((*item).into())?);
            for row in item.gsi_puts() {
                prepared
                    .gsi
                    .push(PreparedGsiWrite::try_new(&item.table, &row.key, Some(row))?);
            }
        }
        Ok(prepared)
    }
}

impl<A: KvAttempt> PreparedWriteAttempt for KvCatalogAttempt<'_, A> {
    type PreparedWrites = KvPreparedWrites;
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>) {
        if let Err(error) = &writes {
            self.attempt.root_attempt().settlement().reject_error(error);
        }
        self.prepared_writes = writes;
    }

    fn prepare_put_items(&mut self, items: &[&BoundPutItem]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        catalog.prepared_writes = Ok(Default::default());
        let result = KvPreparedWrites::try_new(items).map(|writes| {
            catalog.prepared_writes = Ok(writes);
        });
        operation.complete(result)
    }
}

pub(crate) struct PreparedPrimaryWrite {
    history_id: TableHistoryId,
    key: KeyAttributes,
    suffix: Vec<u8>,
    image: stream_provider::retained_image::AcceptedImagePlan,
    owner: stream_provider::retained_image::PrimaryImageOwner,
    tombstone_key: Vec<u8>,
}

impl PreparedPrimaryWrite {
    pub(crate) fn try_new(item: UpsertImage<'_>) -> StorageResult<Self> {
        let item_key = ItemKey::from_key_schema(
            item.table.table_name.clone(),
            &item.table.key_schema,
            item.key,
        )?;
        let owner =
            stream_provider::retained_image::PrimaryImageOwner::try_new(item.table, item.key)?;
        let image = stream_provider::retained_image::AcceptedImagePlan::try_new(
            item.table.history_id,
            item.new_item,
            item.declaration,
        )?;
        Ok(Self {
            history_id: item.table.history_id,
            key: item.key.clone(),
            suffix: item_key.sorted_storage_suffix()?,
            image,
            owner,
            tombstone_key: primary_tombstone_key(item.table, item.key)?.0,
        })
    }

    pub(crate) fn matches(&self, item: &AcceptedUpsert) -> bool {
        self.history_id == item.table.history_id && self.key == item.key
    }

    pub(crate) async fn prepare<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
        identity: &KvTableIdentity,
        effects: &ItemPrimaryEffects,
        previous: Option<&storage_types::VerifiedWireItem>,
    ) -> StorageResult<Vec<KvMutation>> {
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(2)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let bytes = 5usize
            .checked_add(self.suffix.len())
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut key = Vec::new();
        key.try_reserve_exact(bytes)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        key.push(crate::constants::PRIMARY_ITEM_FAMILY);
        key.extend_from_slice(&identity.table_id.to_be_bytes());
        key.extend_from_slice(&self.suffix);
        let image = self.image.prepare(attempt).await?;
        self.owner.replace(attempt, previous, &image).await?;
        let image = image.encode()?;
        changes.push(KvMutation::Put {
            key,
            value: encode_primary_item(&image, effects)?,
        });
        changes.push(KvMutation::Delete {
            key: self.tombstone_key,
        });
        Ok(changes)
    }
}
