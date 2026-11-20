//! KV new-image encoding before the native conflict window opens.

use storage_provider::{AcceptedUpsert, BoundPutItem, LiveItemMetadata, UpsertImage};
use storage_types::{ItemKey, KeyAttributes, StorageResult, TableHistoryId};

use crate::{KvMutation, gsi::PreparedGsiWrite, item_codec::encode_primary_item};

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
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        let projections = items
            .iter()
            .try_fold(0usize, |count, item| {
                count.checked_add(item.gsi_puts().count())
            })
            .ok_or(storage_types::AttemptFailure::CAPACITY)?;
        prepared
            .gsi
            .try_reserve_exact(projections)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
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

pub(crate) struct PreparedPrimaryWrite {
    history_id: TableHistoryId,
    key: KeyAttributes,
    suffix: Vec<u8>,
    image: Vec<u8>,
}

impl PreparedPrimaryWrite {
    pub(crate) fn try_new(item: UpsertImage<'_>) -> StorageResult<Self> {
        let item_key = ItemKey::from_key_schema(
            item.table.table_name.clone(),
            &item.table.key_schema,
            item.key,
        )?;
        let image = storage_types::MaterializedItemRecord::encode(
            item.table.history_id,
            item.new_item,
            item.declaration,
        )?;
        Ok(Self {
            history_id: item.table.history_id,
            key: item.key.clone(),
            suffix: item_key.sorted_storage_suffix()?,
            image,
        })
    }

    pub(crate) fn matches(&self, item: &AcceptedUpsert) -> bool {
        self.history_id == item.table.history_id && self.key == item.key
    }

    pub(crate) fn prepare(
        self,
        table_id: u32,
        effects: &LiveItemMetadata<'_>,
    ) -> StorageResult<Vec<KvMutation>> {
        let mut changes = Vec::new();
        changes
            .try_reserve_exact(1)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        let key = crate::item_read::item_key_from_suffix(table_id, &self.suffix)?;
        let image = self.image;
        changes.push(KvMutation::Put {
            key,
            value: encode_primary_item(&image, effects)?,
        });
        Ok(changes)
    }
}
