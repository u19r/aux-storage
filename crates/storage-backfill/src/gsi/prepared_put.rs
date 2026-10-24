//! New-image projection prepared independently of transactional observations.

use storage_types::{
    AttributeValue, ItemRevision, StorageError, StorageResult, StoredTableInfo, WireItem,
};

use super::{
    GsiProjectedRow, GsiProjectionKey, GsiProjectionLifecycle, GsiProjectionWrite,
    GsiProjectionWriteBatch, GsiWriteAction, lifecycle::projection_key,
    projection::plan_gsi_write_actions_for,
};

#[derive(Default)]
pub struct PreparedGsiPut {
    writes: Vec<PreparedProjection>,
}

struct PreparedProjection {
    key: GsiProjectionKey,
    row: GsiProjectedRow,
    comparison: Option<Vec<Option<AttributeValue>>>,
}

impl PreparedGsiPut {
    pub fn rows(&self) -> impl Iterator<Item = &GsiProjectedRow> {
        self.writes.iter().map(|put| &put.row)
    }

    pub fn try_new(
        table: &StoredTableInfo,
        item: &WireItem,
        indexers: &storage_types::IndexerDeclaration,
    ) -> StorageResult<Self> {
        let indexes = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .filter(|index| !index.index_name.is_ttl_index())
            .collect::<Vec<_>>();
        let writes = plan_gsi_write_actions_for(table, &indexes, None, Some(item))?
            .into_iter()
            .map(|action| PreparedProjection::try_new(action, item, indexers))
            .collect::<StorageResult<Vec<_>>>()?;
        Ok(Self { writes })
    }

    pub fn finish(
        self,
        lifecycle: &GsiProjectionLifecycle,
        table: &StoredTableInfo,
        old: Option<&WireItem>,
        revision: ItemRevision,
    ) -> StorageResult<GsiProjectionWriteBatch> {
        let mut deletes = lifecycle.plan_deletes(table, old, revision)?;
        let mut writes = Vec::with_capacity(self.writes.len() + deletes.len());
        for put in self.writes {
            if !lifecycle.writable_targets().iter().any(|target| {
                target.table_name == table.table_name && target.index_name == put.key.index_name
            }) {
                continue;
            }
            let previous = deletes.iter().position(|delete| delete.key == put.key);
            if let Some(position) = previous {
                deletes.swap_remove(position);
                if put.has_same_projection(old)? {
                    continue;
                }
            }
            writes.push(GsiProjectionWrite {
                key: put.key,
                row: Some(put.row),
                item_version: revision,
            });
        }
        deletes.extend(writes);
        Ok(GsiProjectionWriteBatch(deletes))
    }
}

impl PreparedProjection {
    fn try_new(
        action: GsiWriteAction<'_>,
        item: &WireItem,
        indexers: &storage_types::IndexerDeclaration,
    ) -> StorageResult<Self> {
        let GsiWriteAction::Put {
            index,
            gsi_key,
            table_key,
            projection,
        } = action
        else {
            return Err(StorageError::internal(
                "new-only GSI plan contains a delete",
            ));
        };
        let comparison = projection.select_values(item)?;
        let key = projection_key(index, gsi_key, table_key);
        Ok(Self {
            row: GsiProjectedRow {
                key: key.clone(),
                source_item: item.clone(),
                projection,
                indexers: indexers.clone(),
            },
            key,
            comparison,
        })
    }

    fn has_same_projection(&self, old: Option<&WireItem>) -> StorageResult<bool> {
        let Some(old) = old else {
            return Ok(false);
        };
        let Some(values) = &self.comparison else {
            return Ok(false);
        };
        Ok(self.row.projection.select_values(old)?.as_ref() == Some(values))
    }
}
