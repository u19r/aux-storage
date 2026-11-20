//! Apply shared projection decisions to KV GSI rows.

use std::collections::BTreeMap;

use storage_backfill::{GsiProjectionKey, GsiProjectionWriteBatch};
use storage_types::{BoundTableInfo, StorageError, StorageResult, WireItem};

use super::key_layout::{entry_key, projection_suffix};
use crate::catalog::table_id_from_history;

#[derive(Default)]
pub(crate) struct ProjectionChanges {
    rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

impl ProjectionChanges {
    pub(crate) fn into_mutations(self) -> StorageResult<Vec<crate::KvMutation>> {
        let mut mutations = Vec::new();
        mutations
            .try_reserve_exact(self.rows.len())
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        for (key, value) in self.rows {
            mutations.push(match value {
                Some(value) => crate::KvMutation::Put { key, value },
                None => crate::KvMutation::Delete { key },
            });
        }
        Ok(mutations)
    }
}

pub(crate) struct GsiWriteInput<'a> {
    pub table: &'a BoundTableInfo,
    pub writes: GsiProjectionWriteBatch,
    pub prepared: &'a mut Vec<PreparedGsiWrite>,
}

/// Encode effects derived from the source observed in this native attempt.
pub(crate) fn prepare_gsi_writes<A: crate::KvAttempt>(
    catalog: &mut crate::catalog::KvCatalogAttempt<'_, A>,
    input: GsiWriteInput<'_>,
) -> StorageResult<()> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = operation.as_mut();
    let result = (|| {
        let table = input.table;
        for (item_key, row) in bind_gsi_writes(input)? {
            note_inline_entry(&mut catalog.primary_layouts, table, &item_key, &row.key)?;
            row.store(&mut catalog.projection_changes)?;
        }
        Ok(())
    })();
    operation.complete(result)
}

/// A GSI entry is a projection of its source item and is never larger than
/// the item's primary record. When this attempt observed the source item
/// inline (or absent), the entry at this key holds no chunks, so its put or
/// delete needs no chunk clear.
fn note_inline_entry(
    layouts: &mut Vec<(Vec<u8>, bool)>,
    table: &BoundTableInfo,
    item_key: &storage_types::KeyAttributes,
    entry: &[u8],
) -> StorageResult<()> {
    // Engines without chunks retain no layouts.
    if layouts.is_empty() || layouts.iter().any(|(known, _)| known.as_slice() == entry) {
        return Ok(());
    }
    let primary = crate::item_read::primary_item_key(
        table_id_from_history(table.history_id)?,
        table,
        item_key,
    )?;
    if layouts
        .iter()
        .any(|(known, has_chunks)| *known == primary && !has_chunks)
    {
        layouts
            .try_reserve(1)
            .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
        layouts.push((entry.to_vec(), false));
    }
    Ok(())
}

fn bind_gsi_writes(
    input: GsiWriteInput<'_>,
) -> StorageResult<Vec<(storage_types::KeyAttributes, BoundGsiWrite)>> {
    let mut rows = Vec::new();
    let writes = input.writes.into_iter();
    rows.try_reserve_exact(writes.len())
        .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
    for write in writes {
        let prepared = if write.row.is_some() {
            input
                .prepared
                .iter()
                .position(|put| put.history_id == input.table.history_id && put.key == write.key)
        } else {
            None
        };
        let prepared = match prepared {
            Some(position) => input.prepared.swap_remove(position),
            None => PreparedGsiWrite::try_new(input.table, &write.key, write.row.as_ref())?,
        };
        rows.push((write.key.table_item_key, prepared.bind(input.table)?));
    }
    Ok(rows)
}

/// A prepared projection put or clear bound to the catalog index identity.
pub(crate) struct PreparedGsiWrite {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) key: GsiProjectionKey,
    suffix: Vec<u8>,
    image: Option<Vec<u8>>,
}

impl PreparedGsiWrite {
    pub(crate) fn try_new(
        table: &BoundTableInfo,
        key: &GsiProjectionKey,
        row: Option<&storage_backfill::GsiProjectedRow>,
    ) -> StorageResult<Self> {
        let image = row
            .map(|row| {
                let image = row.projected_image()?;
                let declaration = storage_types::IndexerDeclaration::try_new(
                    Vec::new(),
                    storage_types::MaxIndexers::ZERO,
                )?;
                storage_types::MaterializedItemRecord::encode(
                    table.history_id,
                    &image,
                    &declaration,
                )
            })
            .transpose()?;
        Ok(Self {
            history_id: table.history_id,
            key: key.clone(),
            suffix: projection_suffix(table, key)?,
            image,
        })
    }

    fn bind(self, table: &BoundTableInfo) -> StorageResult<BoundGsiWrite> {
        let index = table
            .binding
            .gsi(&self.key.index_name)
            .ok_or_else(|| StorageError::internal("GSI projection has no bound index"))?;
        Ok(BoundGsiWrite {
            key: entry_key(
                table_id_from_history(table.history_id)?,
                index.index_id,
                &self.suffix,
            )?,
            image: self.image,
        })
    }
}

struct BoundGsiWrite {
    key: Vec<u8>,
    image: Option<Vec<u8>>,
}

impl BoundGsiWrite {
    fn store(self, changes: &mut ProjectionChanges) -> StorageResult<()> {
        let value = self.image.map(|image| {
            let mut value = Vec::with_capacity(1 + image.len());
            value.push(crate::constants::GSI_PROJECTION_FORMAT);
            value.extend_from_slice(&image);
            value
        });
        changes.rows.insert(self.key, value);
        Ok(())
    }
}

pub(crate) struct DecodedProjection {
    pub image: WireItem,
}

pub(super) fn decode_projection(
    table: &BoundTableInfo,
    value: &[u8],
) -> StorageResult<DecodedProjection> {
    let (&format, image) = value
        .split_first()
        .ok_or_else(|| StorageError::internal("GSI envelope is empty"))?;
    if format != crate::constants::GSI_PROJECTION_FORMAT {
        return Err(StorageError::internal("GSI envelope format is invalid"));
    }
    let image = storage_types::MaterializedItemRecord::decode(
        image,
        table.history_id,
        storage_types::MaterializedItemRecord::encoded_length_bound(),
    )?
    .into_stored(storage_types::MaxIndexers::ZERO)?
    .item;
    Ok(DecodedProjection { image })
}
