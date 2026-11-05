//! Apply shared projection decisions to SQL GSI rows.

use storage_backfill::{GsiBuildTarget, GsiProjectionWriteBatch};
use storage_provider::GsiWriteSource;
use storage_types::{AttributeValue, BoundTableInfo, StorageError, StorageResult};

use super::sql_statements::BoundProjectionKey;
use crate::{
    SqlLayout, SqlValue,
    write_batch::{PreparedUpsertRow, UpsertShape},
};

pub(crate) struct GsiWriteInput<'a> {
    pub table: &'a BoundTableInfo,
    pub source: GsiWriteSource<'a>,
    pub writes: GsiProjectionWriteBatch,
    pub targets: &'a [GsiBuildTarget],
    pub prepared: &'a mut Vec<PreparedGsiWrite>,
}

pub(crate) fn prepare_gsi_writes(
    engine: impl SqlLayout,
    input: GsiWriteInput<'_>,
) -> StorageResult<Vec<PreparedUpsertRow>> {
    let mut rows = Vec::new();
    let writes = input.writes.into_iter();
    rows.try_reserve_exact(writes.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for write in writes {
        input.source.validate(&write)?;
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
            None => PreparedGsiWrite::try_new(engine, input.table, &write.key, write.row.as_ref())?,
        };
        rows.push(prepared.bind(engine, input.table, write.item_version, input.targets)?);
    }
    Ok(rows)
}

/// Keys, payload binds and SQL syntax prepared without a lifecycle observation.
pub(crate) struct PreparedGsiWrite {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) key: storage_backfill::GsiProjectionKey,
    shape: UpsertShape,
    binds: Vec<SqlValue>,
    revision_bind: usize,
}

impl PreparedGsiWrite {
    pub(crate) fn try_new(
        engine: impl SqlLayout,
        table: &BoundTableInfo,
        key: &storage_backfill::GsiProjectionKey,
        row: Option<&storage_backfill::GsiProjectedRow>,
    ) -> StorageResult<Self> {
        let index = table
            .global_secondary_indexes
            .as_deref()
            .unwrap_or_default()
            .iter()
            .find(|index| index.index_name == key.index_name)
            .ok_or_else(|| {
                StorageError::internal("GSI write target is absent from table metadata")
            })?;
        let BoundProjectionKey {
            mut columns,
            mut binds,
            numeric_keys,
        } = BoundProjectionKey::try_new(engine, table, index, key)?;
        let key_count = columns.len();
        let (payload_columns, payload_binds) = gsi_payload(table, row)?;
        columns
            .try_reserve_exact(payload_columns.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        binds
            .try_reserve_exact(payload_binds.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        columns.extend(payload_columns);
        binds.extend(payload_binds);
        let conflict_columns = columns
            .get(..key_count)
            .ok_or_else(|| StorageError::internal("GSI conflict key count exceeds columns"))?
            .to_vec();
        let numeric_columns = (1..=columns.len())
            .map(|position| numeric_keys.contains(&position))
            .collect::<Vec<_>>();
        let shape = UpsertShape::try_new(columns, numeric_columns, conflict_columns, true)?;
        Ok(Self {
            history_id: table.history_id,
            key: key.clone(),
            shape,
            binds,
            revision_bind: key_count + 1,
        })
    }

    fn bind(
        mut self,
        engine: impl SqlLayout,
        table: &BoundTableInfo,
        revision: storage_types::ItemRevision,
        targets: &[GsiBuildTarget],
    ) -> StorageResult<PreparedUpsertRow> {
        let target = targets
            .iter()
            .find(|target| {
                target.table_name == table.table_name && target.index_name == self.key.index_name
            })
            .ok_or_else(|| StorageError::internal("SQL GSI write target was not bound"))?;
        let relation = engine.gsi_table_name(&target.incarnation);
        self.binds[self.revision_bind] = SqlValue::Integer(i64::try_from(revision)?);
        Ok(PreparedUpsertRow {
            relation,
            shape: self.shape,
            binds: self.binds,
        })
    }
}

fn gsi_payload(
    table: &BoundTableInfo,
    row: Option<&storage_backfill::GsiProjectedRow>,
) -> StorageResult<(Vec<String>, Vec<SqlValue>)> {
    let count = table
        .max_indexers
        .as_usize()
        .checked_add(4)
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut columns = Vec::new();
    let mut binds = Vec::new();
    columns
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let slots = gsi_indexer_slots(table.max_indexers.as_usize(), row)?;
    let (tombstone, size, image) = match row {
        Some(row) => {
            let image = row.projected_image()?;
            let size = SqlValue::Integer(i64::from(image.logical_size()?));
            let declaration = storage_types::IndexerDeclaration::try_new(
                Vec::new(),
                storage_types::MaxIndexers::ZERO,
            )?;
            let value = storage_types::MaterializedItemRecord::encode(
                table.history_id,
                &image,
                &declaration,
            )?;
            (false, size, value)
        }
        None => (true, SqlValue::Null, Vec::new()),
    };
    for name in [
        "__aux_tombstone",
        "__aux_item_version",
        "__aux_evaluated_bytes",
        "item_value",
    ] {
        columns.push(crate::statement::SqlText::render(format_args!("{name}"))?);
    }
    binds.extend([
        SqlValue::Integer(i64::from(tombstone)),
        SqlValue::Null,
        size,
        SqlValue::Blob(image),
    ]);
    for (ordinal, slot) in slots.into_iter().enumerate() {
        columns.push(crate::statement::SqlText::render(format_args!(
            "__aux_indexer_{ordinal}"
        ))?);
        binds.push(slot);
    }
    Ok((columns, binds))
}

fn gsi_indexer_slots(
    maximum: usize,
    row: Option<&storage_backfill::GsiProjectedRow>,
) -> StorageResult<Vec<SqlValue>> {
    let mut slots = Vec::new();
    let count = maximum.max(row.map_or(0, |row| row.indexers.names().len()));
    slots
        .try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    if let Some(row) = row {
        let mut fields = Vec::new();
        fields
            .try_reserve_exact(row.indexers.names().len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        fields.extend(row.indexers.names().iter().map(String::as_str));
        for value in row.source_item.select_attribute_values(&fields)? {
            slots.push(indexer_slot(value)?);
        }
    }
    slots.resize_with(maximum, || SqlValue::Null);
    Ok(slots)
}

fn indexer_slot(value: Option<AttributeValue>) -> StorageResult<SqlValue> {
    match value {
        Some(AttributeValue::S(value)) => Ok(SqlValue::Text(value)),
        None => Ok(SqlValue::Null),
        _ => Err(StorageError::internal("SQL GSI source has invalid indexer")),
    }
}
