//! Apply shared projection decisions to SQL GSI rows.

use storage_backfill::{GsiBuildTarget, GsiProjectionWriteBatch};
use storage_provider::{GsiProjectionOwner, GsiWriteSource};
use storage_types::{AttributeValue, BoundTableInfo, StorageError, StorageResult};

use super::sql_statements::{BoundProjectionKey, GsiUpsertSql, render_gsi_upsert};
use crate::{SqlAttempt, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt};

pub(crate) struct GsiWriteInput<'a> {
    pub table: &'a BoundTableInfo,
    pub source: GsiWriteSource<'a>,
    pub writes: GsiProjectionWriteBatch,
    pub targets: &'a [GsiBuildTarget],
    pub prepared: &'a mut Vec<PreparedGsiWrite>,
}

pub(crate) async fn prepare_gsi_writes<A: SqlAttempt, L: SqlLayout>(
    catalog: &mut SqlCatalogAttempt<'_, A, L>,
    input: GsiWriteInput<'_>,
) -> StorageResult<Vec<SqlStatement>> {
    let mut operation =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(catalog);
    let catalog = operation.as_mut();
    let result = async {
        let engine = catalog.engine;
        let mut rows = Vec::new();
        let writes = input.writes.into_iter();
        rows.try_reserve_exact(writes.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for write in writes {
            input.source.validate(&write)?;
            let prepared = if write.row.is_some() {
                input.prepared.iter().position(|put| {
                    put.history_id == input.table.history_id && put.key == write.key
                })
            } else {
                None
            };
            let prepared = match prepared {
                Some(position) => input.prepared.swap_remove(position),
                None => {
                    PreparedGsiWrite::try_new(engine, input.table, &write.key, write.row.as_ref())?
                }
            };
            rows.push(prepared.bind(engine, input.table, write.item_version, input.targets)?);
        }
        let previous = BoundGsiWrite::load_descriptors(catalog, input.table, &rows).await?;
        BoundGsiWrite::prepare_batch(catalog, rows, previous).await
    }
    .await;
    operation.complete(result)
}

/// Keys, payload binds and SQL syntax prepared without a lifecycle observation.
pub(crate) struct PreparedGsiWrite {
    pub(crate) history_id: storage_types::TableHistoryId,
    pub(crate) key: storage_backfill::GsiProjectionKey,
    sql: GsiUpsertSql,
    binds: Vec<SqlValue>,
    revision_bind: usize,
    image: Option<stream_provider::retained_image::AcceptedProjectionPlan>,
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
        let (payload_columns, payload_binds, image) = gsi_payload(table, row)?;
        columns
            .try_reserve_exact(payload_columns.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        binds
            .try_reserve_exact(payload_binds.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        columns.extend(payload_columns);
        binds.extend(payload_binds);
        let sql = render_gsi_upsert(engine, &columns, key_count, &numeric_keys)?;
        Ok(Self {
            history_id: table.history_id,
            key: key.clone(),
            sql,
            binds,
            revision_bind: key_count + 1,
            image,
        })
    }

    fn bind(
        mut self,
        engine: impl SqlLayout,
        table: &BoundTableInfo,
        revision: storage_types::ItemRevision,
        targets: &[GsiBuildTarget],
    ) -> StorageResult<BoundGsiWrite> {
        let target = targets
            .iter()
            .find(|target| {
                target.table_name == table.table_name && target.index_name == self.key.index_name
            })
            .ok_or_else(|| StorageError::internal("SQL GSI write target was not bound"))?;
        let physical =
            engine.gsi_table_name(&table.table_name, &self.key.index_name, &target.incarnation);
        self.binds[self.revision_bind] = SqlValue::Integer(i64::try_from(revision)?);
        let owner = GsiProjectionOwner::try_new(table, &self.key, &target.incarnation)?;
        Ok(BoundGsiWrite {
            owner,
            key: self.key,
            target: target.clone(),
            statement: SqlStatement::new(self.sql.bind_relation(&physical)?, self.binds),
            image: self.image,
            image_bind: self.revision_bind + 2,
        })
    }
}

struct BoundGsiWrite {
    owner: GsiProjectionOwner,
    key: storage_backfill::GsiProjectionKey,
    target: GsiBuildTarget,
    statement: SqlStatement,
    image: Option<stream_provider::retained_image::AcceptedProjectionPlan>,
    image_bind: usize,
}

impl BoundGsiWrite {
    async fn load_descriptors<A: SqlAttempt, L: SqlLayout>(
        catalog: &mut SqlCatalogAttempt<'_, A, L>,
        table: &BoundTableInfo,
        rows: &[Self],
    ) -> StorageResult<Vec<Option<stream_provider::retained_image::RetainedProjectionRecord>>> {
        let mut groups: Vec<(&GsiBuildTarget, Vec<usize>)> = Vec::new();
        groups
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (slot, row) in rows.iter().enumerate() {
            match groups.iter_mut().find(|(target, _)| **target == row.target) {
                Some((_, slots)) => {
                    slots.try_reserve(1).map_err(|_| {
                        storage_types::InfrastructureResultKind::NativeCapacityRejected
                    })?;
                    slots.push(slot);
                }
                None => {
                    let mut slots = Vec::new();
                    slots.try_reserve_exact(1).map_err(|_| {
                        storage_types::InfrastructureResultKind::NativeCapacityRejected
                    })?;
                    slots.push(slot);
                    groups.push((&row.target, slots));
                }
            }
        }
        let count = groups
            .iter()
            .try_fold(0usize, |count, (_, slots)| {
                count.checked_add(
                    slots
                        .len()
                        .div_ceil(crate::constants::GSI_PROJECTION_STATE_READ_ROWS),
                )
            })
            .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        let mut statements = Vec::new();
        let mut slots = Vec::new();
        let mut previous = Vec::new();
        statements
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        slots
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        previous
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        previous.resize_with(rows.len(), || None);
        for (target, group) in &groups {
            let index = table
                .global_secondary_indexes
                .as_deref()
                .unwrap_or_default()
                .iter()
                .find(|index| index.index_name == target.index_name)
                .ok_or_else(|| StorageError::internal("GSI descriptor index is absent"))?;
            for chunk in group.chunks(crate::constants::GSI_PROJECTION_STATE_READ_ROWS) {
                let mut keys = Vec::new();
                keys.try_reserve_exact(chunk.len())
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                keys.extend(chunk.iter().map(|slot| &rows[*slot].key));
                statements.push(super::sql_statements::projection_descriptor_statement(
                    catalog.engine,
                    super::sql_statements::ProjectionStateQuery {
                        table: &table.info,
                        index,
                        keys: &keys,
                        incarnation: &target.incarnation,
                    },
                )?);
                slots.push(chunk);
            }
        }
        if statements.is_empty() {
            return Ok(Vec::new());
        }
        let pages = catalog
            .attempt
            .query_batch(
                statements,
                crate::ReadProtection::Snapshot,
                storage_provider::CATALOG_READ_BYTES,
            )
            .await?;
        if pages.len() != slots.len() {
            return Err(StorageError::internal("GSI descriptor page count differs"));
        }
        for (slots, page) in slots.into_iter().zip(pages) {
            decode_descriptors(table, slots, page, &mut previous)?;
        }
        Ok(previous)
    }

    async fn prepare_batch<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        attempt: &mut A,
        mut rows: Vec<Self>,
        previous: Vec<Option<stream_provider::retained_image::RetainedProjectionRecord>>,
    ) -> StorageResult<Vec<SqlStatement>> {
        let mut plans = Vec::new();
        let mut slots = Vec::new();
        let mut transitions = Vec::new();
        let mut statements = Vec::new();
        plans
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        slots
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        transitions
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        statements
            .try_reserve_exact(rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (slot, row) in rows.iter_mut().enumerate() {
            if let Some(image) = row.image.take() {
                plans.push(image);
                slots.push(slot);
            }
        }
        let images =
            stream_provider::retained_image::AcceptedProjectionPlan::prepare_batch(attempt, plans)
                .await?;
        if images.len() != slots.len() {
            return Err(StorageError::internal(
                "GSI projection descriptor count differs",
            ));
        }
        if previous.len() != rows.len() {
            return Err(StorageError::internal(
                "GSI previous descriptor count differs",
            ));
        }
        let mut images = slots.into_iter().zip(images).peekable();
        for (slot, (row, previous)) in rows.into_iter().zip(previous).enumerate() {
            let (sql, mut binds) = row.statement.into_parts();
            let mut next = None;
            if images.peek().is_some_and(|(next, _)| *next == slot) {
                let (_, image) = images
                    .next()
                    .ok_or_else(|| StorageError::internal("Missing GSI projection descriptor"))?;
                binds[row.image_bind] = SqlValue::Blob(image.encode()?);
                next = Some(image);
            }
            transitions.push(
                row.owner
                    .prepare_transition(previous.as_ref(), next.as_ref())?,
            );
            statements.push(SqlStatement::new(sql, binds));
        }
        if images.next().is_some() {
            return Err(StorageError::internal("Unused GSI projection descriptor"));
        }
        stream_provider::retained_image::RetainedOwnerTransition::prepare_batch(
            attempt,
            transitions,
        )
        .await?;
        Ok(statements)
    }
}

fn gsi_payload(
    table: &BoundTableInfo,
    row: Option<&storage_backfill::GsiProjectedRow>,
) -> StorageResult<(
    Vec<String>,
    Vec<SqlValue>,
    Option<stream_provider::retained_image::AcceptedProjectionPlan>,
)> {
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
        Some(row) => (
            false,
            SqlValue::Integer(i64::from(row.projected_image()?.logical_size()?)),
            Some(
                stream_provider::retained_image::AcceptedProjectionPlan::try_new(
                    table.history_id,
                    &row.source_item,
                    &row.projection,
                )?,
            ),
        ),
        None => (true, SqlValue::Null, None),
    };
    for name in [
        "__aux_tombstone",
        "__aux_item_version",
        "__aux_evaluated_bytes",
        "item_descriptor",
    ] {
        columns.push(crate::statement::SqlText::render(format_args!("{name}"))?);
    }
    binds.extend([
        SqlValue::Integer(i64::from(tombstone)),
        SqlValue::Null,
        size,
        if tombstone {
            SqlValue::Blob(Vec::new())
        } else {
            SqlValue::Null
        },
    ]);
    for (ordinal, slot) in slots.into_iter().enumerate() {
        columns.push(crate::statement::SqlText::render(format_args!(
            "__aux_indexer_{ordinal}"
        ))?);
        binds.push(slot);
    }
    Ok((columns, binds, image))
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

fn decode_descriptors(
    table: &BoundTableInfo,
    slots: &[usize],
    page: crate::SqlQueryPage,
    previous: &mut [Option<stream_provider::retained_image::RetainedProjectionRecord>],
) -> StorageResult<()> {
    if page.more || page.rows.len() != slots.len() {
        return Err(StorageError::internal("GSI descriptor page is incomplete"));
    }
    let mut seen = Vec::new();
    seen.try_reserve_exact(slots.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    seen.resize(slots.len(), false);
    for row in page.rows {
        let [ordinal, tombstone, revision, descriptor]: [SqlValue; 4] = row
            .into_values()
            .try_into()
            .map_err(|_| StorageError::internal("GSI descriptor row width differs"))?;
        let SqlValue::Integer(ordinal) = ordinal else {
            return Err(StorageError::internal("GSI descriptor ordinal is invalid"));
        };
        let ordinal = usize::try_from(ordinal)
            .map_err(|_| StorageError::internal("Negative GSI descriptor ordinal"))?;
        let seen = seen
            .get_mut(ordinal)
            .ok_or_else(|| StorageError::internal("GSI descriptor ordinal is outside page"))?;
        if std::mem::replace(seen, true) {
            return Err(StorageError::internal(
                "GSI descriptor ordinal is duplicated",
            ));
        }
        let record = match (tombstone, revision, descriptor) {
            (SqlValue::Null, SqlValue::Null, SqlValue::Null) => None,
            (SqlValue::Integer(1), SqlValue::Integer(revision), SqlValue::Blob(bytes))
                if bytes.is_empty() =>
            {
                storage_types::ItemRevision::try_from(revision)?;
                None
            }
            (SqlValue::Integer(0), SqlValue::Integer(revision), SqlValue::Blob(bytes)) => {
                storage_types::ItemRevision::try_from(revision)?;
                Some(
                    stream_provider::retained_image::RetainedProjectionRecord::decode(
                        &bytes,
                        table.history_id,
                        crate::constants::RANGE_PAGE_READ_BYTES.get(),
                    )?,
                )
            }
            _ => {
                return Err(StorageError::internal(
                    "GSI stored descriptor state is invalid",
                ));
            }
        };
        previous[slots[ordinal]] = record;
    }
    Ok(())
}
