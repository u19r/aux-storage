//! SQL new-image lowering before writer admission; revisions remain protected.

use std::borrow::Cow;

use storage_provider::{
    AcceptedUpsert, BoundPutItem, ItemPrimaryEffects, PreparedWriteAttempt, UpsertImage,
};
use storage_types::{
    AttributeName, AttributeValue, BoundTableInfo, KeyAttributeType, KeyAttributes,
    KeySchemaElement, StorageError, StorageResult, TableHistoryId, planned_key_columns,
};

use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlStatement, SqlValue,
    catalog::SqlCatalogAttempt,
    gsi::PreparedGsiWrite,
    item_read::encode_key_value,
    item_tombstone::sql_statements::delete_tombstone_statement,
    write_batch::{DeleteShape, PreparedDeleteRow, PreparedUpsertRow, UpsertShape},
};

#[derive(Default)]
pub struct SqlPreparedWrites {
    pub(crate) primary: Vec<PreparedPrimaryWrite>,
    pub(crate) gsi: Vec<PreparedGsiWrite>,
}

impl SqlPreparedWrites {
    pub(crate) fn try_new(engine: impl SqlLayout, items: &[&BoundPutItem]) -> StorageResult<Self> {
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
                .push(PreparedPrimaryWrite::try_new(engine, (*item).into())?);
            for row in item.gsi_puts() {
                prepared.gsi.push(PreparedGsiWrite::try_new(
                    engine,
                    &item.table,
                    &row.key,
                    Some(row),
                )?);
            }
        }
        Ok(prepared)
    }
}

impl<A: SqlAttempt, L: SqlLayout> PreparedWriteAttempt for SqlCatalogAttempt<'_, A, L> {
    type PreparedWrites = SqlPreparedWrites;
    fn install_prepared_writes(&mut self, writes: StorageResult<Self::PreparedWrites>) {
        if let Err(error) = &writes {
            self.attempt.reject_settlement_error(error);
        }
        self.prepared_writes = writes;
    }

    fn prepare_put_items(&mut self, items: &[&BoundPutItem]) -> StorageResult<()> {
        let mut operation =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(self);
        let catalog = operation.as_mut();
        catalog.prepared_writes = Ok(Default::default());
        let result = SqlPreparedWrites::try_new(catalog.engine, items).map(|writes| {
            catalog.prepared_writes = Ok(writes);
        });
        operation.complete(result)
    }
}

pub(crate) struct PreparedPrimaryWrite {
    history_id: TableHistoryId,
    key: KeyAttributes,
    relation: String,
    shape: UpsertShape,
    binds: Vec<SqlValue>,
    revision_bind: usize,
    tombstone: SqlStatement,
    image: Vec<u8>,
}

impl PreparedPrimaryWrite {
    pub(crate) fn try_new(engine: impl SqlLayout, item: UpsertImage<'_>) -> StorageResult<Self> {
        let image = storage_types::MaterializedItemRecord::encode(
            item.table.history_id,
            item.new_item,
            item.declaration,
        )?;
        let history_id = item.table.history_id;
        let key = item.key.clone();
        let tombstone = delete_tombstone_statement(engine, item.table, item.key)?;
        let (relation, shape, binds, revision_bind) = encode_primary_upsert(engine, item)?;
        Ok(Self {
            history_id,
            key,
            relation,
            shape,
            binds,
            revision_bind,
            tombstone,
            image,
        })
    }

    pub(crate) fn matches(&self, item: &AcceptedUpsert) -> bool {
        self.history_id == item.table.history_id && self.key == item.key
    }

    pub(crate) fn prepare(
        self,
        effects: &ItemPrimaryEffects,
    ) -> StorageResult<(PreparedUpsertRow, SqlStatement)> {
        let image = self.image;
        let mut binds = self.binds;
        binds[self.revision_bind + 4] = SqlValue::Blob(image);
        binds[self.revision_bind] = SqlValue::Integer(i64::try_from(effects.revision())?);
        binds[self.revision_bind + 1] = SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
            effects.provenance().as_bytes(),
        ))?;
        binds[self.revision_bind + 2] = effects
            .ttl_epoch()
            .map_or(SqlValue::Null, SqlValue::Integer);
        Ok((
            PreparedUpsertRow {
                relation: self.relation,
                shape: self.shape,
                binds,
            },
            self.tombstone,
        ))
    }
}

fn encode_primary_upsert(
    engine: impl SqlLayout,
    item: storage_provider::UpsertImage<'_>,
) -> StorageResult<(String, UpsertShape, Vec<SqlValue>, usize)> {
    let table = &item.table.info;
    let schemas = table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|index| index.key_schema.as_slice());
    let columns = planned_key_columns(&table.key_schema, schemas, &table.attribute_definitions);
    let field_count = columns
        .len()
        .checked_add(item.declaration.names().len())
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut fields = Vec::new();
    fields
        .try_reserve_exact(field_count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    fields.extend(
        columns
            .iter()
            .map(|column| column.attribute_name.as_str())
            .chain(item.declaration.names().iter().map(String::as_str)),
    );
    let count = columns
        .len()
        .checked_add(5)
        .and_then(|count| count.checked_add(table.max_indexers.as_usize()))
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut row = InsertRow::try_new(engine, count)?;
    let attributes = item.new_item.project_attributes(&fields)?;
    row.append_item_keys(&item, columns, &attributes)?;
    let revision_bind = row.binds.len();
    for column in ["__aux_revision", "__aux_provenance", "__aux_ttl_epoch"] {
        row.push(column.into(), None, SqlValue::Null);
    }
    row.append_item_payload(&item, &attributes)?;
    let physical = engine.item_table_name(item.table.history_id);
    let (relation, shape, binds) = row.finish(engine, &physical, &table.key_schema)?;
    Ok((relation, shape, binds, revision_bind))
}

pub(crate) fn encode_primary_delete(
    engine: impl SqlLayout,
    table: &BoundTableInfo,
    key: &KeyAttributes,
) -> StorageResult<PreparedDeleteRow> {
    let key_elements = KeySchemaElement::ordered(&table.info.key_schema).collect::<Vec<_>>();
    let mut columns = Vec::new();
    let mut numeric_columns = Vec::new();
    let mut binds = Vec::new();
    columns
        .try_reserve_exact(key_elements.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    numeric_columns
        .try_reserve_exact(key_elements.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    binds
        .try_reserve_exact(key_elements.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for element in key_elements {
        let column = AttributeName::new(&element.attribute_name);
        columns.push(column.sanitized().to_owned());
        let key_type = table
            .info
            .attribute_definitions
            .iter()
            .find(|definition| definition.attribute_name == element.attribute_name)
            .map_or(KeyAttributeType::S, |definition| definition.attribute_type);
        numeric_columns
            .push(engine.engine() == SqlEngine::Postgres && key_type == KeyAttributeType::N);
        let value = key
            .get(&element.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        binds.push(encode_key_value(engine, value)?);
    }
    Ok(PreparedDeleteRow {
        relation: engine.item_table_name(table.history_id),
        shape: DeleteShape::try_new(columns, numeric_columns)?,
        binds,
    })
}

struct InsertRow<L: SqlLayout> {
    engine: L,
    columns: Vec<Cow<'static, str>>,
    key_types: Vec<Option<KeyAttributeType>>,
    binds: Vec<SqlValue>,
}

impl<L: SqlLayout> InsertRow<L> {
    fn try_new(engine: L, count: usize) -> StorageResult<Self> {
        let mut row = Self {
            engine,
            columns: Vec::new(),
            key_types: Vec::new(),
            binds: Vec::new(),
        };
        row.columns
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        row.key_types
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        row.binds
            .try_reserve_exact(count)
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        Ok(row)
    }

    fn append_item_keys(
        &mut self,
        item: &storage_provider::UpsertImage<'_>,
        columns: Vec<storage_types::PlannedKeyColumn>,
        attributes: &storage_types::AttributeMap,
    ) -> StorageResult<()> {
        for column in columns {
            let value = item.key_column_value(&column, attributes);
            let bind = value
                .map(|value| encode_key_value(self.engine, value))
                .transpose()?
                .unwrap_or(SqlValue::Null);
            self.push(
                crate::statement::SqlText::render(format_args!(
                    "{}",
                    AttributeName::new(&column.attribute_name).sanitized()
                ))?
                .into(),
                Some(column.attribute_type),
                bind,
            );
        }
        Ok(())
    }

    fn append_item_payload(
        &mut self,
        item: &storage_provider::UpsertImage<'_>,
        attributes: &storage_types::AttributeMap,
    ) -> StorageResult<()> {
        let table = &item.table.info;
        self.push(
            "__aux_evaluated_bytes".into(),
            None,
            SqlValue::Integer(i64::from(item.item_size)),
        );
        self.push("item_value".into(), None, SqlValue::Null);
        for ordinal in 0..table.max_indexers.as_usize() {
            let value = match item
                .declaration
                .names()
                .get(ordinal)
                .and_then(|name| attributes.get(name))
            {
                Some(AttributeValue::S(value)) => {
                    SqlValue::try_from(crate::row_view::SqlValueRef::Text(value))?
                }
                None => SqlValue::Null,
                _ => {
                    return Err(StorageError::internal(
                        "accepted SQL item has invalid indexer",
                    ));
                }
            };
            self.push(
                crate::statement::SqlText::render(format_args!("__aux_indexer_{ordinal}"))?.into(),
                None,
                value,
            );
        }
        Ok(())
    }

    fn push(
        &mut self,
        name: Cow<'static, str>,
        key_type: Option<KeyAttributeType>,
        value: SqlValue,
    ) {
        self.columns.push(name);
        self.key_types.push(key_type);
        self.binds.push(value);
    }

    fn finish(
        self,
        engine: L,
        physical: &str,
        key_schema: &[KeySchemaElement],
    ) -> StorageResult<(String, UpsertShape, Vec<SqlValue>)> {
        let conflict_columns = KeySchemaElement::ordered(key_schema)
            .map(|key| {
                AttributeName::new(&key.attribute_name)
                    .sanitized()
                    .to_owned()
            })
            .collect::<Vec<_>>();
        let numeric_columns = self
            .key_types
            .iter()
            .map(|key_type| {
                engine.engine() == SqlEngine::Postgres && *key_type == Some(KeyAttributeType::N)
            })
            .collect::<Vec<_>>();
        let columns = self
            .columns
            .into_iter()
            .map(Cow::into_owned)
            .collect::<Vec<_>>();
        let shape = UpsertShape::try_new(columns, numeric_columns, conflict_columns, false)?;
        Ok((physical.to_owned(), shape, self.binds))
    }
}
