//! SQL new-image lowering before writer admission; revisions remain protected.

use std::borrow::Cow;

use storage_provider::{
    AcceptedUpsert, BoundPutItem, ItemPrimaryEffects, PreparedWriteAttempt, UpsertImage,
};
use storage_types::{
    AttributeName, AttributeValue, KeyAttributeType, KeyAttributes, KeySchemaElement, StorageError,
    StorageResult, TableHistoryId, planned_key_columns,
};

use crate::{
    SqlAttempt, SqlEngine, SqlLayout, SqlStatement, SqlValue, catalog::SqlCatalogAttempt,
    gsi::PreparedGsiWrite, item_read::encode_key_value,
    item_tombstone::sql_statements::delete_tombstone_statement,
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
    statement: SqlStatement,
    revision_bind: usize,
    tombstone: SqlStatement,
    image: stream_provider::retained_image::AcceptedImagePlan,
    owner: stream_provider::retained_image::PrimaryImageOwner,
}

impl PreparedPrimaryWrite {
    pub(crate) fn try_new(engine: impl SqlLayout, item: UpsertImage<'_>) -> StorageResult<Self> {
        let owner =
            stream_provider::retained_image::PrimaryImageOwner::try_new(item.table, item.key)?;
        let image = stream_provider::retained_image::AcceptedImagePlan::try_new(
            item.table.history_id,
            item.new_item,
            item.declaration,
        )?;
        let history_id = item.table.history_id;
        let key = item.key.clone();
        let tombstone = delete_tombstone_statement(engine, item.table, item.key)?;
        let (statement, revision_bind) = encode_primary_upsert(engine, item)?;
        Ok(Self {
            history_id,
            key,
            statement,
            revision_bind,
            tombstone,
            image,
            owner,
        })
    }

    pub(crate) fn matches(&self, item: &AcceptedUpsert) -> bool {
        self.history_id == item.table.history_id && self.key == item.key
    }

    pub(crate) async fn prepare<A: stream_provider::retained_image::RetainedOwnershipAttempt>(
        self,
        attempt: &mut A,
        effects: &ItemPrimaryEffects,
        previous: Option<&storage_types::VerifiedWireItem>,
    ) -> StorageResult<(SqlStatement, SqlStatement)> {
        let image = self.image.prepare(attempt).await?;
        self.owner.replace(attempt, previous, &image).await?;
        let image = image.encode()?;
        let (sql, mut binds) = self.statement.into_parts();
        binds[self.revision_bind + 4] = SqlValue::Blob(image);
        binds[self.revision_bind] = SqlValue::Integer(i64::try_from(effects.revision())?);
        binds[self.revision_bind + 1] = SqlValue::try_from(crate::row_view::SqlValueRef::Blob(
            effects.provenance().as_bytes(),
        ))?;
        binds[self.revision_bind + 2] = effects
            .ttl_epoch()
            .map_or(SqlValue::Null, SqlValue::Integer);
        Ok((SqlStatement::new(sql, binds), self.tombstone))
    }
}

fn encode_primary_upsert(
    engine: impl SqlLayout,
    item: storage_provider::UpsertImage<'_>,
) -> StorageResult<(SqlStatement, usize)> {
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
        .checked_add(6)
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
    let physical = engine.item_table_name(&table.table_name, item.table.history_id);
    Ok((row.finish(&physical, &table.key_schema)?, revision_bind))
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
        self.push("item_descriptor".into(), None, SqlValue::Null);
        self.push(
            "__aux_indexers".into(),
            None,
            SqlValue::Text(serde_json::to_string(item.declaration.names())?),
        );
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
        physical: &str,
        key_schema: &[KeySchemaElement],
    ) -> StorageResult<SqlStatement> {
        let mut sql = crate::statement::SqlText::default();
        sql.append(format_args!("INSERT INTO \"{physical}\" ("))?;
        sql.append_columns(&self.columns)?;
        sql.append(format_args!(") VALUES ("))?;
        for (ordinal, key_type) in self.key_types.iter().enumerate() {
            sql.append(format_args!("{}", if ordinal == 0 { "" } else { ", " }))?;
            sql.append_parameter(
                self.engine.engine(),
                ordinal + 1,
                self.engine.engine() == SqlEngine::Postgres
                    && *key_type == Some(KeyAttributeType::N),
            )?;
        }
        sql.append(format_args!(") ON CONFLICT ("))?;
        for (ordinal, key) in KeySchemaElement::ordered(key_schema).enumerate() {
            sql.append(format_args!(
                "{}\"{}\"",
                if ordinal == 0 { "" } else { ", " },
                AttributeName::new(&key.attribute_name).sanitized()
            ))?;
        }
        sql.append(format_args!(") DO UPDATE SET "))?;
        sql.append_assignments(self.columns.iter().skip(key_schema.len()))?;
        Ok(SqlStatement::new(sql.finish()?, self.binds))
    }
}
