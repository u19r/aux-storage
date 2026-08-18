use std::{borrow::Cow, collections::HashMap};

use storage_common::{GsiKeyPart, GsiWriteAction, plan_gsi_write_actions};
use storage_types::{
    AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType, KeySchemaElement,
    StorageError, StorageResult, StoredTableInfo, TableName,
};

use crate::write_plan::{WriteMaintenancePlan, WriteStatement};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum GsiUpsertStyle {
    InsertOrReplace,
    OnConflictUpdate,
    OnConflictUpdateNonKey,
    OnConflictUpdateReturning,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum TableKeyColumnStyle {
    FixedPkSk,
    PrefixedAttributeNames,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum PlaceholderNumbering {
    PerStatement,
    AcrossPlan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum GsiAttributesBlobStyle {
    FullProjectedItem,
    NonKeyAttributes,
}

pub(crate) struct GsiSqlPlanOptions<'a, P, Name, Param, Null, Placeholder, Column>
where
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    pub(crate) physical_gsi_name: Name,
    pub(crate) scalar_param: Param,
    pub(crate) null_param: Null,
    pub(crate) placeholder: Placeholder,
    pub(crate) column_name: Column,
    pub(crate) upsert_style: GsiUpsertStyle,
    pub(crate) table_key_column_style: TableKeyColumnStyle,
    pub(crate) placeholder_numbering: PlaceholderNumbering,
    pub(crate) attributes_blob_style: GsiAttributesBlobStyle,
    pub(crate) _marker: std::marker::PhantomData<&'a P>,
}

impl<'a, P, Name, Param, Null, Placeholder, Column>
    GsiSqlPlanOptions<'a, P, Name, Param, Null, Placeholder, Column>
where
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        physical_gsi_name: Name,
        scalar_param: Param,
        null_param: Null,
        placeholder: Placeholder,
        column_name: Column,
        upsert_style: GsiUpsertStyle,
        table_key_column_style: TableKeyColumnStyle,
        placeholder_numbering: PlaceholderNumbering,
        attributes_blob_style: GsiAttributesBlobStyle,
    ) -> Self {
        Self {
            physical_gsi_name,
            scalar_param,
            null_param,
            placeholder,
            column_name,
            upsert_style,
            table_key_column_style,
            placeholder_numbering,
            attributes_blob_style,
            _marker: std::marker::PhantomData,
        }
    }
}

pub(crate) fn plan_gsi_sql_statements<P, Name, Param, Null, Placeholder, Column>(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, AttributeValue>>,
    new_item: Option<&HashMap<String, AttributeValue>>,
    new_indexers: &[String],
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
) -> StorageResult<WriteMaintenancePlan<P>>
where
    P: Clone,
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    let actions = plan_gsi_write_actions(table_info, old_item, new_item)?;
    let mut plan = WriteMaintenancePlan::with_capacity(actions.len());
    let mut parameter_offset = 0usize;

    for action in actions {
        let statement = match action {
            GsiWriteAction::Delete {
                index,
                gsi_key,
                table_key,
            } => plan_gsi_delete_statement(
                table_info,
                index,
                &gsi_key,
                &table_key,
                &mut parameter_offset,
                options,
            )?,
            GsiWriteAction::Put {
                index,
                gsi_key,
                table_key,
                projected_item,
            } => plan_gsi_put_statement(
                table_info,
                index,
                &gsi_key,
                &table_key,
                &projected_item,
                new_item.ok_or_else(|| {
                    StorageError::internal("GSI put plan is missing its logical item")
                })?,
                new_indexers,
                &mut parameter_offset,
                options,
            )?,
        };
        plan.push(statement);
    }

    Ok(plan)
}

fn plan_gsi_delete_statement<P, Name, Param, Null, Placeholder, Column>(
    table_info: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    gsi_key: &[GsiKeyPart<'_>],
    table_key: &[GsiKeyPart<'_>],
    parameter_offset: &mut usize,
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
) -> StorageResult<WriteStatement<P>>
where
    P: Clone,
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    let mut bindings = Vec::new();
    push_key_bindings(
        &mut bindings,
        table_info,
        &index.key_schema,
        gsi_key,
        None,
        options,
    )?;
    push_table_key_bindings(&mut bindings, table_info, table_key, false, options)?;

    let mut params = Vec::with_capacity(bindings.len());
    let where_clause = bindings
        .iter()
        .enumerate()
        .map(|(binding_index, binding)| {
            params.push(binding.value.clone());
            let placeholder_index = placeholder_index(
                binding_index + 1,
                *parameter_offset,
                options.placeholder_numbering,
            );
            format!(
                "{} = {}",
                binding.column,
                (options.placeholder)(placeholder_index, binding.attribute_type.as_ref())
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    advance_offset(
        parameter_offset,
        params.len(),
        options.placeholder_numbering,
    );

    let table_name = (options.physical_gsi_name)(&table_info.table_name, &index.index_name);
    let suffix = if matches!(
        options.upsert_style,
        GsiUpsertStyle::OnConflictUpdateReturning
    ) {
        " RETURNING 1"
    } else {
        ""
    };
    Ok(WriteStatement::new(
        format!("DELETE FROM \"{table_name}\" WHERE {where_clause}{suffix}"),
        params,
    ))
}

#[allow(clippy::too_many_arguments)]
fn plan_gsi_put_statement<P, Name, Param, Null, Placeholder, Column>(
    table_info: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    gsi_key: &[GsiKeyPart<'_>],
    table_key: &[GsiKeyPart<'_>],
    projected_item: &HashMap<String, AttributeValue>,
    logical_item: &HashMap<String, AttributeValue>,
    indexers: &[String],
    parameter_offset: &mut usize,
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
) -> StorageResult<WriteStatement<P>>
where
    P: Clone,
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    plan_gsi_put_statement_with_style(
        table_info,
        index,
        gsi_key,
        table_key,
        projected_item,
        logical_item,
        indexers,
        parameter_offset,
        options,
        options.upsert_style,
    )
}

#[allow(clippy::too_many_arguments)]
fn plan_gsi_put_statement_with_style<P, Name, Param, Null, Placeholder, Column>(
    table_info: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
    gsi_key: &[GsiKeyPart<'_>],
    table_key: &[GsiKeyPart<'_>],
    projected_item: &HashMap<String, AttributeValue>,
    logical_item: &HashMap<String, AttributeValue>,
    indexers: &[String],
    parameter_offset: &mut usize,
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
    upsert_style: GsiUpsertStyle,
) -> StorageResult<WriteStatement<P>>
where
    P: Clone,
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    let mut bindings = Vec::new();
    push_key_bindings(
        &mut bindings,
        table_info,
        &index.key_schema,
        gsi_key,
        None,
        options,
    )?;
    push_table_key_bindings(&mut bindings, table_info, table_key, true, options)?;

    let mut columns = bindings
        .iter()
        .map(|binding| binding.column.clone())
        .collect::<Vec<_>>();
    let key_columns = columns.clone();
    let mut params = bindings
        .iter()
        .map(|binding| binding.value.clone())
        .collect::<Vec<_>>();

    let payload = projected_attributes(
        projected_item,
        &index.key_schema,
        &table_info.key_schema,
        options.attributes_blob_style,
    );
    let indexed = crate::indexed_item::SqlIndexedItem::extract(
        logical_item,
        payload.as_ref(),
        Some(indexers),
        table_info.max_indexers,
    )?;
    columns.push("attributes_blob".to_string());
    params.push((options.scalar_param)(&AttributeValue::S(
        indexed.residual_json().to_owned(),
    ))?);
    for ordinal in 0..table_info.max_indexers.as_usize() {
        columns.push(crate::utils::indexer_column_name(ordinal));
        params.push(
            match indexed.slots().get(ordinal).and_then(Option::as_ref) {
                Some(value) => (options.scalar_param)(&AttributeValue::S(value.clone()))?,
                None => (options.null_param)(),
            },
        );
    }

    let placeholders = bindings
        .iter()
        .enumerate()
        .map(|(binding_index, binding)| {
            let placeholder_index = placeholder_index(
                binding_index + 1,
                *parameter_offset,
                options.placeholder_numbering,
            );
            (options.placeholder)(placeholder_index, binding.attribute_type.as_ref())
        })
        .chain((0..=table_info.max_indexers.as_usize()).map(|offset| {
            let placeholder_index = placeholder_index(
                bindings.len() + 1 + offset,
                *parameter_offset,
                options.placeholder_numbering,
            );
            (options.placeholder)(placeholder_index, None)
        }))
        .collect::<Vec<_>>()
        .join(", ");
    advance_offset(
        parameter_offset,
        params.len(),
        options.placeholder_numbering,
    );

    let table_name = (options.physical_gsi_name)(&table_info.table_name, &index.index_name);
    let sql = match upsert_style {
        GsiUpsertStyle::InsertOrReplace => format!(
            "INSERT OR REPLACE INTO \"{table_name}\" ({}) VALUES ({placeholders})",
            columns.join(", ")
        ),
        GsiUpsertStyle::OnConflictUpdate
        | GsiUpsertStyle::OnConflictUpdateNonKey
        | GsiUpsertStyle::OnConflictUpdateReturning => {
            let assignments = columns
                .iter()
                .filter(|column| {
                    !matches!(upsert_style, GsiUpsertStyle::OnConflictUpdateNonKey)
                        || !key_columns.contains(column)
                })
                .map(|column| format!("{column} = excluded.{column}"))
                .collect::<Vec<_>>()
                .join(", ");
            let returning = if matches!(upsert_style, GsiUpsertStyle::OnConflictUpdateReturning) {
                " RETURNING 1"
            } else {
                ""
            };
            format!(
                "INSERT INTO \"{table_name}\" ({}) VALUES ({placeholders}) ON CONFLICT ({}) DO \
                 UPDATE SET {assignments}{returning}",
                columns.join(", "),
                key_columns.join(", ")
            )
        }
    };

    Ok(WriteStatement::new(sql, params))
}

#[derive(Clone)]
struct Binding<P> {
    column: String,
    attribute_type: Option<KeyAttributeType>,
    value: P,
}

fn push_key_bindings<P, Name, Param, Null, Placeholder, Column>(
    bindings: &mut Vec<Binding<P>>,
    table_info: &StoredTableInfo,
    key_schema: &[KeySchemaElement],
    key_values: &[GsiKeyPart<'_>],
    column_prefix: Option<&str>,
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
) -> StorageResult<()>
where
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    for key in key_schema {
        let value = key_part_value(key_values, &key.attribute_name)?;
        bindings.push(Binding {
            column: (options.column_name)(&key.attribute_name, column_prefix),
            attribute_type: key_attribute_type(table_info, &key.attribute_name),
            value: (options.scalar_param)(value)?,
        });
    }
    Ok(())
}

fn push_table_key_bindings<P, Name, Param, Null, Placeholder, Column>(
    bindings: &mut Vec<Binding<P>>,
    table_info: &StoredTableInfo,
    table_key: &[GsiKeyPart<'_>],
    _include_missing_range_as_null: bool,
    options: &GsiSqlPlanOptions<'_, P, Name, Param, Null, Placeholder, Column>,
) -> StorageResult<()>
where
    Name: Fn(&TableName, &IndexName) -> String,
    Param: Fn(&AttributeValue) -> StorageResult<P>,
    Null: Fn() -> P,
    Placeholder: Fn(usize, Option<&KeyAttributeType>) -> String,
    Column: Fn(&str, Option<&str>) -> String,
{
    match options.table_key_column_style {
        TableKeyColumnStyle::FixedPkSk => {
            let hash = table_info
                .key_schema
                .iter()
                .find(|key| matches!(key.key_type, storage_types::KeyType::Hash))
                .ok_or_else(StorageError::invalid_or_missing_key)?;
            let hash_value = key_part_value(table_key, &hash.attribute_name)?;
            bindings.push(Binding {
                column: "table_pk".to_string(),
                attribute_type: Some(KeyAttributeType::S),
                value: (options.scalar_param)(hash_value)?,
            });

            let range = table_info
                .key_schema
                .iter()
                .find(|key| matches!(key.key_type, storage_types::KeyType::Range));
            if let Some(range) = range {
                let range_value = key_part_value(table_key, &range.attribute_name)?;
                bindings.push(Binding {
                    column: "table_sk".to_string(),
                    attribute_type: Some(KeyAttributeType::S),
                    value: (options.scalar_param)(range_value)?,
                });
            }
            Ok(())
        }
        TableKeyColumnStyle::PrefixedAttributeNames => push_key_bindings(
            bindings,
            table_info,
            &table_info.key_schema,
            table_key,
            Some("table_"),
            options,
        ),
    }
}

fn key_part_value<'a>(
    parts: &'a [GsiKeyPart<'_>],
    attribute_name: &str,
) -> StorageResult<&'a AttributeValue> {
    parts
        .iter()
        .find(|part| part.name == attribute_name)
        .map(|part| part.value)
        .ok_or_else(StorageError::invalid_or_missing_key)
}

pub(super) fn projected_attributes<'a>(
    projected_item: &'a HashMap<String, AttributeValue>,
    gsi_key_schema: &[KeySchemaElement],
    table_key_schema: &[KeySchemaElement],
    style: GsiAttributesBlobStyle,
) -> Cow<'a, HashMap<String, AttributeValue>> {
    if matches!(style, GsiAttributesBlobStyle::FullProjectedItem) {
        return Cow::Borrowed(projected_item);
    }

    let attributes = projected_item
        .iter()
        .filter(|(name, _)| !is_projected_key_attribute(name, gsi_key_schema, table_key_schema))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    Cow::Owned(attributes)
}

fn is_projected_key_attribute(
    name: &str,
    gsi_key_schema: &[KeySchemaElement],
    table_key_schema: &[KeySchemaElement],
) -> bool {
    gsi_key_schema
        .iter()
        .chain(table_key_schema.iter())
        .any(|key| key.attribute_name == name)
}

fn key_attribute_type(
    table_info: &StoredTableInfo,
    attribute_name: &str,
) -> Option<KeyAttributeType> {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == attribute_name)
        .map(|definition| definition.attribute_type.clone())
}

fn placeholder_index(
    statement_index: usize,
    parameter_offset: usize,
    numbering: PlaceholderNumbering,
) -> usize {
    match numbering {
        PlaceholderNumbering::PerStatement => statement_index,
        PlaceholderNumbering::AcrossPlan => parameter_offset + statement_index,
    }
}

fn advance_offset(offset: &mut usize, count: usize, numbering: PlaceholderNumbering) {
    if matches!(numbering, PlaceholderNumbering::AcrossPlan) {
        *offset += count;
    }
}
