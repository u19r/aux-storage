use std::{collections::HashMap, time::Instant};

use deadpool_postgres::GenericClient;
#[cfg(test)]
use storage_common::provider_perf;
use storage_types::{
    CreateTableRequest, KeyAttributes, StorageError, StorageResult, StoredTableInfo, TableName,
    WireItem,
};
use tokio_postgres::{Row, types::ToSql};

use crate::{
    AttributeName,
    backends::postgres::{
        KeyColumnBinding, OrderedKeyColumn, PostgresStorageProvider, physical_names, sql_statements,
    },
    provider_core::gsi_write::{
        GsiSqlPlanOptions, GsiUpsertStyle, PlaceholderNumbering, TableKeyColumnStyle,
        plan_gsi_sql_statements,
    },
    write_plan::WriteMaintenancePlan,
};

#[derive(Clone)]
pub(super) struct PreparedGetItemQuery {
    table_info: StoredTableInfo,
    sql: String,
    bind_values: Vec<String>,
}

impl PostgresStorageProvider {
    pub(super) fn key_attribute_type(
        table_info: &StoredTableInfo,
        attribute_name: &str,
    ) -> StorageResult<storage_types::KeyAttributeType> {
        table_info
            .attribute_definitions
            .iter()
            .find(|attribute| attribute.attribute_name == attribute_name)
            .map(|attribute| attribute.attribute_type.clone())
            .ok_or_else(|| {
                StorageError::validation(format!(
                    "missing attribute definition for key '{attribute_name}'"
                ))
            })
    }

    pub(super) fn sanitize_column_name(attribute_name: &str) -> String {
        AttributeName::new(attribute_name).sanitized().to_string()
    }

    pub(super) fn prefixed_column_name(attribute_name: &str, prefix: Option<&str>) -> String {
        let mut column = String::new();
        if let Some(prefix) = prefix {
            column.push_str(prefix);
        }
        column.push_str(&Self::sanitize_column_name(attribute_name));
        column
    }

    pub(super) fn postgres_key_sql_type(
        attribute_type: &storage_types::KeyAttributeType,
    ) -> &'static str {
        match attribute_type {
            storage_types::KeyAttributeType::S => "TEXT",
            storage_types::KeyAttributeType::N => "NUMERIC",
            storage_types::KeyAttributeType::B => "TEXT",
        }
    }

    pub(super) fn postgres_placeholder_for_type(
        parameter_index: usize,
        attribute_type: &storage_types::KeyAttributeType,
    ) -> String {
        match attribute_type {
            storage_types::KeyAttributeType::N => {
                format!("CAST(${parameter_index} AS TEXT)::NUMERIC")
            }
            storage_types::KeyAttributeType::S | storage_types::KeyAttributeType::B => {
                format!("${parameter_index}")
            }
        }
    }

    pub(super) fn build_postgres_table_creation_sqls(
        table_name: &TableName,
        attribute_definitions: &[storage_types::AttributeDefinition],
        key_schema: &[storage_types::KeySchemaElement],
        global_secondary_indexes: Option<&[storage_types::GlobalSecondaryIndex]>,
    ) -> Vec<String> {
        let physical_table_name = physical_names::physical_table_name(table_name);
        let mut key_columns = Vec::new();
        let mut processed_attributes = std::collections::HashSet::new();
        for key_element in key_schema {
            if processed_attributes.insert(&key_element.attribute_name)
                && let Some(attr_def) = attribute_definitions
                    .iter()
                    .find(|attr| attr.attribute_name == key_element.attribute_name)
            {
                let column_name = Self::sanitize_column_name(&attr_def.attribute_name);
                let sql_type = Self::postgres_key_sql_type(&attr_def.attribute_type);
                key_columns.push(format!("{column_name} {sql_type}"));
            }
        }

        if let Some(gsis) = global_secondary_indexes {
            for gsi in gsis {
                for key_element in &gsi.key_schema {
                    if processed_attributes.insert(&key_element.attribute_name)
                        && let Some(attr_def) = attribute_definitions
                            .iter()
                            .find(|attr| attr.attribute_name == key_element.attribute_name)
                    {
                        let column_name = Self::sanitize_column_name(&attr_def.attribute_name);
                        let sql_type = Self::postgres_key_sql_type(&attr_def.attribute_type);
                        key_columns.push(format!("{column_name} {sql_type}"));
                    }
                }
            }
        }

        let mut primary_key_columns = Vec::new();
        for key_element in key_schema {
            let column_name = Self::sanitize_column_name(&key_element.attribute_name);
            match key_element.key_type {
                storage_types::KeyType::Hash => primary_key_columns.insert(0, column_name),
                storage_types::KeyType::Range => primary_key_columns.push(column_name),
            }
        }
        vec![sql_statements::create_physical_table(
            &physical_table_name,
            &key_columns,
            &primary_key_columns,
        )]
    }

    pub(super) fn build_postgres_gsi_creation_sqls(
        table_name: &TableName,
        attribute_definitions: &[storage_types::AttributeDefinition],
        table_key_schema: &[storage_types::KeySchemaElement],
        global_secondary_indexes: &[storage_types::GlobalSecondaryIndex],
    ) -> Vec<String> {
        let mut gsi_sqls = Vec::new();
        for gsi in global_secondary_indexes {
            let gsi_table_name =
                physical_names::physical_gsi_table_name(table_name, &gsi.index_name);
            let mut key_columns = Vec::new();
            for key_element in &gsi.key_schema {
                if let Some(attr_def) = attribute_definitions
                    .iter()
                    .find(|attr| attr.attribute_name == key_element.attribute_name)
                {
                    let column_name = Self::sanitize_column_name(&attr_def.attribute_name);
                    let sql_type = Self::postgres_key_sql_type(&attr_def.attribute_type);
                    key_columns.push(format!("{column_name} {sql_type}"));
                }
            }
            for key_element in table_key_schema {
                let column_name = Self::sanitize_column_name(&key_element.attribute_name);
                let sql_type = attribute_definitions
                    .iter()
                    .find(|attr| attr.attribute_name == key_element.attribute_name)
                    .map_or("TEXT", |attr| {
                        Self::postgres_key_sql_type(&attr.attribute_type)
                    });
                let table_key_column = format!("table_{column_name} {sql_type}");
                key_columns.push(table_key_column);
            }

            let mut primary_key_columns = Vec::new();
            for key_element in &gsi.key_schema {
                let column_name = Self::sanitize_column_name(&key_element.attribute_name);
                match key_element.key_type {
                    storage_types::KeyType::Hash => primary_key_columns.insert(0, column_name),
                    storage_types::KeyType::Range => primary_key_columns.push(column_name),
                }
            }
            for key_element in table_key_schema {
                let table_key_column = format!(
                    "table_{}",
                    Self::sanitize_column_name(&key_element.attribute_name)
                );
                primary_key_columns.push(table_key_column);
            }

            gsi_sqls.push(sql_statements::create_gsi_table(
                &gsi_table_name,
                &key_columns,
                &primary_key_columns,
            ));
        }
        gsi_sqls
    }

    pub(super) async fn create_table_storage_with_client<C>(
        &self,
        client: &C,
        table_name: &TableName,
        request: &CreateTableRequest,
    ) -> StorageResult<()>
    where
        C: GenericClient + Sync,
    {
        let gsi_storage_indexes: Option<Vec<storage_types::GlobalSecondaryIndex>> = request
            .global_secondary_indexes
            .as_ref()
            .map(|indexes| indexes.iter().cloned().map(Into::into).collect());
        let create_table_sqls = Self::build_postgres_table_creation_sqls(
            table_name,
            &request.attribute_definitions,
            &request.key_schema,
            gsi_storage_indexes.as_deref(),
        );
        let gsi_sqls = gsi_storage_indexes
            .as_ref()
            .map(|indexes| {
                Self::build_postgres_gsi_creation_sqls(
                    table_name,
                    &request.attribute_definitions,
                    &request.key_schema,
                    indexes,
                )
            })
            .unwrap_or_default();
        let ttl_table_name = physical_names::physical_ttl_index_table_name(table_name);
        let create_ttl_sql = sql_statements::create_ttl_index_table(&ttl_table_name);

        for create_table_sql in create_table_sqls {
            client
                .batch_execute(&create_table_sql)
                .await
                .map_err(|err| Self::map_postgres_error("create main table storage", err))?;
        }
        for gsi_sql in gsi_sqls {
            client
                .batch_execute(&gsi_sql)
                .await
                .map_err(|err| Self::map_postgres_error("create gsi table storage", err))?;
        }
        client
            .batch_execute(&create_ttl_sql)
            .await
            .map_err(|err| Self::map_postgres_error("create ttl index table", err))?;
        Ok(())
    }

    pub(super) fn scalar_key_value(
        value: &storage_provider::AttributeValue,
        attribute_name: &str,
    ) -> StorageResult<String> {
        value.inner_string().map_err(|err| {
            StorageError::validation(format!(
                "key attribute '{attribute_name}' must be scalar: {err}"
            ))
        })
    }

    pub(super) fn key_column_bindings_for_schema(
        table_info: &StoredTableInfo,
        key_schema: &[storage_types::KeySchemaElement],
        key_attributes: &KeyAttributes,
        column_prefix: Option<&str>,
    ) -> StorageResult<Vec<KeyColumnBinding>> {
        let mut bindings = Vec::with_capacity(key_schema.len());
        for key in key_schema {
            let value = key_attributes
                .get(&key.attribute_name)
                .ok_or_else(StorageError::invalid_or_missing_key)?;
            bindings.push(KeyColumnBinding {
                column: Self::prefixed_column_name(&key.attribute_name, column_prefix),
                attribute_type: Self::key_attribute_type(table_info, &key.attribute_name)?,
                value: Self::scalar_key_value(value, &key.attribute_name)?,
            });
        }
        Ok(bindings)
    }

    pub(super) fn ordered_key_columns_for_origin(
        table_info: &StoredTableInfo,
        primary_key_schema: &[storage_types::KeySchemaElement],
        secondary_key_schema: Option<&[storage_types::KeySchemaElement]>,
    ) -> StorageResult<Vec<OrderedKeyColumn>> {
        let mut columns = Vec::with_capacity(
            primary_key_schema.len() + secondary_key_schema.map_or(0, |schema| schema.len()),
        );
        for key in primary_key_schema {
            columns.push(OrderedKeyColumn {
                column: Self::prefixed_column_name(&key.attribute_name, None),
                attribute_type: Self::key_attribute_type(table_info, &key.attribute_name)?,
            });
        }
        if let Some(schema) = secondary_key_schema {
            for key in schema {
                columns.push(OrderedKeyColumn {
                    column: Self::prefixed_column_name(&key.attribute_name, Some("table_")),
                    attribute_type: Self::key_attribute_type(table_info, &key.attribute_name)?,
                });
            }
        }
        Ok(columns)
    }

    pub(super) fn where_clause_for_bindings(
        bindings: &[KeyColumnBinding],
        bind_values: &mut Vec<String>,
    ) -> String {
        Self::where_clause_for_bindings_with_offset(bindings, bind_values, 0)
    }

    pub(super) fn where_clause_for_bindings_with_offset(
        bindings: &[KeyColumnBinding],
        bind_values: &mut Vec<String>,
        parameter_offset: usize,
    ) -> String {
        bindings
            .iter()
            .map(|binding| {
                bind_values.push(binding.value.clone());
                let placeholder = Self::postgres_placeholder_for_type(
                    parameter_offset + bind_values.len(),
                    &binding.attribute_type,
                );
                format!("{} = {placeholder}", binding.column)
            })
            .collect::<Vec<_>>()
            .join(" AND ")
    }

    pub(super) fn scalar_component(
        value: &storage_provider::AttributeValue,
    ) -> StorageResult<String> {
        value
            .inner_string()
            .map_err(|err| StorageError::validation(format!("key attribute must be scalar: {err}")))
    }

    pub(super) fn key_components_from_item_key(
        item_key: &storage_types::ItemKey,
    ) -> StorageResult<Vec<String>> {
        let mut components = Vec::new();
        components.push(Self::scalar_component(item_key.hash_key())?);
        if let Some(range_key) = item_key.range_key() {
            components.push(Self::scalar_component(range_key)?);
        }
        if let Some(table_key) = item_key.table_key_ref() {
            components.push(Self::scalar_component(&table_key.hash_key)?);
            if let Some(range_key) = &table_key.range_key {
                components.push(Self::scalar_component(range_key)?);
            }
        }
        Ok(components)
    }

    pub(super) fn build_exclusive_start_predicate(
        ordered_columns: &[OrderedKeyColumn],
        exclusive_start_key: &storage_types::ItemKey,
        scan_forward: bool,
        bind_values: &mut Vec<String>,
    ) -> StorageResult<Option<String>> {
        Self::build_exclusive_start_predicate_after_prefix(
            ordered_columns,
            exclusive_start_key,
            scan_forward,
            0,
            bind_values,
        )
    }

    pub(super) fn build_exclusive_start_predicate_after_prefix(
        ordered_columns: &[OrderedKeyColumn],
        exclusive_start_key: &storage_types::ItemKey,
        scan_forward: bool,
        fixed_prefix_columns: usize,
        bind_values: &mut Vec<String>,
    ) -> StorageResult<Option<String>> {
        let components = Self::key_components_from_item_key(exclusive_start_key)?;
        if components.is_empty() || ordered_columns.is_empty() {
            return Ok(None);
        }
        let component_count = components.len().min(ordered_columns.len());
        let fixed_prefix_columns = fixed_prefix_columns.min(component_count);
        if component_count <= fixed_prefix_columns {
            return Ok(None);
        }
        let op = if scan_forward { ">" } else { "<" };
        let columns = &ordered_columns[fixed_prefix_columns..component_count];
        let component_values = &components[fixed_prefix_columns..component_count];
        if columns.len() == 1 {
            bind_values.push(component_values[0].clone());
            let placeholder =
                Self::postgres_placeholder_for_type(bind_values.len(), &columns[0].attribute_type);
            return Ok(Some(format!("{} {op} {placeholder}", columns[0].column)));
        }

        let column_tuple = columns
            .iter()
            .map(|column| column.column.clone())
            .collect::<Vec<_>>()
            .join(", ");
        let placeholders = columns
            .iter()
            .zip(component_values)
            .map(|(column, value)| {
                bind_values.push(value.clone());
                Self::postgres_placeholder_for_type(bind_values.len(), &column.attribute_type)
            })
            .collect::<Vec<_>>()
            .join(", ");
        Ok(Some(format!("({column_tuple}) {op} ({placeholders})")))
    }

    pub(super) fn key_attribute_types_map_for_schema(
        table_info: &StoredTableInfo,
        key_schema: &[storage_types::KeySchemaElement],
    ) -> StorageResult<HashMap<String, storage_types::KeyAttributeType>> {
        let mut key_types = HashMap::with_capacity(key_schema.len());
        for key in key_schema {
            key_types.insert(
                key.attribute_name.clone(),
                Self::key_attribute_type(table_info, &key.attribute_name)?,
            );
        }
        Ok(key_types)
    }

    pub(super) fn compile_key_condition_sql(
        condition: &storage_condition::Condition,
        key_attribute_types: &HashMap<String, storage_types::KeyAttributeType>,
        bind_values: &mut Vec<String>,
    ) -> StorageResult<String> {
        fn compile(
            condition: &storage_condition::Condition,
            key_attribute_types: &HashMap<String, storage_types::KeyAttributeType>,
            bind_values: &mut Vec<String>,
        ) -> StorageResult<String> {
            let key_field =
                |field: &str| -> StorageResult<(String, storage_types::KeyAttributeType)> {
                    let Some(attribute_type) = key_attribute_types.get(field) else {
                        return Err(StorageError::validation(format!(
                            "Key condition field '{field}' is not part of the index key schema"
                        )));
                    };
                    Ok((
                        PostgresStorageProvider::sanitize_column_name(field),
                        attribute_type.clone(),
                    ))
                };

            match condition {
                storage_condition::Condition::Equal { field, value } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(key_condition_scalar_value(value)?);
                    let placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!("{column} = {placeholder}"))
                }
                storage_condition::Condition::LessThan { field, value } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(value.clone());
                    let placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!("{column} < {placeholder}"))
                }
                storage_condition::Condition::LessThanEqual { field, value } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(value.clone());
                    let placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!("{column} <= {placeholder}"))
                }
                storage_condition::Condition::GreaterThan { field, value } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(value.clone());
                    let placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!("{column} > {placeholder}"))
                }
                storage_condition::Condition::GreaterThanEqual { field, value } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(value.clone());
                    let placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!("{column} >= {placeholder}"))
                }
                storage_condition::Condition::Between { field, min, max } => {
                    let (column, attribute_type) = key_field(field)?;
                    bind_values.push(min.clone());
                    let min_placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    bind_values.push(max.clone());
                    let max_placeholder = PostgresStorageProvider::postgres_placeholder_for_type(
                        bind_values.len(),
                        &attribute_type,
                    );
                    Ok(format!(
                        "{column} BETWEEN {min_placeholder} AND {max_placeholder}"
                    ))
                }
                storage_condition::Condition::BeginsWith { field, prefix } => {
                    let (column, attribute_type) = key_field(field)?;
                    if matches!(attribute_type, storage_types::KeyAttributeType::N) {
                        return Err(StorageError::validation(
                            "begins_with is only valid for string or binary key attributes",
                        ));
                    }
                    let prefix = match prefix {
                        storage_types::AttributeValue::S(prefix) => prefix.clone(),
                        storage_types::AttributeValue::B(prefix) => {
                            prefix.trim_end_matches('=').to_string()
                        }
                        _ => {
                            return Err(StorageError::validation(
                                "begins_with is only valid for string or binary key attributes",
                            ));
                        }
                    };
                    bind_values.push(format!("{prefix}%"));
                    let placeholder = format!("${}", bind_values.len());
                    Ok(format!("{column} LIKE {placeholder}"))
                }
                storage_condition::Condition::And { conditions } => {
                    if conditions.is_empty() {
                        return Err(StorageError::validation(
                            "Invalid key condition expression: empty AND condition",
                        ));
                    }
                    let mut compiled = Vec::with_capacity(conditions.len());
                    for child in conditions {
                        compiled.push(compile(child, key_attribute_types, bind_values)?);
                    }
                    Ok(format!("({})", compiled.join(" AND ")))
                }
                _ => Err(StorageError::validation(
                    "Unsupported key condition expression",
                )),
            }
        }

        compile(condition, key_attribute_types, bind_values)
    }

    pub(super) fn attribute_from_key_scalar(
        table_info: &StoredTableInfo,
        attribute_name: &str,
        value: String,
    ) -> StorageResult<storage_provider::AttributeValue> {
        let attribute_type = Self::key_attribute_type(table_info, attribute_name)?;
        let attribute = match attribute_type {
            storage_types::KeyAttributeType::S => storage_provider::AttributeValue::S(value),
            storage_types::KeyAttributeType::N => storage_provider::AttributeValue::N(value),
            storage_types::KeyAttributeType::B => storage_provider::AttributeValue::B(value),
        };
        Ok(attribute)
    }

    pub(super) fn row_key_column_value(row: &Row, column: &str) -> StorageResult<String> {
        if let Ok(value) = row.try_get::<_, String>(column) {
            return Ok(value);
        }
        if let Ok(value) = row.try_get::<_, i64>(column) {
            return Ok(value.to_string());
        }
        if let Ok(value) = row.try_get::<_, i32>(column) {
            return Ok(value.to_string());
        }
        if let Ok(value) = row.try_get::<_, f64>(column) {
            return Ok(value.to_string());
        }
        if let Ok(value) = row.try_get::<_, Vec<u8>>(column) {
            return String::from_utf8(value).map_err(|_| {
                StorageError::validation(format!("invalid utf-8 in column '{column}'"))
            });
        }

        Err(StorageError::invalid_or_missing_key())
    }

    pub(super) fn row_key_attributes(
        row: &Row,
        table_info: &StoredTableInfo,
        key_schema: &[storage_types::KeySchemaElement],
        column_prefix: Option<&str>,
    ) -> StorageResult<HashMap<String, storage_provider::AttributeValue>> {
        let mut key_attributes = HashMap::with_capacity(key_schema.len());
        for key in key_schema {
            let column = Self::prefixed_column_name(&key.attribute_name, column_prefix);
            let value = Self::row_key_column_value(row, &column)?;
            key_attributes.insert(
                key.attribute_name.clone(),
                Self::attribute_from_key_scalar(table_info, &key.attribute_name, value)?,
            );
        }
        Ok(key_attributes)
    }

    pub(super) fn row_to_wire_item_for_origin(
        row: &Row,
        table_info: &StoredTableInfo,
        primary_key_schema: &[storage_types::KeySchemaElement],
        secondary_key_schema: Option<&[storage_types::KeySchemaElement]>,
    ) -> StorageResult<WireItem> {
        let primary_key_attributes =
            Self::row_key_attributes(row, table_info, primary_key_schema, None)?;
        let primary_key = storage_types::WireItemKeyAttributes::from_key_schema(
            primary_key_schema,
            &primary_key_attributes,
        )?;

        let secondary_key = if let Some(schema) = secondary_key_schema {
            let attributes = Self::row_key_attributes(row, table_info, schema, Some("table_"))?;
            Some(storage_types::WireItemKeyAttributes::from_key_schema(
                schema,
                &attributes,
            )?)
        } else {
            None
        };

        let non_key_attributes_blob = row
            .try_get::<_, Option<String>>("attributes_blob")
            .map_err(|err| Self::map_postgres_error("row decode attributes_blob", err))?
            .map(String::into_bytes);

        Ok(WireItem::local_split(
            primary_key,
            secondary_key,
            non_key_attributes_blob,
        ))
    }

    pub(super) fn row_to_wire_item(
        row: &Row,
        table_info: &StoredTableInfo,
    ) -> StorageResult<WireItem> {
        Self::row_to_wire_item_for_origin(row, table_info, &table_info.key_schema, None)
    }

    pub(super) fn build_select_projection_for_key_schema(
        table_info: &StoredTableInfo,
        key_schema: &[storage_types::KeySchemaElement],
        column_prefix: Option<&str>,
    ) -> StorageResult<Vec<String>> {
        let mut projection = Vec::with_capacity(key_schema.len());
        for key in key_schema {
            let column = Self::prefixed_column_name(&key.attribute_name, column_prefix);
            let key_type = Self::key_attribute_type(table_info, &key.attribute_name)?;
            if matches!(key_type, storage_types::KeyAttributeType::N) {
                projection.push(format!("{column}::TEXT AS {column}"));
            } else {
                projection.push(column);
            }
        }
        Ok(projection)
    }

    pub(super) fn build_select_projection_for_origin(
        table_info: &StoredTableInfo,
        primary_key_schema: &[storage_types::KeySchemaElement],
        secondary_key_schema: Option<&[storage_types::KeySchemaElement]>,
    ) -> StorageResult<String> {
        let mut projection =
            Self::build_select_projection_for_key_schema(table_info, primary_key_schema, None)?;
        if let Some(schema) = secondary_key_schema {
            projection.extend(Self::build_select_projection_for_key_schema(
                table_info,
                schema,
                Some("table_"),
            )?);
        }
        projection.push("attributes_blob".to_string());
        Ok(projection.join(", "))
    }

    pub(super) async fn delete_gsi_entries_for_item_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        item: &HashMap<String, storage_provider::AttributeValue>,
    ) -> StorageResult<()> {
        self.apply_gsi_entries_for_item_change_with_client(
            client,
            table_name,
            table_info,
            Some(item),
            None,
        )
        .await
    }

    pub(super) async fn apply_gsi_entries_for_item_change_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        _table_name: &TableName,
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
        new_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
    ) -> StorageResult<()> {
        #[cfg(test)]
        let plan_started = Instant::now();
        let plan = plan_postgres_gsi_sql_statements(table_info, old_item, new_item)?;

        #[cfg(test)]
        {
            provider_perf::record_amount(
                "postgres",
                "table_write_gsi_mutations",
                plan.statements().len() as u64,
            );
            provider_perf::record_amount(
                "postgres",
                "table_write_applied_mutations",
                plan.statements().len() as u64,
            );
            provider_perf::record_amount("postgres", "table_write_gsi_key_overlap", 0);
        }

        #[cfg(test)]
        provider_perf::record("postgres", "gsi_change_plan", plan_started.elapsed());

        #[cfg(test)]
        let execute_started = Instant::now();
        self.execute_postgres_write_plan(
            client,
            &plan,
            "apply gsi changes",
            "sql_execute_gsi_change",
        )
        .await?;
        #[cfg(test)]
        provider_perf::record("postgres", "gsi_change_execute", execute_started.elapsed());
        Ok(())
    }

    pub(super) async fn upsert_gsi_entries_for_item_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
        table_info: &StoredTableInfo,
        item: &HashMap<String, storage_provider::AttributeValue>,
    ) -> StorageResult<()> {
        self.apply_gsi_entries_for_item_change_with_client(
            client,
            table_name,
            table_info,
            None,
            Some(item),
        )
        .await
    }

    async fn execute_postgres_write_plan<C: GenericClient + Sync>(
        &self,
        client: &C,
        plan: &WriteMaintenancePlan<String>,
        error_context: &'static str,
        _perf_counter: &'static str,
    ) -> StorageResult<()> {
        if plan.statements().is_empty() {
            return Ok(());
        }

        let sql = if plan.statements().len() == 1 {
            plan.statements()[0].sql.clone()
        } else {
            let statements = plan
                .statements()
                .iter()
                .map(|statement| statement.sql.clone())
                .collect::<Vec<_>>();
            sql_statements::dml_ctes(&statements)
        };
        let bind_values = plan
            .statements()
            .iter()
            .flat_map(|statement| statement.params.iter().cloned())
            .collect::<Vec<_>>();
        let params: Vec<&(dyn ToSql + Sync)> = bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let started = Instant::now();
        client
            .execute(&sql, &params)
            .await
            .map_err(|err| Self::map_postgres_error(error_context, err))?;
        self.record_transaction_phase("batch_write_item", "gsi_execute", started.elapsed());
        #[cfg(test)]
        provider_perf::record("postgres", _perf_counter, started.elapsed());
        Ok(())
    }

    pub(super) async fn get_item_with_client<C: GenericClient + Sync>(
        &self,
        client: &C,
        table_name: &TableName,
        key_attributes: &KeyAttributes,
        table_info: &StoredTableInfo,
    ) -> StorageResult<Option<WireItem>> {
        let prepared = Self::prepare_get_item_query(table_name, key_attributes, table_info)?;
        self.execute_prepared_get_item_query(client, &prepared, "get_item", "db_query")
            .await
    }

    pub(super) fn prepare_get_item_query(
        table_name: &TableName,
        key_attributes: &KeyAttributes,
        table_info: &StoredTableInfo,
    ) -> StorageResult<PreparedGetItemQuery> {
        let select_projection =
            Self::build_select_projection_for_origin(table_info, &table_info.key_schema, None)?;
        let key_bindings = Self::key_column_bindings_for_schema(
            table_info,
            &table_info.key_schema,
            key_attributes,
            None,
        )?;
        let mut bind_values = Vec::with_capacity(key_bindings.len());
        let where_sql = Self::where_clause_for_bindings(&key_bindings, &mut bind_values);
        let table_name_safe = table_name.sanitized_name();
        let sql = sql_statements::get_item(&table_name_safe, &select_projection, &where_sql);
        Ok(PreparedGetItemQuery {
            table_info: table_info.clone(),
            sql,
            bind_values,
        })
    }

    pub(super) async fn execute_prepared_get_item_query<C: GenericClient + Sync>(
        &self,
        client: &C,
        prepared: &PreparedGetItemQuery,
        operation: &'static str,
        phase: &'static str,
    ) -> StorageResult<Option<WireItem>> {
        let params: Vec<&(dyn ToSql + Sync)> = prepared
            .bind_values
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect();
        let started = Instant::now();
        let row = client
            .query_opt(&prepared.sql, &params)
            .await
            .map_err(|err| Self::map_postgres_error("get_item query", err))?;
        self.record_transaction_phase(operation, phase, started.elapsed());
        row.map(|row| Self::row_to_wire_item(&row, &prepared.table_info))
            .transpose()
    }
}

fn key_condition_scalar_value(value: &storage_types::AttributeValue) -> StorageResult<String> {
    match value {
        storage_types::AttributeValue::S(value)
        | storage_types::AttributeValue::N(value)
        | storage_types::AttributeValue::B(value) => Ok(value.clone()),
        _ => Err(StorageError::validation(
            "KeyConditionExpression comparison values must be scalar",
        )),
    }
}

fn plan_postgres_gsi_sql_statements(
    table_info: &StoredTableInfo,
    old_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
    new_item: Option<&HashMap<String, storage_provider::AttributeValue>>,
) -> StorageResult<WriteMaintenancePlan<String>> {
    let options = GsiSqlPlanOptions::new(
        physical_names::physical_gsi_table_name,
        |value: &storage_provider::AttributeValue| {
            value.inner_string().map_err(|err| {
                StorageError::validation(format!("key attribute must be scalar: {err}"))
            })
        },
        String::new,
        |index, attribute_type| match attribute_type {
            Some(attribute_type) => {
                PostgresStorageProvider::postgres_placeholder_for_type(index, attribute_type)
            }
            None => format!("${index}"),
        },
        PostgresStorageProvider::prefixed_column_name,
        GsiUpsertStyle::OnConflictUpdateReturning,
        TableKeyColumnStyle::PrefixedAttributeNames,
        PlaceholderNumbering::AcrossPlan,
        crate::provider_core::gsi_write::GsiAttributesBlobStyle::NonKeyAttributes,
    );
    plan_gsi_sql_statements(table_info, old_item, new_item, &options)
}
