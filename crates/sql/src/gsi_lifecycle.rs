use std::{borrow::Cow, collections::HashMap};

use rusqlite::OptionalExtension as _;
use serde::ser::SerializeMap as _;
use storage_common::ttl::{augment_item_with_ttl_partition, is_ttl_index};
use storage_types::{
    self, AttributeValue, CreateGlobalSecondaryIndex, GlobalSecondaryIndex,
    GlobalSecondaryIndexUpdate, IndexName, KeySchemaElement, StorageError, StorageResult,
    StoredTableInfo, TTL_PARTITION_ATTRIBUTE, TableName, TimestampMillis,
};
use tracing::{Span, debug, field, instrument};

use crate::{
    backends::sqlite::SQLiteStorageProvider,
    constants::MAX_GSI_COUNT,
    error_handler::map_sqlite_error,
    sql_statements,
    utils::{SqliteTableRowidMode, build_gsi_creation_sqls, call_sqlite},
};

pub(crate) fn encode_gsi_json(
    global_secondary_indexes: Option<&Vec<GlobalSecondaryIndex>>,
) -> StorageResult<Option<String>> {
    match global_secondary_indexes {
        Some(gsis) => serde_json::to_string(gsis)
            .map(Some)
            .map_err(|e| StorageError::internal(&format!("serialize gsi metadata: {e}"))),
        None => Ok(None),
    }
}

/// Apply GSI projection filtering rules. Returns a new item map containing only
/// attributes that should appear in the GSI row.
#[cfg(test)]
pub(crate) fn apply_gsi_projection(
    full_item: &HashMap<String, storage_types::AttributeValue>,
    gsi_key: &HashMap<String, storage_types::AttributeValue>,
    main_key: &HashMap<String, storage_types::AttributeValue>,
    projection: &storage_types::Projection,
) -> HashMap<String, storage_types::AttributeValue> {
    use storage_types::ProjectionType;
    if projection.projection_type.is_none()
        || projection.projection_type.as_ref() == Some(&ProjectionType::All)
    {
        return full_item.clone();
    }
    let mut filtered = HashMap::new();
    // Always include GSI key + main table key attributes
    for (k, v) in gsi_key {
        filtered.insert(k.clone(), v.clone());
    }
    for (k, v) in main_key {
        filtered.insert(k.clone(), v.clone());
    }

    if projection.projection_type == Some(ProjectionType::Include)
        && let Some(attrs) = &projection.non_key_attributes
    {
        for attr_name in attrs {
            if let Some(val) = full_item.get(attr_name) {
                filtered.insert(attr_name.clone(), val.clone());
            }
        }
    }
    filtered
}

pub(crate) fn ttl_attribute_for_gsi(
    index_name: &IndexName,
    key_schema: &[KeySchemaElement],
) -> Option<String> {
    if !is_ttl_index(index_name) {
        return None;
    }
    key_schema
        .iter()
        .find(|elem| elem.attribute_name != TTL_PARTITION_ATTRIBUTE)
        .map(|elem| elem.attribute_name.clone())
}

#[cfg(test)]
pub(crate) fn non_key_attributes_for_gsi_row(
    filtered_item: HashMap<String, AttributeValue>,
    gsi_key: &HashMap<String, AttributeValue>,
    main_table_key: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    filtered_item
        .into_iter()
        .filter(|(k, _)| !gsi_key.contains_key(k) && !main_table_key.contains_key(k))
        .collect()
}

#[cfg(test)]
pub(crate) fn encode_gsi_attributes_blob(
    non_key_attributes: &HashMap<String, AttributeValue>,
) -> StorageResult<String> {
    if non_key_attributes.is_empty() {
        return Ok("{}".to_string());
    }
    serde_json::to_string(non_key_attributes).map_err(|e| StorageError::internal(&e.to_string()))
}

struct AttributePairs<'a>(&'a [(&'a str, &'a AttributeValue)]);

impl serde::Serialize for AttributePairs<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (name, value) in self.0 {
            map.serialize_entry(name, value)?;
        }
        map.end()
    }
}

fn is_gsi_row_key_attribute(
    key: &str,
    gsi_key: &[(&str, &AttributeValue)],
    main_table_key: &[(&str, &AttributeValue)],
) -> bool {
    gsi_key.iter().any(|(name, _)| *name == key)
        || main_table_key.iter().any(|(name, _)| *name == key)
}

pub(crate) fn key_attribute_refs<'a>(
    item: &'a HashMap<String, AttributeValue>,
    key_schema: &'a [KeySchemaElement],
) -> Option<Vec<(&'a str, &'a AttributeValue)>> {
    let mut key_attributes = Vec::with_capacity(key_schema.len());
    for key_element in key_schema {
        let value = item.get(&key_element.attribute_name)?;
        key_attributes.push((key_element.attribute_name.as_str(), value));
    }
    Some(key_attributes)
}

pub(crate) fn encode_gsi_projected_attributes_blob(
    item: &HashMap<String, AttributeValue>,
    gsi_key: &[(&str, &AttributeValue)],
    main_table_key: &[(&str, &AttributeValue)],
    projection: &storage_types::Projection,
) -> StorageResult<Cow<'static, str>> {
    use storage_types::ProjectionType;

    if projection.projection_type == Some(ProjectionType::KeysOnly) {
        return Ok(Cow::Borrowed("{}"));
    }

    let mut non_key_attributes = Vec::new();
    match projection.projection_type.as_ref() {
        None | Some(ProjectionType::All) => {
            for (key, value) in item {
                if is_gsi_row_key_attribute(key, gsi_key, main_table_key) {
                    continue;
                }
                non_key_attributes.push((key.as_str(), value));
            }
        }
        Some(ProjectionType::Include) => {
            if let Some(attrs) = &projection.non_key_attributes {
                for attr_name in attrs {
                    if let Some(value) = item.get(attr_name)
                        && !is_gsi_row_key_attribute(attr_name, gsi_key, main_table_key)
                    {
                        non_key_attributes.push((attr_name.as_str(), value));
                    }
                }
            }
        }
        Some(ProjectionType::KeysOnly) => {}
    }

    if non_key_attributes.is_empty() {
        return Ok(Cow::Borrowed("{}"));
    }

    serde_json::to_string(&AttributePairs(&non_key_attributes))
        .map(Cow::Owned)
        .map_err(|err| StorageError::internal(&format!("serialize gsi attributes: {err}")))
}

pub(crate) fn scalar_gsi_value(value: &AttributeValue) -> StorageResult<String> {
    value
        .inner_string()
        .map_err(|err| StorageError::validation(format!("key attribute must be scalar: {err}")))
}

pub(crate) fn push_scalar_key_values(
    values: &mut Vec<Cow<'static, str>>,
    key_attributes: &[(&str, &AttributeValue)],
) -> StorageResult<()> {
    for (_, value) in key_attributes {
        values.push(Cow::Owned(scalar_gsi_value(value)?));
    }
    Ok(())
}

pub(crate) fn gsi_backfill_insert_sql(
    table_name: &TableName,
    index_name: &IndexName,
    gsi_schema: &[KeySchemaElement],
    table_key_schema: &[KeySchemaElement],
) -> String {
    let gsi_table_name = crate::naming::physical_gsi_table_name(table_name, index_name);
    let mut all_columns = Vec::with_capacity(gsi_schema.len() + table_key_schema.len() + 1);
    all_columns.extend(gsi_schema.iter().map(|key| key.attribute_name.clone()));
    all_columns.extend(
        table_key_schema
            .iter()
            .map(|key| format!("table_{}", key.attribute_name)),
    );
    all_columns.push("attributes_blob".to_string());

    let placeholders = (1..=all_columns.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT OR REPLACE INTO \"{gsi_table_name}\" ({}) VALUES ({placeholders})",
        all_columns.join(", ")
    )
}

/// Apply a single GSI create operation: metadata update, physical table
/// creation, backfill tracking row insertion, and initial backfill population.
#[instrument(
    skip_all,
    fields(feature = "storage",
        table = %table_name,
        index = %create.index_name,
        key_elems = create.key_schema.len(),
        projection_type = field::Empty
    )
)]
pub async fn apply_gsi_create(
    provider: &SQLiteStorageProvider,
    table_info: &mut StoredTableInfo,
    table_name: &TableName,
    create: CreateGlobalSecondaryIndex,
) -> StorageResult<()> {
    let projection = create
        .projection
        .projection_type
        .as_ref()
        .map_or_else(|| "All".to_string(), |p| format!("{p:?}"));
    Span::current().record("projection_type", field::display(&projection));
    // Defensive validations (runtime) before mutating metadata
    if table_info
        .global_secondary_indexes
        .as_ref()
        .map_or(0, std::vec::Vec::len)
        + 1
        > MAX_GSI_COUNT
    {
        return Err(StorageError::validation(format!(
            "Too many global secondary indexes: would exceed {MAX_GSI_COUNT}"
        )));
    }
    // Ensure all key attributes for the new GSI exist in table attribute
    // definitions
    for ks in &create.key_schema {
        if !table_info
            .attribute_definitions
            .iter()
            .any(|d| d.attribute_name == ks.attribute_name)
        {
            return Err(StorageError::validation(format!(
                "GSI '{}' key attribute '{}' missing from attribute definitions",
                create.index_name, ks.attribute_name
            )));
        }
    }

    let mut new_gsis = table_info
        .global_secondary_indexes
        .clone()
        .unwrap_or_default();
    new_gsis.push(create.clone().into());
    table_info.global_secondary_indexes = Some(new_gsis);

    let table_name_clone = table_name.clone();
    let gsis_json = encode_gsi_json(table_info.global_secondary_indexes.as_ref())?;
    call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::update_gsis(&table_name_clone, &gsis_json);
        conn.execute(sql, params).map_err(map_sqlite_error)
    })
    .await?;

    let new_gsi = GlobalSecondaryIndex {
        index_name: create.index_name.clone(),
        key_schema: create.key_schema.clone(),
        projection: create.projection.clone(),
    };
    let new_gsis = vec![new_gsi];
    let gsi_sqls = build_gsi_creation_sqls(
        table_name,
        &table_info.attribute_definitions,
        &table_info.key_schema,
        &new_gsis,
        SqliteTableRowidMode::WithoutRowid,
    );
    for gsi_sql in gsi_sqls {
        call_sqlite(&provider.connection, move |conn| {
            conn.execute(&gsi_sql, []).map_err(map_sqlite_error)
        })
        .await?;
    }

    let sys_stream = storage_types::StreamName::system_table_stream();
    let captured_tail = call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::get_latest_item_for_cursor(&sys_stream);
        conn.query_row(sql, params, |row| row.get::<_, String>(0))
            .optional()
            .map_err(map_sqlite_error)
    })
    .await?;

    let now = TimestampMillis::now();
    let table_name_clone_for_bf = table_name.clone();
    let index_name_clone_for_bf = create.index_name.clone();
    let captured_tail_clone = captured_tail.clone();
    call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::upsert_gsi_backfill_start(
            &table_name_clone_for_bf,
            &index_name_clone_for_bf,
            "Backfilling",
            None,
            captured_tail_clone.as_deref(),
            &now,
            &now,
        );
        conn.execute(sql, params).map_err(map_sqlite_error)
    })
    .await?;

    backfill_gsi(provider, table_info, table_name, &create).await?;

    Ok(())
}

/// Perform backfill of a newly created GSI. Pages through main table via
/// `scan_table` API.
pub async fn backfill_gsi(
    provider: &SQLiteStorageProvider,
    table_info: &StoredTableInfo,
    table_name: &TableName,
    create: &CreateGlobalSecondaryIndex,
) -> StorageResult<()> {
    use storage_types::ScanTableRequest;

    let mut exclusive_start_key: Option<String> = call_sqlite(&provider.connection, {
        let table_name_q = table_name.clone();
        let index_name_q = create.index_name.clone();
        move |conn| {
            let (sql, params) = sql_statements::get_gsi_backfill(&table_name_q, &index_name_q);
            conn.query_row(sql, params, |row| row.get::<_, Option<String>>(1))
                .optional()
                .map_err(map_sqlite_error)
                .map(std::option::Option::flatten)
        }
    })
    .await?;

    let mut pages = 0usize;
    let mut rows_written = 0usize;
    let ttl_attribute = ttl_attribute_for_gsi(&create.index_name, &create.key_schema);
    let insert_sql = gsi_backfill_insert_sql(
        table_name,
        &create.index_name,
        &create.key_schema,
        &table_info.key_schema,
    );

    loop {
        let (wire_items, lek) =
            <SQLiteStorageProvider as storage_provider::StorageProvider>::scan_table(
                provider,
                &ScanTableRequest {
                    table_name: table_name.clone(),
                    index_name: None,
                    limit: Some(1000),
                    exclusive_start_key: exclusive_start_key.clone(),
                    consistent_read: true,
                },
            )
            .await?;

        if wire_items.is_empty() {
            break;
        }
        pages += 1;
        rows_written += wire_items.len();
        tracing::debug!(
            table=%table_name,
            index=%create.index_name,
            page=pages,
            page_rows = wire_items.len(),
            total_rows = rows_written,
            has_lek = lek.is_some(),
            "gsi.backfill.page"
        );

        let gsi_schema = create.key_schema.clone();
        let projection = create.projection.clone();
        let table_info_clone2 = table_info.clone();
        let ttl_attr_clone = ttl_attribute.clone();
        let insert_sql = insert_sql.clone();
        call_sqlite(
            &provider.connection,
            move |conn| -> Result<(), storage_types::StorageError> {
                let tx = conn.transaction().map_err(map_sqlite_error)?;
                for wire_item in wire_items {
                    let item = wire_item.into_attribute_map()?;
                    let item = if let Some(ref ttl_attr) = ttl_attr_clone {
                        match augment_item_with_ttl_partition(&table_info_clone2, &item, ttl_attr)?
                        {
                            Some(prepared) => prepared,
                            None => continue,
                        }
                    } else {
                        item
                    };

                    let Some(gsi_key) = key_attribute_refs(&item, &gsi_schema) else {
                        continue;
                    };
                    let Some(main_table_key) =
                        key_attribute_refs(&item, &table_info_clone2.key_schema)
                    else {
                        continue;
                    };
                    let attributes_blob = encode_gsi_projected_attributes_blob(
                        &item,
                        &gsi_key,
                        &main_table_key,
                        &projection,
                    )?;

                    let mut all_values =
                        Vec::with_capacity(gsi_key.len() + main_table_key.len() + 1);
                    push_scalar_key_values(&mut all_values, &gsi_key)?;
                    push_scalar_key_values(&mut all_values, &main_table_key)?;
                    all_values.push(attributes_blob);

                    tx.execute(&insert_sql, rusqlite::params_from_iter(all_values.iter()))
                        .map_err(map_sqlite_error)?;
                }
                tx.commit().map_err(map_sqlite_error)?;
                Ok::<(), StorageError>(())
            },
        )
        .await?;

        exclusive_start_key.clone_from(&lek);

        if exclusive_start_key.is_some() {
            let now = TimestampMillis::now();
            let table_name_clone_for_prog = table_name.clone();
            let index_name_clone_for_prog = create.index_name.clone();
            let lek_clone = exclusive_start_key.clone();
            call_sqlite(&provider.connection, move |conn| {
                let (sql, params) = sql_statements::update_gsi_backfill_progress(
                    &table_name_clone_for_prog,
                    &index_name_clone_for_prog,
                    lek_clone.as_deref(),
                    &now,
                );
                conn.execute(sql, params).map_err(map_sqlite_error)
            })
            .await?;
        }
        if exclusive_start_key.is_none() {
            tracing::debug!(
                table=%table_name,
                index=%create.index_name,
                pages,
                rows=rows_written,
                "gsi.backfill.near_completion"
            );
            break;
        }
    }

    let now = TimestampMillis::now();
    let table_name_clone_for_done = table_name.clone();
    let index_name_clone_for_done = create.index_name.clone();
    call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::mark_gsi_backfill_done(
            &table_name_clone_for_done,
            &index_name_clone_for_done,
            &now,
        );
        conn.execute(sql, params).map_err(map_sqlite_error)
    })
    .await?;

    debug!(table=%table_name, index=%create.index_name, pages, rows=rows_written, "gsi.backfill.complete");
    Ok(())
}

pub async fn apply_gsi_delete(
    provider: &SQLiteStorageProvider,
    table_info: &mut StoredTableInfo,
    table_name: &TableName,
    delete: storage_types::DeleteGlobalSecondaryIndexAction,
) -> StorageResult<()> {
    if let Some(mut gsis) = table_info.global_secondary_indexes.clone() {
        gsis.retain(|g| g.index_name != delete.index_name);
        table_info.global_secondary_indexes = if gsis.is_empty() { None } else { Some(gsis) };
    }

    let table_name_clone = table_name.clone();
    let gsis_json = encode_gsi_json(table_info.global_secondary_indexes.as_ref())?;
    call_sqlite(&provider.connection, move |conn| {
        let (sql, params) = sql_statements::update_gsis(&table_name_clone, &gsis_json);
        conn.execute(sql, params).map_err(map_sqlite_error)
    })
    .await?;

    let gsi_table_name = crate::naming::physical_gsi_table_name(table_name, &delete.index_name);
    let drop_sql = format!("DROP TABLE IF EXISTS \"{gsi_table_name}\"");
    call_sqlite(&provider.connection, move |conn| {
        conn.execute(&drop_sql, []).map_err(map_sqlite_error)
    })
    .await?;

    Ok(())
}

pub async fn process_gsi_updates(
    provider: &SQLiteStorageProvider,
    table_info: &mut StoredTableInfo,
    table_name: &TableName,
    gsi_updates: Vec<GlobalSecondaryIndexUpdate>,
) -> StorageResult<()> {
    for gsi_update in gsi_updates {
        if let Some(create) = gsi_update.create {
            apply_gsi_create(provider, table_info, table_name, create).await?;
        }
        if let Some(delete) = gsi_update.delete {
            apply_gsi_delete(provider, table_info, table_name, delete).await?;
        }
        if gsi_update.update.is_some() { /* Throughput only; no-op */ }
    }
    Ok(())
}
