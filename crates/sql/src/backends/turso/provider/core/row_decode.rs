use crate::backends::turso::provider::core::*;

pub(crate) fn row_to_table_info(
    row: &HashMap<String, TursoValue>,
) -> StorageResult<StoredTableInfo> {
    let table_name = TableName::new(&row_required_text(row, "table_name")?);
    let table_status: TableStatus = row_required_text(row, "table_status")?.as_str().into();
    let created_at = row_required_i64(row, "created_at")?.into();

    let attribute_definitions = parse_json_or_default::<Vec<AttributeDefinition>>(
        row_required_text(row, "attribute_definitions")?.as_str(),
    )?;
    let key_schema = parse_json_or_default::<Vec<KeySchemaElement>>(
        row_required_text(row, "key_schema")?.as_str(),
    )?;

    let global_secondary_indexes = row_optional_text(row, "global_secondary_indexes")?
        .filter(|value| !value.trim().is_empty() && !value.trim().eq_ignore_ascii_case("null"))
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| StorageError::internal(&format!("parse gsi json failed: {error}")))?;

    let stream_specification = row_optional_text(row, "stream_specification")?
        .filter(|value| !value.trim().is_empty() && !value.trim().eq_ignore_ascii_case("null"))
        .map(|value| serde_json::from_str(&value))
        .transpose()
        .map_err(|error| {
            StorageError::internal(&format!("parse stream spec json failed: {error}"))
        })?;

    let table_size_bytes =
        u64::try_from(row_required_i64(row, "table_size_bytes")?).unwrap_or_default();
    let item_count = u64::try_from(row_required_i64(row, "item_count")?).unwrap_or_default();
    let max_indexers = storage_types::MaxIndexers::try_new(
        u8::try_from(row_required_i64(row, "max_indexers")?)
            .map_err(|_| StorageError::internal("invalid max_indexers metadata"))?,
    )?;
    let deletion_protection_enabled = row_required_i64(row, "deletion_protection_enabled")? != 0;
    let table_stream_duration = storage_types::StreamRetentionDuration::try_from(row_required_i64(
        row,
        "table_stream_duration_hours",
    )?)
    .map_err(|error| {
        StorageError::validation(format!("invalid table stream duration metadata: {error}"))
    })?;
    let default_item_stream_duration = storage_types::StreamRetentionDuration::try_from(
        row_required_i64(row, "default_item_stream_duration_hours")?,
    )
    .map_err(|error| {
        StorageError::validation(format!(
            "invalid default item stream duration metadata: {error}"
        ))
    })?;

    Ok(StoredTableInfo {
        table_name,
        table_status,
        created_at,
        attribute_definitions,
        key_schema,
        max_indexers,
        global_secondary_indexes,
        table_size_bytes,
        item_count,
        stream_specification,
        table_stream_duration,
        default_item_stream_duration,
        deletion_protection_enabled,
    })
}

pub(crate) fn row_to_item_map_main(
    row: &HashMap<String, TursoValue>,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    row_to_decoded_item_main(row, table_info)?
        .item
        .into_attribute_map()
}

pub(crate) fn row_to_decoded_item_main(
    row: &HashMap<String, TursoValue>,
    table_info: &StoredTableInfo,
) -> StorageResult<crate::indexed_item::SqlDecodedItem> {
    let mut key_attributes = HashMap::with_capacity(table_info.key_schema.len());
    for key in &table_info.key_schema {
        let value = row
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let key_type = attribute_type_for_key(table_info, &key.attribute_name);
        key_attributes.insert(
            key.attribute_name.clone(),
            key_attr_from_row_value(value, &key_type)?,
        );
    }
    crate::indexed_item::SqlIndexedItem::reconstruct_with_indexers(
        row_optional_text(row, "attributes_blob")?.unwrap_or_else(|| "{}".to_string()),
        row_indexer_slots(row, table_info)?,
        &KeyAttributes::from(key_attributes),
        table_info.max_indexers,
    )
}

pub(crate) fn row_view_to_item_map_main(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<HashMap<String, AttributeValue>> {
    row_view_to_decoded_item_main(row, table_info)?
        .item
        .into_attribute_map()
}

pub(crate) fn row_view_to_decoded_item_main(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<crate::indexed_item::SqlDecodedItem> {
    let mut key_attributes = HashMap::with_capacity(table_info.key_schema.len());
    for key in &table_info.key_schema {
        let value = row
            .get(&key.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let key_type = attribute_type_for_key(table_info, &key.attribute_name);
        key_attributes.insert(
            key.attribute_name.clone(),
            key_attr_from_row_value(value, &key_type)?,
        );
    }
    crate::indexed_item::SqlIndexedItem::reconstruct_with_indexers(
        row_view_optional_text(row, "attributes_blob")?.unwrap_or_else(|| "{}".to_string()),
        row_view_indexer_slots(row, table_info)?,
        &KeyAttributes::from(key_attributes),
        table_info.max_indexers,
    )
}

pub(crate) fn row_view_to_main_wire_item(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<WireItem> {
    let item = row_view_to_item_map_main(row, table_info)?;
    WireItem::from_attribute_map(&item)
}

pub(crate) fn row_view_to_gsi_wire_item(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    gsi_key_schema: &[KeySchemaElement],
) -> StorageResult<WireItem> {
    let primary_key =
        row_view_to_wire_key_attributes(row, table_info, gsi_key_schema, KeyColumn::Named)?;
    let secondary_key = row_view_to_wire_key_attributes(
        row,
        table_info,
        &table_info.key_schema,
        KeyColumn::TursoGsiTableKey,
    )?;
    let mut key_attributes =
        KeyAttributes::with_capacity(gsi_key_schema.len() + table_info.key_schema.len());
    append_wire_key_attributes(&mut key_attributes, primary_key);
    append_wire_key_attributes(&mut key_attributes, secondary_key);
    crate::indexed_item::SqlIndexedItem::reconstruct_with_indexers(
        row_view_optional_text(row, "attributes_blob")?.unwrap_or_else(|| "{}".to_string()),
        row_view_indexer_slots(row, table_info)?,
        &key_attributes,
        table_info.max_indexers,
    )
    .map(|decoded| decoded.item)
}

fn append_wire_key_attributes(target: &mut KeyAttributes, source: WireItemKeyAttributes) {
    target.insert(source.hash_key_name.into_owned(), source.hash_key);
    if let (Some(name), Some(value)) = (source.sort_key_name, source.sort_key) {
        target.insert(name.into_owned(), value);
    }
}

#[derive(Clone, Copy)]
enum KeyColumn {
    Named,
    TursoGsiTableKey,
}

fn row_view_to_wire_key_attributes(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    key_schema: &[KeySchemaElement],
    column: KeyColumn,
) -> StorageResult<WireItemKeyAttributes> {
    let hash_key = key_schema
        .iter()
        .find(|key| key.key_type == KeyType::Hash)
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    let hash_value = row_view_key_attr_from_column(row, table_info, hash_key, column)?;

    let range_key = key_schema.iter().find(|key| key.key_type == KeyType::Range);
    let range_value = range_key
        .map(|key| row_view_key_attr_from_column(row, table_info, key, column))
        .transpose()?;

    Ok(WireItemKeyAttributes::new(
        hash_key.attribute_name.clone(),
        hash_value,
        range_key.map(|key| key.attribute_name.clone()),
        range_value,
    ))
}

fn row_view_key_attr_from_column(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
    key: &KeySchemaElement,
    column: KeyColumn,
) -> StorageResult<AttributeValue> {
    let column_name = match (column, &key.key_type) {
        (KeyColumn::Named, _) => key.attribute_name.as_str(),
        (KeyColumn::TursoGsiTableKey, KeyType::Hash) => "table_pk",
        (KeyColumn::TursoGsiTableKey, KeyType::Range) => "table_sk",
    };
    let value = row
        .get(column_name)
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    let key_type = attribute_type_for_key(table_info, &key.attribute_name);
    key_attr_from_row_value(value, &key_type)
}

pub(crate) fn build_key_where_clause(
    key: &KeyAttributes,
    key_schema: &[KeySchemaElement],
) -> StorageResult<(String, Vec<TursoValue>)> {
    let mut clauses = Vec::new();
    let mut params = Vec::new();

    for (index, key_attr) in key_schema.iter().enumerate() {
        let value = key
            .get(&key_attr.attribute_name)
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        clauses.push(format!("{} = ?{}", key_attr.attribute_name, index + 1));
        params.push(attribute_scalar_to_turso_value(value)?);
    }

    Ok((clauses.join(" AND "), params))
}

pub(crate) fn gsi_table_name(
    table_name: &TableName,
    index_name: &storage_types::IndexName,
) -> String {
    GsiPhysicalName::compose(&table_name.sanitized_name(), &index_name.sanitized_name()).to_string()
}

pub(crate) fn parse_json_or_default<T>(raw: &str) -> StorageResult<T>
where T: serde::de::DeserializeOwned + Default {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("null") {
        return Ok(T::default());
    }

    serde_json::from_str(trimmed)
        .map_err(|error| StorageError::internal(&format!("json parse failed: {error}")))
}

fn attribute_type_for_key(table_info: &StoredTableInfo, key_name: &str) -> KeyAttributeType {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == key_name)
        .map(|definition| definition.attribute_type.clone())
        .unwrap_or(KeyAttributeType::S)
}

fn key_attr_from_row_value(
    value: &TursoValue,
    key_type: &KeyAttributeType,
) -> StorageResult<AttributeValue> {
    let scalar = value_to_string(value)?;
    Ok(match key_type {
        KeyAttributeType::S => AttributeValue::S(scalar),
        KeyAttributeType::N => AttributeValue::N(scalar),
        KeyAttributeType::B => AttributeValue::B(scalar),
    })
}

pub(crate) fn attribute_scalar_to_turso_value(value: &AttributeValue) -> StorageResult<TursoValue> {
    match value {
        AttributeValue::S(raw) | AttributeValue::B(raw) => Ok(TursoValue::Text(raw.clone())),
        AttributeValue::N(raw) => {
            if let Ok(int_value) = raw.parse::<i64>() {
                return Ok(TursoValue::Integer(int_value));
            }
            if let Ok(float_value) = raw.parse::<f64>() {
                return Ok(TursoValue::Real(float_value));
            }
            Ok(TursoValue::Text(raw.clone()))
        }
        AttributeValue::BOOL(value) => Ok(TursoValue::Integer(if *value { 1 } else { 0 })),
        AttributeValue::NULL(_) => Ok(TursoValue::Null),
        _ => value
            .inner_str()
            .map(|raw| TursoValue::Text(raw.to_string()))
            .map_err(|error| {
                StorageError::validation(format!("attribute must be scalar: {error}"))
            }),
    }
}

pub(crate) fn row_required_text(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<String> {
    row.get(column)
        .ok_or_else(|| StorageError::internal(&format!("missing column '{column}'")))
        .and_then(value_to_string)
}

pub(crate) fn row_optional_text(
    row: &HashMap<String, TursoValue>,
    column: &str,
) -> StorageResult<Option<String>> {
    let Some(value) = row.get(column) else {
        return Ok(None);
    };
    match value {
        TursoValue::Null => Ok(None),
        _ => value_to_string(value).map(Some),
    }
}

fn row_view_optional_text(row: TursoRowView<'_>, column: &str) -> StorageResult<Option<String>> {
    let Some(value) = row.get(column) else {
        return Ok(None);
    };
    match value {
        TursoValue::Null => Ok(None),
        _ => value_to_string(value).map(Some),
    }
}

fn row_indexer_slots(
    row: &HashMap<String, TursoValue>,
    table_info: &StoredTableInfo,
) -> StorageResult<Vec<Option<String>>> {
    (0..table_info.max_indexers.as_usize())
        .map(|ordinal| row_optional_text(row, &crate::utils::indexer_column_name(ordinal)))
        .collect()
}

fn row_view_indexer_slots(
    row: TursoRowView<'_>,
    table_info: &StoredTableInfo,
) -> StorageResult<Vec<Option<String>>> {
    (0..table_info.max_indexers.as_usize())
        .map(|ordinal| row_view_optional_text(row, &crate::utils::indexer_column_name(ordinal)))
        .collect()
}
