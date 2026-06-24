use storage_common::ttl::TtlConfigRecord;
use storage_types::{
    AttributeValue, ItemKey, KeyAttributeType, StorageError, StorageResult, StoredTableInfo,
    TimeToLiveStatus, WireItem,
};

use crate::{
    keyspace::table_identity::TableIdentity, sorted_kv_store::TransactWriteOperation, ttl,
};

pub(crate) fn project_wire_item_table_key_and_ttl(
    item: &WireItem,
    table_info: &StoredTableInfo,
    ttl_attribute: Option<&str>,
) -> StorageResult<(ItemKey, Option<i64>)> {
    // Shortcut: for primary writes and TTL index maintenance we only need
    // table key attributes plus optional TTL attribute.
    // DynamoDB business rule: primary key drives item identity, TTL index key
    // is derived from (ttl, primary_key_token), so parsing the full item map is
    // unnecessary work.
    let hash_key = table_info
        .key_schema
        .iter()
        .find(|key| key.key_type == storage_types::KeyType::Hash)
        .ok_or_else(|| StorageError::internal("missing hash key in table schema"))?;
    let range_key = table_info
        .key_schema
        .iter()
        .find(|key| key.key_type == storage_types::KeyType::Range);

    let mut fields = Vec::with_capacity(2 + usize::from(ttl_attribute.is_some()));
    if let Some(ttl_attribute) = ttl_attribute {
        fields.push(ttl_attribute);
    }
    // Projection order is fixed so we can parse once and index into the result
    // without allocating intermediary attribute maps.
    fields.push(hash_key.attribute_name.as_str());
    if let Some(range_key) = range_key {
        fields.push(range_key.attribute_name.as_str());
    }

    let values = item.scalar_attributes(&fields)?;
    let mut index = 0usize;

    let ttl_value = if ttl_attribute.is_some() {
        let value = values[index]
            .as_deref()
            .and_then(|value| value.parse::<i64>().ok());
        index += 1;
        value
    } else {
        None
    };

    let hash_scalar = values[index]
        .as_deref()
        .ok_or_else(StorageError::invalid_or_missing_key)?;
    index += 1;

    let hash_attribute_type = key_attribute_type_for_name(table_info, &hash_key.attribute_name)?;
    let hash_attribute = key_attribute_value_from_scalar(hash_scalar, hash_attribute_type);

    let range_attribute = if let Some(range_key) = range_key {
        let range_scalar = values[index]
            .as_deref()
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        let range_attribute_type =
            key_attribute_type_for_name(table_info, &range_key.attribute_name)?;
        Some(key_attribute_value_from_scalar(
            range_scalar,
            range_attribute_type,
        ))
    } else {
        None
    };

    let item_key = ItemKey::table_key(
        table_info.table_name.clone(),
        hash_attribute,
        range_attribute,
    );
    Ok((item_key, ttl_value))
}

pub(crate) fn wire_item_key_token_from_item_key(item_key: &ItemKey) -> StorageResult<String> {
    item_key
        .next_page_token()
        .map_err(|err| StorageError::internal(&format!("wire item key token build failed: {err}")))
}

pub(crate) fn ttl_tracking_enabled(config: Option<&TtlConfigRecord>) -> bool {
    config.is_some_and(|config| {
        matches!(
            config.status,
            TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
        )
    })
}

pub(crate) fn ttl_index_direct_operations_for_wire_items(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    ttl_config: Option<&TtlConfigRecord>,
    old_item: Option<&WireItem>,
    new_item: Option<&WireItem>,
    new_item_key_token: Option<&str>,
    new_item_ttl_value: Option<i64>,
) -> StorageResult<Vec<TransactWriteOperation>> {
    let Some(config) = ttl_config else {
        return Ok(Vec::new());
    };
    if !matches!(
        config.status,
        TimeToLiveStatus::Enabled | TimeToLiveStatus::Enabling
    ) {
        return Ok(Vec::new());
    }

    let old_key = if let Some(item) = old_item {
        ttl::compact_ttl_index_key_for_wire_item(
            table_identity,
            table_info,
            &config.attribute_name,
            item,
        )?
    } else {
        None
    };
    let new_key = ttl_index_key_for_new_item(
        table_identity,
        table_info,
        &config.attribute_name,
        new_item,
        new_item_key_token,
        new_item_ttl_value,
    )?;

    if old_key.is_some() && old_key == new_key {
        // Business rule: unchanged TTL index key means the item stays in the
        // same expiration bucket, so no index mutation is required.
        return Ok(Vec::new());
    }

    let mut operations = Vec::new();
    if let Some(key) = old_key {
        operations.push(TransactWriteOperation::Delete {
            key,
            condition: None,
        });
    }
    if let Some(key) = new_key {
        operations.push(TransactWriteOperation::Put {
            key,
            value: Vec::new(),
            condition: None,
        });
    }
    Ok(operations)
}

fn key_attribute_type_for_name(
    table_info: &StoredTableInfo,
    attribute_name: &str,
) -> StorageResult<KeyAttributeType> {
    table_info
        .attribute_definitions
        .iter()
        .find(|definition| definition.attribute_name == attribute_name)
        .map(|definition| definition.attribute_type.clone())
        .ok_or_else(|| {
            StorageError::internal(&format!(
                "missing key attribute definition for {attribute_name}"
            ))
        })
}

fn key_attribute_value_from_scalar(
    value: &str,
    attribute_type: KeyAttributeType,
) -> AttributeValue {
    match attribute_type {
        KeyAttributeType::S => AttributeValue::S(value.to_string()),
        KeyAttributeType::N => AttributeValue::N(value.to_string()),
        KeyAttributeType::B => AttributeValue::B(value.to_string()),
    }
}

fn ttl_index_key_for_new_item(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    ttl_attribute: &str,
    new_item: Option<&WireItem>,
    new_item_key_token: Option<&str>,
    new_item_ttl_value: Option<i64>,
) -> StorageResult<Option<Vec<u8>>> {
    let Some(item) = new_item else {
        return Ok(None);
    };

    // Shortcut: when caller already projected key token and TTL value from the
    // wire payload, reuse them to avoid reparsing wire JSON.
    // This preserves TTL behavior because TTL key shape is deterministic:
    // "__ttl-index/<table>/<ttl>/<primary_key_token>".
    if let Some(token) = new_item_key_token {
        if let Some(ttl) = new_item_ttl_value {
            return ttl::compact_ttl_index_key(table_identity, ttl, token).map(Some);
        }
        return ttl_index_key_for_wire_item_with_token(table_identity, ttl_attribute, token, item);
    }

    ttl::compact_ttl_index_key_for_wire_item(table_identity, table_info, ttl_attribute, item)
}

fn ttl_index_key_for_wire_item_with_token(
    table_identity: &TableIdentity,
    ttl_attribute: &str,
    key_token: &str,
    item: &WireItem,
) -> StorageResult<Option<Vec<u8>>> {
    let ttl_value = storage_common::ttl::ttl_value_from_wire_item(item, ttl_attribute)?;
    ttl_value
        .map(|ttl| ttl::compact_ttl_index_key(table_identity, ttl, key_token))
        .transpose()
}
