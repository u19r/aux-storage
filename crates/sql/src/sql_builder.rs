use std::fmt::Write;

use storage_types::{ItemKey, KeySchemaElement, KeyType, StorageError, StorageResult};

use crate::parse_conditions::CompiledKeyCondition;

/// Builds a SQL query for key-condition reads with optional pagination.
pub fn build_sql_query(
    table_name_safe: &str,
    key_schema: &[KeySchemaElement],
    conditions: Option<CompiledKeyCondition>,
    exclusive_start_key: Option<ItemKey>,
    limit: u32,
    scan_index_forward: Option<bool>,
    table_key_schema_for_index: Option<&[KeySchemaElement]>,
) -> StorageResult<(String, Vec<String>)> {
    let mut sql = format!("SELECT * FROM \"{table_name_safe}\" ");
    let mut values = Vec::new();
    let mut has_where_clause = false;
    let fixed_pagination_prefix_columns = usize::from(conditions.is_some());

    // Add WHERE clause for conditions
    if let Some(conditions) = conditions {
        let (condition_sql, condition_values) = conditions.into_parts();
        let _ = write!(sql, " WHERE {condition_sql}");
        values.extend(condition_values);
        has_where_clause = true;
    }

    if table_key_schema_for_index.is_some() {
        let join_str = if has_where_clause { " AND " } else { " WHERE " };
        let _ = write!(sql, "{join_str}__aux_tombstone = 0");
        has_where_clause = true;
    }

    // Add WHERE clause for exclusive start key if provided
    if let Some(start_key) = exclusive_start_key {
        let join_str = if has_where_clause { " AND " } else { " WHERE " };

        let op = if scan_index_forward.unwrap_or(true) {
            ">"
        } else {
            "<"
        };

        let mut key_parts: Vec<(String, String)> = Vec::new();

        match start_key {
            ItemKey::Table(table_key) => {
                let hash_key_name = &key_schema
                    .iter()
                    .find_map(|ks| key_name_for_type(ks, KeyType::Hash))
                    .ok_or_else(|| StorageError::validation("table hash key schema missing"))?;
                let hash_value =
                    table_key
                        .hash_key
                        .inner_str()
                        .map(str::to_owned)
                        .map_err(|err| {
                            StorageError::validation(format!(
                                "exclusive start key hash must be scalar: {err}"
                            ))
                        })?;
                key_parts.push(((*hash_key_name).clone(), hash_value));

                if let Some(range_key) = table_key.range_key {
                    let range_key_name = &key_schema
                        .iter()
                        .find_map(|ks| key_name_for_type(ks, KeyType::Range))
                        .ok_or_else(|| {
                            StorageError::validation("table range key schema missing")
                        })?;
                    let range_value = range_key.inner_str().map(str::to_owned).map_err(|err| {
                        StorageError::validation(format!(
                            "exclusive start key range must be scalar: {err}"
                        ))
                    })?;
                    key_parts.push(((*range_key_name).clone(), range_value));
                }
            }
            ItemKey::Index(index_key) => {
                let hash_key_name = &key_schema
                    .iter()
                    .find_map(|ks| key_name_for_type(ks, KeyType::Hash))
                    .ok_or_else(|| StorageError::validation("index hash key schema missing"))?;
                let hash_value =
                    index_key
                        .hash_key
                        .inner_str()
                        .map(str::to_owned)
                        .map_err(|err| {
                            StorageError::validation(format!(
                                "exclusive start key hash must be scalar: {err}"
                            ))
                        })?;
                key_parts.push(((*hash_key_name).clone(), hash_value));

                if let Some(range_key) = index_key.range_key {
                    let range_key_name = &key_schema
                        .iter()
                        .find_map(|ks| key_name_for_type(ks, KeyType::Range))
                        .ok_or_else(|| {
                            StorageError::validation("index range key schema missing")
                        })?;
                    let range_value = range_key.inner_str().map(str::to_owned).map_err(|err| {
                        StorageError::validation(format!(
                            "exclusive start key range must be scalar: {err}"
                        ))
                    })?;
                    key_parts.push(((*range_key_name).clone(), range_value));
                }

                if let Some(table_key_schema) = table_key_schema_for_index {
                    let table_hash_name = table_key_schema
                        .iter()
                        .find_map(|ks| key_name_for_type(ks, KeyType::Hash))
                        .ok_or_else(|| StorageError::validation("table hash key schema missing"))?;
                    let table_hash_value = index_key
                        .table_key
                        .hash_key
                        .inner_str()
                        .map(str::to_owned)
                        .map_err(|err| {
                            StorageError::validation(format!(
                                "exclusive start table hash must be scalar: {err}"
                            ))
                        })?;
                    key_parts.push((format!("table_{table_hash_name}"), table_hash_value));

                    if let Some(table_range_key) = index_key.table_key.range_key {
                        let table_range_name = table_key_schema
                            .iter()
                            .find_map(|ks| key_name_for_type(ks, KeyType::Range))
                            .ok_or_else(|| {
                                StorageError::validation("table range key schema missing")
                            })?;
                        let table_range_value = table_range_key
                            .inner_str()
                            .map(str::to_owned)
                            .map_err(|err| {
                                StorageError::validation(format!(
                                    "exclusive start table range must be scalar: {err}"
                                ))
                            })?;
                        key_parts.push((format!("table_{table_range_name}"), table_range_value));
                    }
                }
            }
            ItemKey::IndexPrefix(index_key) => {
                let hash_key_name = &key_schema
                    .iter()
                    .find_map(|ks| key_name_for_type(ks, KeyType::Hash))
                    .ok_or_else(|| StorageError::validation("index hash key schema missing"))?;
                let hash_value =
                    index_key
                        .hash_key
                        .inner_str()
                        .map(str::to_owned)
                        .map_err(|err| {
                            StorageError::validation(format!(
                                "exclusive start key hash must be scalar: {err}"
                            ))
                        })?;
                key_parts.push(((*hash_key_name).clone(), hash_value));

                if let Some(range_key) = index_key.range_key {
                    let range_key_name = &key_schema
                        .iter()
                        .find_map(|ks| key_name_for_type(ks, KeyType::Range))
                        .ok_or_else(|| {
                            StorageError::validation("index range key schema missing")
                        })?;
                    let range_value = range_key.inner_str().map(str::to_owned).map_err(|err| {
                        StorageError::validation(format!(
                            "exclusive start key range must be scalar: {err}"
                        ))
                    })?;
                    key_parts.push(((*range_key_name).clone(), range_value));
                }
            }
        }

        let fixed_prefix_columns = fixed_pagination_prefix_columns.min(key_parts.len());
        if key_parts.len() > fixed_prefix_columns {
            let suffix = &key_parts[fixed_prefix_columns..];
            if suffix.len() == 1 {
                let (name, value) = &suffix[0];
                let placeholder = values.len() + 1;
                values.push(value.clone());
                let _ = write!(sql, "{join_str}{name} {op} ?{placeholder}");
            } else {
                let column_tuple = suffix
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                let value_tuple = suffix
                    .iter()
                    .map(|(_, value)| {
                        let placeholder = values.len() + 1;
                        values.push(value.clone());
                        format!("?{placeholder}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let _ = write!(sql, "{join_str}({column_tuple}) {op} ({value_tuple})");
            }
        }
    }

    // Add ordering for consistent pagination
    if !key_schema.is_empty() {
        let order_direction = if scan_index_forward.unwrap_or(true) {
            "ASC"
        } else {
            "DESC"
        };
        let mut order_by_parts: Vec<String> = key_schema
            .iter()
            .map(|ks| format!("{} {}", ks.attribute_name, order_direction))
            .collect();
        if let Some(table_key_schema) = table_key_schema_for_index {
            order_by_parts.extend(
                table_key_schema
                    .iter()
                    .map(|ks| format!("table_{} {}", ks.attribute_name, order_direction)),
            );
        }
        let order_by = order_by_parts.join(", ");
        let _ = write!(sql, " ORDER BY {order_by}");
    }

    // Add limit if specified
    let _ = write!(sql, " LIMIT {}", limit + 1);

    Ok((sql, values))
}

fn key_name_for_type(key_schema: &KeySchemaElement, key_type: KeyType) -> Option<&String> {
    if key_schema.key_type == key_type {
        Some(&key_schema.attribute_name)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use storage_types::{AttributeValue, IndexName, ItemKey, TableKey, TableName};

    use super::*;

    fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
        KeySchemaElement {
            attribute_name: name.to_string(),
            key_type,
        }
    }

    #[test]
    fn scan_pagination_uses_row_value_comparison_for_full_key() {
        let key_schema = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
        let exclusive_start_key = ItemKey::table_key(
            TableName::new("orders"),
            AttributeValue::S("tenant#1".to_string()),
            Some(AttributeValue::S("item#2".to_string())),
        );

        let (sql, values) = build_sql_query(
            "table_orders",
            &key_schema,
            None,
            Some(exclusive_start_key),
            10,
            Some(true),
            None,
        )
        .expect("build scan sql");

        assert!(sql.contains("WHERE (pk, sk) > (?1, ?2)"));
        assert_eq!(values, vec!["tenant#1", "item#2"]);
    }

    #[test]
    fn query_pagination_skips_fixed_hash_prefix_for_table_reads() {
        let key_schema = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
        let condition = CompiledKeyCondition::new("pk = ?1".to_string(), vec!["tenant#1".into()]);
        let exclusive_start_key = ItemKey::table_key(
            TableName::new("orders"),
            AttributeValue::S("tenant#1".to_string()),
            Some(AttributeValue::S("item#2".to_string())),
        );

        let (sql, values) = build_sql_query(
            "table_orders",
            &key_schema,
            Some(condition),
            Some(exclusive_start_key),
            10,
            Some(true),
            None,
        )
        .expect("build query sql");

        assert!(sql.contains("WHERE pk = ?1 AND sk > ?2"));
        assert_eq!(values, vec!["tenant#1", "item#2"]);
    }

    #[test]
    fn query_pagination_skips_fixed_hash_prefix_for_gsi_reads() {
        let gsi_schema = vec![key("gsi_pk", KeyType::Hash), key("gsi_sk", KeyType::Range)];
        let table_schema = vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)];
        let condition =
            CompiledKeyCondition::new("gsi_pk = ?1".to_string(), vec!["group#1".into()]);
        let exclusive_start_key = ItemKey::index_key(
            TableName::new("orders"),
            IndexName::new("gsi0"),
            AttributeValue::S("group#1".to_string()),
            Some(AttributeValue::S("score#2".to_string())),
            TableKey::new(
                TableName::new("orders"),
                AttributeValue::S("tenant#1".to_string()),
                Some(AttributeValue::S("item#2".to_string())),
            ),
        );

        let (sql, values) = build_sql_query(
            "gsi_orders_gsi0",
            &gsi_schema,
            Some(condition),
            Some(exclusive_start_key),
            10,
            Some(false),
            Some(&table_schema),
        )
        .expect("build gsi query sql");

        assert!(
            sql.contains("AND __aux_tombstone = 0 AND (gsi_sk, table_pk, table_sk) < (?2, ?3, ?4)")
        );
        assert_eq!(values, vec!["group#1", "score#2", "tenant#1", "item#2"]);
    }
}
