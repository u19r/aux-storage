use storage_types::{
    AttributeValue, IndexName, ItemKey, KeySchemaElement, KeyType, TableKey, TableName,
};

use crate::{parse_conditions::CompiledKeyCondition, sql_builder::build_sql_query};

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
    let condition = CompiledKeyCondition::new("gsi_pk = ?1".to_string(), vec!["group#1".into()]);
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
