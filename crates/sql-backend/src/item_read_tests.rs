use std::sync::Arc;

use storage_types::{
    AttributeDefinition, AttributeValue, BoundTableInfo, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType,
};

use crate::{SqlEngine, item_read::primary_key_predicate, test_support::table};

fn keyed_table(count: usize, width: usize, numeric: bool) -> (BoundTableInfo, KeyAttributes) {
    let mut table = table();
    let info = Arc::make_mut(&mut table.info);
    let mut key = KeyAttributes::with_capacity(count);
    for index in 0..count {
        let name = format!("key_{index}_{}", "x".repeat(width));
        info.key_schema.push(KeySchemaElement {
            attribute_name: name.clone(),
            key_type: if index == 0 {
                KeyType::Hash
            } else {
                KeyType::Range
            },
        });
        info.attribute_definitions.push(AttributeDefinition {
            attribute_name: name.clone(),
            attribute_type: if numeric {
                KeyAttributeType::N
            } else {
                KeyAttributeType::S
            },
        });
        key.insert(
            name,
            if numeric {
                AttributeValue::N("123.45".to_owned())
            } else {
                AttributeValue::S("value".to_owned())
            },
        );
    }
    (table, key)
}

#[test]
fn given_primary_predicate_when_rendered_then_preserve_order_types_and_missing_key_error() {
    for (engine, prefix) in [
        (SqlEngine::Sqlite, "?"),
        (SqlEngine::Turso, "?"),
        (SqlEngine::Postgres, "$"),
    ] {
        let (mut table, key) = keyed_table(2, 0, false);
        Arc::make_mut(&mut table.info).key_schema.reverse();
        let (sql, binds) = primary_key_predicate(engine, &table, &key).unwrap();
        assert_eq!(
            sql,
            format!("\"key_0_\" = {prefix}1 AND \"key_1_\" = {prefix}2")
        );
        assert_eq!(binds.len(), 2);
        let (table, key) = keyed_table(2, 0, true);
        let (sql, binds) = primary_key_predicate(engine, &table, &key).unwrap();
        let first = if engine == SqlEngine::Postgres {
            format!("CAST({prefix}1 AS TEXT)::NUMERIC")
        } else {
            format!("{prefix}1")
        };
        assert!(sql.starts_with(&format!("\"key_0_\" = {first}")));
        assert_eq!(binds.len(), 2);
        assert!(primary_key_predicate(engine, &table, &KeyAttributes::new()).is_err());
    }
}
