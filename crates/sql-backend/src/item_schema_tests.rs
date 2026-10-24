use std::sync::Arc;

use storage_types::{AttributeDefinition, KeyAttributeType, KeySchemaElement, KeyType};

use crate::{SqlEngine, item_schema::render_item_table};

#[test]
fn given_reversed_key_schema_when_rendering_then_primary_key_order_is_preserved() {
    let mut table = crate::test_support::table();
    let info = Arc::make_mut(&mut table.info);
    info.key_schema = vec![
        KeySchemaElement {
            attribute_name: "sk".to_owned(),
            key_type: KeyType::Range,
        },
        KeySchemaElement {
            attribute_name: "pk".to_owned(),
            key_type: KeyType::Hash,
        },
    ];
    info.attribute_definitions = ["pk", "sk"]
        .map(|name| AttributeDefinition {
            attribute_name: name.to_owned(),
            attribute_type: KeyAttributeType::S,
        })
        .to_vec();
    let root = render_item_table(SqlEngine::Postgres, &table.info, table.history_id);
    assert!(root.contains("PRIMARY KEY (\"pk\", \"sk\")"));
    for engine in [SqlEngine::Postgres, SqlEngine::Sqlite, SqlEngine::Turso] {
        assert!(!render_item_table(engine, &table.info, table.history_id).contains("PARTITION BY"));
    }
}
