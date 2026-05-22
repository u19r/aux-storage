use storage_types::{
    AttributeDefinition, GlobalSecondaryIndex, IndexName, KeyAttributeType, KeySchemaElement,
    KeyType, Projection, TableName,
};

use crate::utils::{SqliteTableRowidMode, build_gsi_creation_sqls, build_table_creation_sql};

fn attribute_definitions() -> Vec<AttributeDefinition> {
    vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "sk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
        AttributeDefinition {
            attribute_name: "gsi_pk".to_string(),
            attribute_type: KeyAttributeType::S,
        },
    ]
}

fn table_key_schema() -> Vec<KeySchemaElement> {
    vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ]
}

fn global_secondary_indexes() -> Vec<GlobalSecondaryIndex> {
    vec![GlobalSecondaryIndex {
        index_name: IndexName::new("by_gsi_pk"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: None,
            non_key_attributes: None,
        },
    }]
}

#[test]
fn build_table_creation_sql_adds_without_rowid_when_requested() {
    let gsis = global_secondary_indexes();
    let sql = build_table_creation_sql(
        &TableName::new("orders"),
        &attribute_definitions(),
        &table_key_schema(),
        Some(&gsis),
        SqliteTableRowidMode::WithoutRowid,
    );

    assert!(sql.ends_with(") WITHOUT ROWID"));
}

#[test]
fn build_table_creation_sql_omits_without_rowid_when_requested() {
    let gsis = global_secondary_indexes();
    let sql = build_table_creation_sql(
        &TableName::new("orders"),
        &attribute_definitions(),
        &table_key_schema(),
        Some(&gsis),
        SqliteTableRowidMode::WithRowid,
    );

    assert!(sql.ends_with(')'));
    assert!(!sql.ends_with(") WITHOUT ROWID"));
}

#[test]
fn build_gsi_creation_sqls_add_without_rowid_when_requested() {
    let sqls = build_gsi_creation_sqls(
        &TableName::new("orders"),
        &attribute_definitions(),
        &table_key_schema(),
        &global_secondary_indexes(),
        SqliteTableRowidMode::WithoutRowid,
    );

    assert_eq!(sqls.len(), 1);
    assert!(sqls[0].ends_with(") WITHOUT ROWID"));
}

#[test]
fn build_gsi_creation_sqls_uses_actual_hash_only_table_key_shape() {
    let sqls = build_gsi_creation_sqls(
        &TableName::new("orders"),
        &attribute_definitions(),
        &[KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        &global_secondary_indexes(),
        SqliteTableRowidMode::WithoutRowid,
    );

    assert_eq!(sqls.len(), 1);
    assert!(sqls[0].contains("table_pk TEXT"));
    assert!(!sqls[0].contains("table_sk"));
    assert!(sqls[0].contains("PRIMARY KEY (gsi_pk, table_pk)"));
}
