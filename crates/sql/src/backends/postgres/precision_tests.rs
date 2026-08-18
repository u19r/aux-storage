use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, ItemKey,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, StoredTableInfo,
    TableKey, TableName, TableStatus,
};

use crate::backends::postgres::PostgresStorageProvider;

fn table_info_with_numeric_keys() -> StoredTableInfo {
    let key_schema = vec![
        KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "sk".to_string(),
            key_type: KeyType::Range,
        },
    ];
    let gsi_key_schema = vec![
        KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        },
        KeySchemaElement {
            attribute_name: "gsi_sk".to_string(),
            key_type: KeyType::Range,
        },
    ];
    StoredTableInfo {
        table_name: TableName::new("precision_table"),
        table_status: TableStatus::Active,
        created_at: 0_i64.into(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: key_schema.clone(),
        max_indexers: storage_types::MaxIndexers::ZERO,
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi_precision"),
            key_schema: gsi_key_schema,
            projection: Projection {
                projection_type: Some(ProjectionType::All),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: storage_types::StreamRetentionDuration::default(),
        default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn postgres_number_keys_use_numeric_sql_type_and_casted_placeholders() {
    assert_eq!(
        PostgresStorageProvider::postgres_key_sql_type(&KeyAttributeType::N),
        "NUMERIC"
    );
    assert_eq!(
        PostgresStorageProvider::postgres_placeholder_for_type(3, &KeyAttributeType::N),
        "CAST($3 AS TEXT)::NUMERIC"
    );
}

#[test]
fn postgres_table_and_gsi_creation_use_numeric_for_number_keys() {
    let table_name = TableName::new("pg_numeric_keys");
    let attribute_definitions = vec![
        AttributeDefinition {
            attribute_name: "pk".to_string(),
            attribute_type: KeyAttributeType::N,
        },
        AttributeDefinition {
            attribute_name: "gsi_pk".to_string(),
            attribute_type: KeyAttributeType::N,
        },
    ];
    let key_schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];
    let gsis = vec![GlobalSecondaryIndex {
        index_name: IndexName::new("gsi_numeric"),
        key_schema: vec![KeySchemaElement {
            attribute_name: "gsi_pk".to_string(),
            key_type: KeyType::Hash,
        }],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
    }];

    let table_sqls = PostgresStorageProvider::build_postgres_table_creation_sqls(
        &table_name,
        &attribute_definitions,
        &key_schema,
        Some(&gsis),
        storage_types::MaxIndexers::ZERO,
    );
    assert_eq!(table_sqls.len(), 1);
    let table_sql = &table_sqls[0];
    assert!(table_sql.contains("pk NUMERIC"));
    assert!(table_sql.contains("gsi_pk NUMERIC"));
    assert!(!table_sql.contains("PARTITION BY HASH"));

    let gsi_sqls = PostgresStorageProvider::build_postgres_gsi_creation_sqls(
        &table_name,
        &attribute_definitions,
        &key_schema,
        &gsis,
        storage_types::MaxIndexers::ZERO,
    );
    assert_eq!(gsi_sqls.len(), 1);
    let gsi_sql = &gsi_sqls[0];
    assert!(gsi_sql.contains("gsi_pk NUMERIC"));
    assert!(gsi_sql.contains("table_pk NUMERIC"));
    assert!(!gsi_sql.contains("PARTITION BY HASH"));
}

#[test]
fn postgres_projection_casts_numeric_key_columns_to_text_for_reads() {
    let table_info = table_info_with_numeric_keys();
    let table_projection = PostgresStorageProvider::build_select_projection_for_origin(
        &table_info,
        &table_info.key_schema,
        None,
    )
    .expect("table projection");

    assert!(table_projection.contains("pk::TEXT AS pk"));
    assert!(table_projection.contains("sk"));
    assert!(!table_projection.contains("sk::TEXT AS sk"));
    assert!(table_projection.contains("attributes_blob"));

    let gsi_key_schema = &table_info
        .global_secondary_indexes
        .as_ref()
        .expect("gsi metadata")[0]
        .key_schema;
    let gsi_projection = PostgresStorageProvider::build_select_projection_for_origin(
        &table_info,
        gsi_key_schema,
        Some(&table_info.key_schema),
    )
    .expect("gsi projection");

    assert!(gsi_projection.contains("gsi_pk::TEXT AS gsi_pk"));
    assert!(gsi_projection.contains("table_pk::TEXT AS table_pk"));
    assert!(!gsi_projection.contains("gsi_sk::TEXT AS gsi_sk"));
    assert!(!gsi_projection.contains("table_sk::TEXT AS table_sk"));
}

#[test]
fn postgres_numeric_conditions_and_pagination_keep_full_precision_strings() {
    let high_precision =
        "123456789012345678901234567890.123456789012345678901234567890".to_string();

    let mut bind_values = Vec::new();
    let key_types = HashMap::from([("pk".to_string(), KeyAttributeType::N)]);
    let condition = storage_condition::Condition::Equal {
        field: "pk".to_string(),
        value: AttributeValue::N(high_precision.clone()),
    };
    let condition_sql = PostgresStorageProvider::compile_key_condition_sql(
        &condition,
        &key_types,
        &mut bind_values,
    )
    .expect("compile key condition");
    assert_eq!(condition_sql, "pk = CAST($1 AS TEXT)::NUMERIC");
    assert_eq!(bind_values, vec![high_precision.clone()]);

    let table_info = table_info_with_numeric_keys();
    let ordered_columns = PostgresStorageProvider::ordered_key_columns_for_origin(
        &table_info,
        &table_info.key_schema,
        None,
    )
    .expect("ordered columns");
    let exclusive_start_key = ItemKey::table_key(
        TableName::new("precision_table"),
        AttributeValue::N(high_precision.clone()),
        Some(AttributeValue::S("sort-1".to_string())),
    );
    let mut pagination_bind_values = Vec::new();
    let pagination_sql = PostgresStorageProvider::build_exclusive_start_predicate(
        &ordered_columns,
        &exclusive_start_key,
        true,
        &mut pagination_bind_values,
    )
    .expect("pagination predicate")
    .expect("predicate exists");

    assert!(pagination_sql.contains("(pk, sk) > (CAST($1 AS TEXT)::NUMERIC, $2)"));
    assert_eq!(pagination_bind_values[0], high_precision);
    assert_eq!(pagination_bind_values[1], "sort-1");
}

#[test]
fn postgres_query_pagination_skips_fixed_hash_prefix_for_table_reads() {
    let table_info = table_info_with_numeric_keys();
    let ordered_columns = PostgresStorageProvider::ordered_key_columns_for_origin(
        &table_info,
        &table_info.key_schema,
        None,
    )
    .expect("ordered columns");
    let exclusive_start_key = ItemKey::table_key(
        TableName::new("precision_table"),
        AttributeValue::N("42".to_string()),
        Some(AttributeValue::S("sort-1".to_string())),
    );
    let mut bind_values = Vec::new();

    let predicate = PostgresStorageProvider::build_exclusive_start_predicate_after_prefix(
        &ordered_columns,
        &exclusive_start_key,
        true,
        1,
        &mut bind_values,
    )
    .expect("pagination predicate")
    .expect("predicate exists");

    assert_eq!(predicate, "sk > $1");
    assert_eq!(bind_values, vec!["sort-1"]);
}

#[test]
fn postgres_query_pagination_skips_fixed_hash_prefix_for_gsi_reads() {
    let table_info = table_info_with_numeric_keys();
    let gsi_key_schema = &table_info
        .global_secondary_indexes
        .as_ref()
        .expect("gsi metadata")[0]
        .key_schema;
    let ordered_columns = PostgresStorageProvider::ordered_key_columns_for_origin(
        &table_info,
        gsi_key_schema,
        Some(&table_info.key_schema),
    )
    .expect("ordered columns");
    let exclusive_start_key = ItemKey::index_key(
        TableName::new("precision_table"),
        IndexName::new("gsi_precision"),
        AttributeValue::N("7".to_string()),
        Some(AttributeValue::S("gsi-sort-1".to_string())),
        TableKey::new(
            TableName::new("precision_table"),
            AttributeValue::N("42".to_string()),
            Some(AttributeValue::S("sort-1".to_string())),
        ),
    );
    let mut bind_values = Vec::new();

    let predicate = PostgresStorageProvider::build_exclusive_start_predicate_after_prefix(
        &ordered_columns,
        &exclusive_start_key,
        false,
        1,
        &mut bind_values,
    )
    .expect("pagination predicate")
    .expect("predicate exists");

    assert!(!predicate.contains("gsi_pk <"));
    assert!(
        predicate.contains("(gsi_sk, table_pk, table_sk) < ($1, CAST($2 AS TEXT)::NUMERIC, $3)")
    );
    assert_eq!(bind_values[0], "gsi-sort-1");
    assert_eq!(bind_values[1], "42");
    assert_eq!(bind_values[2], "sort-1");
}
