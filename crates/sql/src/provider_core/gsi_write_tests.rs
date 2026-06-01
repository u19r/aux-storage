use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, StorageError, StorageResult,
    StoredTableInfo, TableName, TableStatus, TimestampMillis,
};

use super::gsi_write::{
    GsiAttributesBlobStyle, GsiSqlPlanOptions, GsiUpsertStyle, PlaceholderNumbering,
    TableKeyColumnStyle, plan_gsi_sql_statements,
};

#[test]
fn fixed_key_full_blob_plan_deletes_by_full_gsi_and_table_key() {
    let table = table_info(ProjectionType::All);
    let old = item("old-gpk", "payload-a");
    let new = item("new-gpk", "payload-b");

    let plan = plan_gsi_sql_statements(
        &table,
        Some(&old),
        Some(&new),
        &fixed_key_options(GsiAttributesBlobStyle::FullProjectedItem),
    )
    .unwrap();

    assert_eq!(plan.statements().len(), 2);
    assert_eq!(
        plan.statements()[0].sql,
        "DELETE FROM \"table_gsi\" WHERE gsi_pk = ?1 AND gsi_sk = ?2 AND table_pk = ?3 AND \
         table_sk = ?4"
    );
    assert_eq!(plan.statements()[0].params, ["old-gpk", "gsk", "pk", "sk"]);
    assert_eq!(
        plan.statements()[1].sql,
        "INSERT INTO \"table_gsi\" (gsi_pk, gsi_sk, table_pk, table_sk, attributes_blob) VALUES \
         (?1, ?2, ?3, ?4, ?5) ON CONFLICT (gsi_pk, gsi_sk, table_pk, table_sk) DO UPDATE SET \
         gsi_pk = excluded.gsi_pk, gsi_sk = excluded.gsi_sk, table_pk = excluded.table_pk, \
         table_sk = excluded.table_sk, attributes_blob = excluded.attributes_blob"
    );
    assert!(plan.statements()[1].params[4].contains("payload-b"));
    assert!(plan.statements()[1].params[4].contains("new-gpk"));
}

#[test]
fn non_key_conflict_update_plan_does_not_update_primary_key_columns() {
    let table = table_info(ProjectionType::All);
    let new = item("new-gpk", "payload-b");

    let plan = plan_gsi_sql_statements(
        &table,
        None,
        Some(&new),
        &fixed_key_non_key_conflict_update_options(GsiAttributesBlobStyle::FullProjectedItem),
    )
    .unwrap();

    assert_eq!(plan.statements().len(), 1);
    assert_eq!(
        plan.statements()[0].sql,
        "INSERT INTO \"table_gsi\" (gsi_pk, gsi_sk, table_pk, table_sk, attributes_blob) VALUES \
         (?1, ?2, ?3, ?4, ?5) ON CONFLICT (gsi_pk, gsi_sk, table_pk, table_sk) DO UPDATE SET \
         attributes_blob = excluded.attributes_blob"
    );
}

#[test]
fn fixed_key_hash_only_table_plan_omits_missing_table_range_column() {
    let table = hash_only_table_info(ProjectionType::All);
    let new = hash_only_item("new-gpk", "payload-b");

    let plan = plan_gsi_sql_statements(
        &table,
        None,
        Some(&new),
        &fixed_key_non_key_conflict_update_options(GsiAttributesBlobStyle::FullProjectedItem),
    )
    .unwrap();

    assert_eq!(
        plan.statements()[0].sql,
        "INSERT INTO \"table_gsi\" (gsi_pk, table_pk, attributes_blob) VALUES (?1, ?2, ?3) ON \
         CONFLICT (gsi_pk, table_pk) DO UPDATE SET attributes_blob = excluded.attributes_blob"
    );
    assert_eq!(plan.statements()[0].params.len(), 3);
}

#[test]
fn prefixed_key_non_key_blob_plan_numbers_placeholders_across_statements() {
    let table = table_info(ProjectionType::All);
    let old = item("old-gpk", "payload-a");
    let new = item("new-gpk", "payload-b");

    let plan = plan_gsi_sql_statements(
        &table,
        Some(&old),
        Some(&new),
        &prefixed_key_options(GsiAttributesBlobStyle::NonKeyAttributes),
    )
    .unwrap();

    assert_eq!(
        plan.statements()[0].sql,
        "DELETE FROM \"table_gsi\" WHERE gsi_pk = $1 AND gsi_sk = $2 AND table_pk = $3 AND \
         table_sk = $4 RETURNING 1"
    );
    assert_eq!(
        plan.statements()[1].sql,
        "INSERT INTO \"table_gsi\" (gsi_pk, gsi_sk, table_pk, table_sk, attributes_blob) VALUES \
         ($5, $6, $7, $8, $9) ON CONFLICT (gsi_pk, gsi_sk, table_pk, table_sk) DO UPDATE SET \
         gsi_pk = excluded.gsi_pk, gsi_sk = excluded.gsi_sk, table_pk = excluded.table_pk, \
         table_sk = excluded.table_sk, attributes_blob = excluded.attributes_blob RETURNING 1"
    );
    assert!(plan.statements()[1].params[4].contains("payload-b"));
    assert!(!plan.statements()[1].params[4].contains("new-gpk"));
}

#[allow(clippy::type_complexity)]
fn fixed_key_non_key_conflict_update_options(
    blob_style: GsiAttributesBlobStyle,
) -> GsiSqlPlanOptions<
    'static,
    String,
    impl Fn(&TableName, &IndexName) -> String,
    impl Fn(&AttributeValue) -> StorageResult<String>,
    impl Fn() -> String,
    impl Fn(usize, Option<&KeyAttributeType>) -> String,
    impl Fn(&str, Option<&str>) -> String,
> {
    GsiSqlPlanOptions::new(
        physical_name,
        string_param,
        String::new,
        |index, _| format!("?{index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdateNonKey,
        TableKeyColumnStyle::FixedPkSk,
        PlaceholderNumbering::PerStatement,
        blob_style,
    )
}

#[allow(clippy::type_complexity)]
fn fixed_key_options(
    blob_style: GsiAttributesBlobStyle,
) -> GsiSqlPlanOptions<
    'static,
    String,
    impl Fn(&TableName, &IndexName) -> String,
    impl Fn(&AttributeValue) -> StorageResult<String>,
    impl Fn() -> String,
    impl Fn(usize, Option<&KeyAttributeType>) -> String,
    impl Fn(&str, Option<&str>) -> String,
> {
    GsiSqlPlanOptions::new(
        physical_name,
        string_param,
        String::new,
        |index, _| format!("?{index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdate,
        TableKeyColumnStyle::FixedPkSk,
        PlaceholderNumbering::PerStatement,
        blob_style,
    )
}

#[allow(clippy::type_complexity)]
fn prefixed_key_options(
    blob_style: GsiAttributesBlobStyle,
) -> GsiSqlPlanOptions<
    'static,
    String,
    impl Fn(&TableName, &IndexName) -> String,
    impl Fn(&AttributeValue) -> StorageResult<String>,
    impl Fn() -> String,
    impl Fn(usize, Option<&KeyAttributeType>) -> String,
    impl Fn(&str, Option<&str>) -> String,
> {
    GsiSqlPlanOptions::new(
        physical_name,
        string_param,
        String::new,
        |index, _| format!("${index}"),
        |attribute_name, prefix| match prefix {
            Some(prefix) => format!("{prefix}{attribute_name}"),
            None => attribute_name.to_string(),
        },
        GsiUpsertStyle::OnConflictUpdateReturning,
        TableKeyColumnStyle::PrefixedAttributeNames,
        PlaceholderNumbering::AcrossPlan,
        blob_style,
    )
}

fn physical_name(table_name: &TableName, index_name: &IndexName) -> String {
    format!("{}_{}", table_name.as_ref(), index_name.as_ref())
}

fn string_param(value: &AttributeValue) -> StorageResult<String> {
    match value {
        AttributeValue::S(value) => Ok(value.clone()),
        _ => Err(StorageError::validation("test value must be a string")),
    }
}

fn table_info(projection_type: ProjectionType) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![attr("pk"), attr("sk"), attr("gsi_pk"), attr("gsi_sk")],
        key_schema: vec![key("pk", KeyType::Hash), key("sk", KeyType::Range)],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi"),
            key_schema: vec![key("gsi_pk", KeyType::Hash), key("gsi_sk", KeyType::Range)],
            projection: Projection {
                projection_type: Some(projection_type),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        deletion_protection_enabled: false,
    }
}

fn hash_only_table_info(projection_type: ProjectionType) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![attr("pk"), attr("gsi_pk")],
        key_schema: vec![key("pk", KeyType::Hash)],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi"),
            key_schema: vec![key("gsi_pk", KeyType::Hash)],
            projection: Projection {
                projection_type: Some(projection_type),
                non_key_attributes: None,
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        deletion_protection_enabled: false,
    }
}

fn item(gsi_pk: &str, payload: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk".to_string())),
        ("sk".to_string(), AttributeValue::S("sk".to_string())),
        ("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
        ("gsi_sk".to_string(), AttributeValue::S("gsk".to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
}

fn hash_only_item(gsi_pk: &str, payload: &str) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("pk".to_string(), AttributeValue::S("pk".to_string())),
        ("gsi_pk".to_string(), AttributeValue::S(gsi_pk.to_string())),
        (
            "payload".to_string(),
            AttributeValue::S(payload.to_string()),
        ),
    ])
}

fn attr(name: &str) -> AttributeDefinition {
    AttributeDefinition {
        attribute_name: name.to_string(),
        attribute_type: KeyAttributeType::S,
    }
}

fn key(name: &str, key_type: KeyType) -> KeySchemaElement {
    KeySchemaElement {
        attribute_name: name.to_string(),
        key_type,
    }
}
