use std::collections::HashMap;

use alloc_counter::AllocationGuard;
use storage_types::{
    AttributeDefinition, AttributeValue, KeyAttributeType, KeySchemaElement, KeyType,
    StoredTableInfo, TableName, TableStatus, TimestampMillis,
};
use turso::Value as TursoValue;

use super::core::{TursoRowSet, row_to_item_map_main, row_view_to_item_map_main};

#[test]
fn turso_query_row_set_decode_avoids_per_row_hashmap_work() {
    let table_info = realistic_table_info();
    let columns = vec![
        "pk".to_string(),
        "sk".to_string(),
        "attributes_blob".to_string(),
    ];
    let rows = realistic_rows(256);

    let legacy = {
        let guard = AllocationGuard::start(
            module_path!(),
            "legacy_turso_query_row_hashmap_decode",
            file!(),
            line!(),
            Some("baseline"),
        );
        for values in &rows {
            let mapped = columns
                .iter()
                .cloned()
                .zip(values.iter().cloned())
                .collect::<HashMap<_, _>>();
            let item = row_to_item_map_main(&mapped, &table_info).expect("legacy row decode");
            assert_eq!(
                item.get("pk"),
                Some(&AttributeValue::S("tenant#1".to_string()))
            );
        }
        guard.finish()
    };

    let optimized = {
        let row_set = TursoRowSet::from_parts(columns, rows);
        let guard = AllocationGuard::start(
            module_path!(),
            "indexed_turso_query_row_decode",
            file!(),
            line!(),
            Some("optimized"),
        );
        for row in row_set.iter() {
            let item = row_view_to_item_map_main(row, &table_info).expect("indexed row decode");
            assert_eq!(
                item.get("pk"),
                Some(&AttributeValue::S("tenant#1".to_string()))
            );
        }
        guard.finish()
    };

    alloc_counter::emit_report(&legacy);
    alloc_counter::emit_report(&optimized);
    assert!(
        optimized.allocation_count < legacy.allocation_count,
        "indexed decode should allocate less than per-row HashMap decode: legacy={} optimized={}",
        legacy.allocation_count,
        optimized.allocation_count
    );
    assert!(
        optimized.allocated_bytes < legacy.allocated_bytes,
        "indexed decode should allocate fewer bytes than per-row HashMap decode: legacy={} \
         optimized={}",
        legacy.allocated_bytes,
        optimized.allocated_bytes
    );
}

fn realistic_table_info() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("query_decode_table"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
    }
}

fn realistic_rows(count: usize) -> Vec<Vec<TursoValue>> {
    (0..count)
        .map(|index| {
            vec![
                TursoValue::Text("tenant#1".to_string()),
                TursoValue::Text(format!("item#{index:04}")),
                TursoValue::Text(format!(
                    r#"{{"pk":{{"S":"tenant#1"}},"sk":{{"S":"item#{index:04}"}},"payload":{{"S":"{}"}},"count":{{"N":"{}"}}}}"#,
                    "x".repeat(1024),
                    index
                )),
            ]
        })
        .collect()
}
