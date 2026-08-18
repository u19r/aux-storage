use foundationdb::tuple::{Element, unpack};
use storage_types::{AttributeValue, IndexName, ItemKey, TableKey, TableName};

use crate::keyspace::{
    compact::{CompactKeyError, KeyFamily, TableStorageId, parse_compact_key},
    table_identity::TableIdentity,
    tuple_keys::{
        FORMAT, GSI, TupleKeyElement, TupleMapperElement, gsi_prefix, item_key, item_key_prefix,
        item_key_prefix_end, item_mapper_elements, item_partition_mapper_elements,
    },
};

fn table() -> TableIdentity {
    TableIdentity::new(
        TableStorageId::new(42),
        TableName::new("orders"),
        Vec::new(),
    )
}

#[test]
fn primary_key_is_a_complete_tuple_and_has_fixed_null_positions() {
    let key = ItemKey::Table(TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("pk".to_string()),
        None,
    ));
    let encoded = item_key(&table(), &key).expect("tuple key");
    assert_eq!(&encoded[..2], &[0x15, 0x02]);
    assert!(!encoded.starts_with(b"p\0\0\0"));
    assert_eq!(
        encoded,
        vec![
            0x15, 0x02, 0x01, 0x00, 0x02, 0x69, 0x74, 0x65, 0x6d, 0x00, 0x15, 0x2a, 0x02, 0x53,
            0x00, 0x01, 0x70, 0x6b, 0x00, 0x00, 0x00,
        ]
    );
    assert_eq!(encoded, item_key(&table(), &key).unwrap());
}

#[test]
fn gsi_prefix_contains_no_legacy_family_byte() {
    let table = TableIdentity::user_indexes_for_table(
        TableStorageId::new(42),
        &TableName::new("orders"),
        Some(&[storage_types::GlobalSecondaryIndex {
            index_name: IndexName::new("status"),
            key_schema: Vec::new(),
            projection: storage_types::Projection {
                projection_type: None,
                non_key_attributes: None,
            },
        }]),
    );
    let range = gsi_prefix(&table, &IndexName::new("status")).expect("prefix");
    assert!(range.start.starts_with(&[0x15, 0x02]));
    assert!(range.end > range.start);
}

#[test]
fn gsi_tuple_places_base_key_fields_at_mapper_positions() {
    let table = TableIdentity::user_indexes_for_table(
        TableStorageId::new(7),
        &TableName::new("orders"),
        Some(&[storage_types::GlobalSecondaryIndex {
            index_name: IndexName::new("status"),
            key_schema: vec![storage_types::KeySchemaElement {
                attribute_name: "status".to_string(),
                key_type: storage_types::KeyType::Hash,
            }],
            projection: storage_types::Projection {
                projection_type: None,
                non_key_attributes: None,
            },
        }]),
    );
    let key = ItemKey::Index(storage_types::IndexKey {
        table_name: TableName::new("orders"),
        index_id: IndexName::new("status"),
        hash_key: AttributeValue::S("open".to_string()),
        range_key: None,
        table_key: TableKey::new(
            TableName::new("orders"),
            AttributeValue::S("tenant-a".to_string()),
            Some(AttributeValue::S("order-1".to_string())),
        ),
    });
    let encoded = item_key(&table, &key).expect("gsi tuple key");
    assert_eq!(
        encoded,
        vec![
            0x15, 0x02, 0x01, 0x00, 0x02, 0x67, 0x73, 0x69, 0x00, 0x15, 0x07, 0x15, 0x01, 0x02,
            0x53, 0x00, 0x01, 0x6f, 0x70, 0x65, 0x6e, 0x00, 0x00, 0x00, 0x02, 0x53, 0x00, 0x01,
            0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x2d, 0x61, 0x00, 0x02, 0x53, 0x00, 0x01, 0x6f,
            0x72, 0x64, 0x65, 0x72, 0x2d, 0x31, 0x00,
        ]
    );
    let elements = unpack::<Vec<Element<'_>>>(&encoded).expect("tuple key");
    assert_eq!(elements.len(), 13);
    assert!(matches!(elements[0], Element::Int(value) if value == FORMAT));
    assert!(matches!(elements[1], Element::Bytes(ref value) if value.as_ref().is_empty()));
    assert!(matches!(elements[2], Element::String(ref value) if value == GSI));
    assert!(matches!(elements[3], Element::Int(value) if value == 7));
    assert!(matches!(elements[4], Element::Int(_)));
    assert!(matches!(elements[9], Element::String(ref value) if value == "S"));
    assert!(matches!(&elements[10], Element::Bytes(value) if value.as_ref() == b"tenant-a"));
    assert!(matches!(elements[11], Element::String(ref value) if value == "S"));
    assert!(matches!(&elements[12], Element::Bytes(value) if value.as_ref() == b"order-1"));
}

#[test]
fn direct_mapper_is_tuple_decodable_and_maps_composite_source_keys() {
    let mapper = item_mapper_elements(
        TableStorageId::new(9),
        &TupleMapperElement::Key(TupleKeyElement { tag: 10, value: 11 }),
        Some(&TupleMapperElement::Key(TupleKeyElement {
            tag: 12,
            value: 13,
        })),
    )
    .expect("mapper");
    assert!(!mapper.is_empty());
    let elements = unpack::<Vec<Element<'_>>>(&mapper).expect("mapper tuple");
    assert_eq!(elements.len(), 9);
    assert!(matches!(elements[1], Element::String(ref value) if value == "{K[2]}"));
    assert!(matches!(elements[3], Element::Int(9)));
    assert!(matches!(elements[7], Element::String(ref value) if value == "{K[13]}"));
    assert!(matches!(elements[8], Element::String(ref value) if value == "{...}"));
    assert!(
        elements
            .iter()
            .all(|element| { !matches!(element, Element::String(value) if value.contains("{V[")) })
    );
}

#[test]
fn indexed_mapper_reads_the_absolute_foundationdb_value_slot() {
    let mapper = item_mapper_elements(
        TableStorageId::new(9),
        &TupleMapperElement::Value(storage_types::indexer_tuple_index(0)),
        None,
    )
    .expect("mapper");
    let elements = unpack::<Vec<Element<'_>>>(&mapper).expect("mapper tuple");

    assert!(matches!(elements[4], Element::String(ref value) if value == "S"));
    assert!(matches!(elements[5], Element::String(ref value) if value == "{V[2]}"));
    assert!(matches!(elements[6], Element::Nil));
    assert!(matches!(elements[7], Element::Nil));
}

#[test]
fn literal_mapper_element_encodes_a_static_string_key() {
    let mapper = item_mapper_elements(
        TableStorageId::new(9),
        &TupleMapperElement::Key(TupleKeyElement { tag: 7, value: 8 }),
        Some(&TupleMapperElement::Literal(
            storage_types::AttributeValue::S("META".to_string()),
        )),
    )
    .expect("mapper");
    let elements = unpack::<Vec<Element<'_>>>(&mapper).expect("mapper tuple");

    assert!(matches!(elements[6], Element::String(ref value) if value == "S"));
    assert!(matches!(&elements[7], Element::Bytes(value) if value.as_ref() == b"META"));
}

#[test]
fn partition_mapper_places_expansion_marker_after_hash_pair() {
    let mapper = item_partition_mapper_elements(
        TableStorageId::new(9),
        &TupleMapperElement::Key(TupleKeyElement { tag: 5, value: 6 }),
    )
    .expect("partition mapper");
    let elements = unpack::<Vec<Element<'_>>>(&mapper).expect("mapper tuple");

    assert_eq!(elements.len(), 7);
    assert!(matches!(&elements[4], Element::String(value) if value == "{K[5]}"));
    assert!(matches!(&elements[5], Element::String(value) if value == "{K[6]}"));
    assert!(matches!(&elements[6], Element::String(value) if value == "{...}"));
}

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn direct_mapper_avoids_placeholder_allocations() {
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "direct_mapper_avoids_placeholder_allocations",
        file!(),
        line!(),
        Some("fdb_direct_mapper"),
    );

    let mapper = item_mapper_elements(
        TableStorageId::new(9),
        &TupleMapperElement::Key(TupleKeyElement { tag: 5, value: 6 }),
        Some(&TupleMapperElement::Key(TupleKeyElement {
            tag: 7,
            value: 8,
        })),
    )
    .expect("mapper");
    std::hint::black_box(mapper);

    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert!(report.allocation_count <= 4, "{report:?}");
}

#[test]
fn foundationdb_rejects_the_superseded_compact_item_and_gsi_families() {
    assert_eq!(
        parse_compact_key(b"p\0\0\0\x01pk"),
        Err(CompactKeyError::LegacyItemFamily(KeyFamily::PrimaryItem))
    );
    assert_eq!(
        parse_compact_key(b"g\0\0\0\x01\0\x01gsi"),
        Err(CompactKeyError::LegacyItemFamily(KeyFamily::GsiItem))
    );
}

#[test]
fn tuple_range_prefix_contains_longer_string_keys() {
    let prefix = ItemKey::Table(TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("user1".to_string()),
        Some(AttributeValue::S("item0".to_string())),
    ));
    let full = ItemKey::Table(TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("user1".to_string()),
        Some(AttributeValue::S("item01".to_string())),
    ));
    let prefix_bytes = item_key_prefix(&table(), &prefix).expect("prefix");
    let end = item_key_prefix_end(&table(), &prefix).expect("end");
    let full_bytes = item_key(&table(), &full).expect("full key");
    assert!(full_bytes >= prefix_bytes);
    assert!(full_bytes < end);
}

#[test]
fn tuple_hash_prefix_end_excludes_longer_hash_values() {
    let prefix = ItemKey::Table(TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("user".to_string()),
        None,
    ));
    let longer = ItemKey::Table(TableKey::new(
        TableName::new("orders"),
        AttributeValue::S("user-2".to_string()),
        None,
    ));
    let start = item_key_prefix(&table(), &prefix).expect("prefix");
    let end = item_key_prefix_end(&table(), &prefix).expect("end");
    let longer_bytes = item_key(&table(), &longer).expect("longer key");
    assert!(longer_bytes >= start);
    assert!(longer_bytes >= end);
}
