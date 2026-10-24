use std::collections::HashMap;

use crate::{
    AttributeDefinition, AttributeMap, AttributeValue, INDEXED_VALUE_LZ4_HEADER,
    INDEXED_VALUE_RAW_HEADER, IndexedRowView, IndexedWireItem, IndexerDeclaration,
    KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType, MAX_INDEXERS_CAPACITY, MaxIndexers,
    StorageEnum, StoredTableInfo, StreamRetentionDuration, TableName, TableStatus, TimestampMillis,
    constants::INDEXER_TUPLE_OFFSET, context::WrappedError,
};

fn attributes<const N: usize>(entries: [(String, AttributeValue); N]) -> AttributeMap {
    entries.into_iter().collect()
}

fn declaration(names: &[&str]) -> IndexerDeclaration {
    IndexerDeclaration::try_new(
        names.iter().map(|name| (*name).to_string()).collect(),
        MaxIndexers::try_new(32).unwrap(),
    )
    .unwrap()
}

#[test]
fn given_large_payload_when_extracting_indexers_then_projection_copies_are_eliminated() {
    let item = attributes([
        ("pk".to_string(), AttributeValue::S("item".to_string())),
        (
            "reference".to_string(),
            AttributeValue::S("other".to_string()),
        ),
        ("payload".to_string(), AttributeValue::S("x".repeat(4096))),
    ]);
    let indexers = declaration(&["reference"]);
    let measure = |label, encode: &dyn Fn() -> IndexedWireItem| {
        let guard = alloc_counter::AllocationGuard::start(
            module_path!(),
            "indexed_projection",
            file!(),
            line!(),
            Some(label),
        );
        for _ in 0..256 {
            std::hint::black_box(encode());
        }
        guard.finish()
    };
    let legacy = measure("copied_maps", &|| {
        let logical = indexers
            .names()
            .iter()
            .filter_map(|name| item.get(name).cloned().map(|value| (name.clone(), value)))
            .collect::<AttributeMap>();
        let projected = item.clone();
        IndexedWireItem::extract_projected(&logical, &projected, &indexers).unwrap()
    });
    let borrowed = measure("borrowed_maps", &|| {
        IndexedWireItem::extract_projected(&item, &item, &indexers).unwrap()
    });
    assert_eq!(
        IndexedWireItem::extract(&item, &indexers)
            .unwrap()
            .to_attribute_map()
            .unwrap(),
        item
    );
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&borrowed);
    assert!(borrowed.allocation_count < legacy.allocation_count);
    assert!(borrowed.allocated_bytes + 4096 * 256 < legacy.allocated_bytes);
}

fn assert_corruption(error: crate::StorageError, invariant: &str) {
    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message == &format!("stored_item_corruption:{invariant}")
    ));
}

#[test]
fn given_present_and_missing_values_when_extracted_then_slots_and_logical_item_round_trip() {
    let item = attributes([
        ("pk".to_string(), AttributeValue::S("entity#1".to_string())),
        (
            "customer_id".to_string(),
            AttributeValue::S("c-1".to_string()),
        ),
        ("region_id".to_string(), AttributeValue::S("eu".to_string())),
    ]);

    let indexed = IndexedWireItem::extract(
        &item,
        &declaration(&["customer_id", "optional_id", "region_id"]),
    )
    .unwrap();

    assert_eq!(
        indexed.slots(),
        &[Some("c-1".to_string()), None, Some("eu".to_string())]
    );
    let (logical, reconstructed_declaration) =
        indexed.into_attribute_map_with_declaration().unwrap();
    assert_eq!(logical, item);
    assert_eq!(
        reconstructed_declaration.names(),
        &["customer_id", "optional_id", "region_id"]
    );
}

#[test]
fn given_non_string_indexed_value_when_extracted_then_validation_fails() {
    let item = attributes([(
        "customer_id".to_string(),
        AttributeValue::N("1".to_string()),
    )]);

    let error = IndexedWireItem::extract(&item, &declaration(&["customer_id"])).unwrap_err();

    assert!(
        error
            .to_string()
            .contains("Indexers:attribute_must_be_string")
    );
}

#[test]
fn given_each_non_string_attribute_family_when_indexed_then_validation_fails() {
    let values = [
        AttributeValue::N("1".to_string()),
        AttributeValue::B("AA==".to_string()),
        AttributeValue::BOOL(true),
        AttributeValue::NULL(true),
        AttributeValue::M(HashMap::new()),
        AttributeValue::L(Vec::new()),
        AttributeValue::SS(vec!["x".to_string()]),
        AttributeValue::NS(vec!["1".to_string()]),
        AttributeValue::BS(vec!["AA==".to_string()]),
    ];

    for value in values {
        let item = attributes([("indexed".to_string(), value)]);
        let error = IndexedWireItem::extract(&item, &declaration(&["indexed"])).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Indexers:attribute_must_be_string")
        );
    }
}

#[test]
fn given_empty_indexed_string_when_extracted_then_validation_fails() {
    let item = attributes([("customer_id".to_string(), AttributeValue::S(String::new()))]);

    let error = IndexedWireItem::extract(&item, &declaration(&["customer_id"])).unwrap_err();

    assert!(error.to_string().contains("Indexers:empty_string"));
}

#[test]
fn given_duplicate_declaration_when_validated_then_validation_fails() {
    let error = IndexerDeclaration::try_new(
        vec!["id".to_string(), "id".to_string()],
        MaxIndexers::try_new(2).unwrap(),
    )
    .unwrap_err();

    assert!(error.to_string().contains("Indexers:duplicate_attribute"));
}

#[test]
fn given_case_distinct_unicode_and_whitespace_names_when_validated_then_names_round_trip() {
    let names = vec![
        "id".to_string(),
        "Id".to_string(),
        " customer ".to_string(),
        "顧客".to_string(),
    ];
    let declaration =
        IndexerDeclaration::try_new(names.clone(), MaxIndexers::try_new(4).unwrap()).unwrap();

    assert_eq!(declaration.names(), names);
}

#[test]
fn given_maximum_and_above_maximum_declarations_when_validated_then_bound_is_exact() {
    let names = (0..MAX_INDEXERS_CAPACITY)
        .map(|index| format!("field_{index}"))
        .collect::<Vec<_>>();
    assert!(
        IndexerDeclaration::try_new(
            names.clone(),
            MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).unwrap()
        )
        .is_ok()
    );
    let mut too_many = names;
    too_many.push("overflow".to_string());
    let error = IndexerDeclaration::try_new(
        too_many,
        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).unwrap(),
    )
    .unwrap_err();
    assert!(error.to_string().contains("Indexers:too_many"));
}

#[test]
fn given_gapped_markers_when_decoded_then_corruption_is_reported() {
    let residual = serde_json::to_vec(&serde_json::json!({
        "a": {"I": INDEXER_TUPLE_OFFSET},
        "b": {"I": INDEXER_TUPLE_OFFSET + 2}
    }))
    .unwrap();

    let error = IndexedWireItem::from_parts(residual, vec![None, None]).unwrap_err();

    assert!(matches!(
        error.to_enum(),
        StorageEnum::InternalServerError { message }
            if message.contains("stored_item_corruption")
    ));
}

#[test]
fn given_malformed_markers_when_decoded_then_each_invariant_is_rejected() {
    let cases = [
        (serde_json::json!({"a": {"I": -1}}), "marker_index"),
        (serde_json::json!({"a": {"I": 2.5}}), "marker_index"),
        (serde_json::json!({"a": {"I": 1}}), "marker_out_of_range"),
        (serde_json::json!({"a": {"I": 3}}), "marker_out_of_range"),
        (
            serde_json::json!({"a": {"I": 2}, "b": {"I": 2}}),
            "duplicate_marker",
        ),
        (
            serde_json::json!({"a": {"I": 2, "S": "not-a-marker"}}),
            "marker_shape",
        ),
        (
            serde_json::json!({"a": {"S": "public-value"}}),
            "marker_slot_count",
        ),
    ];

    for (residual, invariant) in cases {
        let residual = serde_json::to_vec(&residual).unwrap();
        assert_corruption(
            IndexedWireItem::from_parts(residual, vec![None]).unwrap_err(),
            invariant,
        );
    }
}

#[test]
fn given_public_attribute_value_with_internal_marker_when_deserialized_then_request_is_rejected() {
    assert!(serde_json::from_str::<AttributeValue>(r#"{"I":2}"#).is_err());
}

#[test]
fn given_indexed_item_when_enveloped_then_round_trip_preserves_declaration() {
    let item = attributes([("id".to_string(), AttributeValue::S("42".to_string()))]);
    let indexed = IndexedWireItem::extract(&item, &declaration(&["id", "missing"])).unwrap();

    let bytes = indexed.encode_envelope().unwrap();
    let decoded = IndexedWireItem::decode_envelope(&bytes).unwrap();

    assert_eq!(decoded, indexed);
    assert_eq!(decoded.to_attribute_map().unwrap(), item);
}

#[test]
fn given_same_projection_with_different_map_order_when_encoded_then_bytes_match() {
    let first = attributes([
        ("pk".to_string(), AttributeValue::S("a".to_string())),
        ("status".to_string(), AttributeValue::S("open".to_string())),
        ("gsi_pk".to_string(), AttributeValue::S("b".to_string())),
    ]);
    let second = attributes([
        ("gsi_pk".to_string(), AttributeValue::S("b".to_string())),
        ("pk".to_string(), AttributeValue::S("a".to_string())),
        ("status".to_string(), AttributeValue::S("open".to_string())),
    ]);
    let first = IndexedWireItem::extract(&first, &declaration(&[]))
        .unwrap()
        .encode_envelope()
        .unwrap();
    let second = IndexedWireItem::extract(&second, &declaration(&[]))
        .unwrap()
        .encode_envelope()
        .unwrap();

    assert_eq!(first, second);
}

#[test]
fn given_raw_and_compressible_items_when_enveloped_then_header_and_payload_round_trip() {
    let raw = IndexedWireItem::extract(
        &attributes([("id".to_string(), AttributeValue::S("42".to_string()))]),
        &declaration(&[]),
    )
    .unwrap();
    let raw_bytes = raw.encode_envelope().unwrap();
    assert_eq!(raw_bytes[0], INDEXED_VALUE_RAW_HEADER);
    assert_eq!(IndexedWireItem::decode_envelope(&raw_bytes).unwrap(), raw);

    let compressed = IndexedWireItem::extract(
        &attributes([
            ("id".to_string(), AttributeValue::S("42".to_string())),
            (
                "payload".to_string(),
                AttributeValue::S("compressible".repeat(512)),
            ),
        ]),
        &declaration(&[]),
    )
    .unwrap();
    let compressed_bytes = compressed.encode_envelope().unwrap();
    assert_eq!(compressed_bytes[0], INDEXED_VALUE_LZ4_HEADER);
    assert_eq!(
        IndexedWireItem::decode_envelope(&compressed_bytes).unwrap(),
        compressed
    );
}

#[test]
fn given_invalid_envelope_header_or_shape_when_decoded_then_corruption_is_reported() {
    for (bytes, invariant) in [
        (Vec::new(), "missing_header"),
        (vec![0x00], "format_version"),
        (vec![0x1f], "payload_codec"),
        (vec![INDEXED_VALUE_RAW_HEADER], "truncated_envelope"),
    ] {
        assert_corruption(
            IndexedWireItem::decode_envelope(&bytes).unwrap_err(),
            invariant,
        );
    }
}

#[test]
fn given_present_missing_unicode_nul_and_indexed_key_when_round_tripped_then_logical_item_is_exact()
{
    let item = attributes([
        (
            "pk".to_string(),
            AttributeValue::S("entity\0顧客".to_string()),
        ),
        ("middle".to_string(), AttributeValue::S("value".to_string())),
        ("tail".to_string(), AttributeValue::S("終".to_string())),
    ]);
    let indexed =
        IndexedWireItem::extract(&item, &declaration(&["pk", "absent", "middle", "tail"])).unwrap();

    assert_eq!(indexed.slots()[1], None);
    assert_eq!(indexed.to_attribute_map().unwrap(), item);
    assert_eq!(
        IndexedWireItem::decode_envelope(&indexed.encode_envelope().unwrap())
            .unwrap()
            .to_attribute_map()
            .unwrap(),
        item
    );
}

/// Fake backend-neutral row mirroring `TursoRowView { columns, values }`:
/// named-column access without leaking a driver value type. Uses the shared
/// default number codec (sqlite/turso sortable text).
struct TestRowView {
    keys: HashMap<String, String>,
    texts: HashMap<String, Option<String>>,
}

impl IndexedRowView for TestRowView {
    fn key_column_text(
        &self,
        attribute_name: &str,
        column_prefix: Option<&str>,
    ) -> crate::StorageResult<String> {
        let mut column = String::new();
        if let Some(prefix) = column_prefix {
            column.push_str(prefix);
        }
        column.push_str(attribute_name);
        self.keys
            .get(&column)
            .cloned()
            .ok_or_else(crate::StorageError::invalid_or_missing_key)
    }

    fn optional_text(&self, column: &str) -> crate::StorageResult<Option<String>> {
        Ok(self.texts.get(column).cloned().flatten())
    }
}

/// Postgres-shaped fake: identical column view, but the native `NUMERIC`
/// plain-text codec returns the input unchanged (backend-local narrow piece).
struct PlainTestRowView(TestRowView);

impl IndexedRowView for PlainTestRowView {
    fn key_column_text(
        &self,
        attribute_name: &str,
        column_prefix: Option<&str>,
    ) -> crate::StorageResult<String> {
        self.0.key_column_text(attribute_name, column_prefix)
    }

    fn optional_text(&self, column: &str) -> crate::StorageResult<Option<String>> {
        self.0.optional_text(column)
    }

    fn decode_number_key(raw: &str) -> crate::StorageResult<String>
    where Self: Sized {
        Ok(raw.to_owned())
    }
}

fn table_info_fixture(
    key_schema: Vec<(&str, KeyType)>,
    definitions: Vec<(&str, KeyAttributeType)>,
    max_indexers: u8,
) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("row_view"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::now(),
        attribute_definitions: definitions
            .into_iter()
            .map(|(name, kind)| AttributeDefinition {
                attribute_name: name.to_string(),
                attribute_type: kind,
            })
            .collect(),
        key_schema: key_schema
            .into_iter()
            .map(|(name, key_type)| KeySchemaElement {
                attribute_name: name.to_string(),
                key_type,
            })
            .collect(),
        max_indexers: MaxIndexers::try_new(max_indexers).unwrap(),
        global_secondary_indexes: None,
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        table_stream_duration: StreamRetentionDuration::default(),
        default_item_stream_duration: StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

fn row_view_from_indexed(
    keys: &[(&str, &str)],
    indexed: &IndexedWireItem,
    capacity: usize,
) -> TestRowView {
    let (residual, slots) = indexed.clone().into_parts();
    let residual = String::from_utf8(residual).unwrap();
    let mut texts = HashMap::new();
    texts.insert("attributes_blob".to_string(), Some(residual));
    for (ordinal, slot) in slots.iter().enumerate() {
        texts.insert(format!("__aux_indexer_{ordinal}"), slot.clone());
    }
    for ordinal in slots.len()..capacity {
        texts.insert(format!("__aux_indexer_{ordinal}"), None);
    }
    TestRowView {
        keys: keys
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect(),
        texts,
    }
}

#[test]
fn given_s_b_and_missing_definition_keys_when_decoded_then_shared_matches_direct_reconstruct() {
    let table_info = table_info_fixture(
        vec![("pk", KeyType::Hash), ("bin", KeyType::Range)],
        vec![("pk", KeyAttributeType::S), ("bin", KeyAttributeType::B)],
        2,
    );
    let item = attributes([
        ("pk".to_string(), AttributeValue::S("entity#1".to_string())),
        ("bin".to_string(), AttributeValue::B("AA==".to_string())),
        (
            "customer_id".to_string(),
            AttributeValue::S("c-1".to_string()),
        ),
    ]);
    let declaration = declaration(&["customer_id", "absent"]);
    let indexed = IndexedWireItem::extract(&item, &declaration).unwrap();
    // `extra` has no attribute definition: shared defaults to `S`,
    // matching sqlite/turso.
    let view = row_view_from_indexed(&[("pk", "entity#1"), ("bin", "AA==")], &indexed, 2);

    let (keys, decoded) =
        IndexedWireItem::decode_keyed_row(&view, &table_info, None, None).unwrap();
    assert_eq!(
        keys.get("pk"),
        Some(&AttributeValue::S("entity#1".to_string()))
    );
    assert_eq!(
        keys.get("bin"),
        Some(&AttributeValue::B("AA==".to_string()))
    );
    assert_eq!(decoded.item.to_attribute_map().unwrap(), item);
    assert_eq!(decoded.indexers, vec!["customer_id", "absent"]);
}

#[test]
fn given_encoded_and_plain_number_keys_when_decoded_then_sqlite_and_postgres_views_agree() {
    let table_info = table_info_fixture(
        vec![("pk", KeyType::Hash)],
        vec![("pk", KeyAttributeType::N)],
        0,
    );
    let encoded = crate::encode_dynamodb_number_key("42").unwrap();
    let item = attributes([("pk".to_string(), AttributeValue::N("42".to_string()))]);
    let indexed = IndexedWireItem::extract(&item, &declaration(&[])).unwrap();

    let sqlite_view = row_view_from_indexed(&[("pk", encoded.as_str())], &indexed, 0);
    let (_, sqlite_decoded) =
        IndexedWireItem::decode_keyed_row(&sqlite_view, &table_info, None, None).unwrap();
    assert_eq!(
        sqlite_decoded.item.to_attribute_map().unwrap(),
        item.clone()
    );

    let postgres_view = PlainTestRowView(row_view_from_indexed(&[("pk", "42")], &indexed, 0));
    let postgres_decoded = IndexedWireItem::decode_row_for_origin(
        &postgres_view,
        &table_info,
        &table_info.key_schema,
        None,
    )
    .unwrap();
    assert_eq!(postgres_decoded.item.to_attribute_map().unwrap(), item);
    assert_eq!(
        sqlite_decoded.item.to_attribute_map().unwrap(),
        postgres_decoded.item.to_attribute_map().unwrap()
    );
}

#[test]
fn given_missing_key_column_when_decoded_then_frozen_validation_is_preserved() {
    let table_info = table_info_fixture(
        vec![("pk", KeyType::Hash)],
        vec![("pk", KeyAttributeType::S)],
        0,
    );
    let view = TestRowView {
        keys: HashMap::new(),
        texts: HashMap::from([("attributes_blob".to_string(), Some("{}".to_string()))]),
    };
    let error = IndexedWireItem::decode_keyed_row(&view, &table_info, None, None)
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("Invalid or missing key"),
        "frozen validation string must not change: {error}"
    );
}

#[test]
fn given_prefixed_main_and_index_schemas_when_decoded_then_keys_merge_identically() {
    let table_info = table_info_fixture(
        vec![("pk", KeyType::Hash)],
        vec![("pk", KeyAttributeType::S), ("gsi_pk", KeyAttributeType::S)],
        1,
    );
    let item = attributes([
        ("pk".to_string(), AttributeValue::S("entity#1".to_string())),
        (
            "gsi_pk".to_string(),
            AttributeValue::S("active".to_string()),
        ),
    ]);
    let indexed = IndexedWireItem::extract(&item, &declaration(&[])).unwrap();
    let gsi_schema = vec![KeySchemaElement {
        attribute_name: "gsi_pk".to_string(),
        key_type: KeyType::Hash,
    }];

    // sqlite GSI shape: main keys with `table_` prefix plus index keys.
    let sqlite_view = {
        let (residual, slots) = indexed.clone().into_parts();
        let mut texts = HashMap::from([(
            "attributes_blob".to_string(),
            Some(String::from_utf8(residual).unwrap()),
        )]);
        for (ordinal, slot) in slots.iter().enumerate() {
            texts.insert(format!("__aux_indexer_{ordinal}"), slot.clone());
        }
        TestRowView {
            keys: HashMap::from([
                ("table_pk".to_string(), "entity#1".to_string()),
                ("gsi_pk".to_string(), "active".to_string()),
            ]),
            texts,
        }
    };
    let (keys, decoded) = IndexedWireItem::decode_keyed_row(
        &sqlite_view,
        &table_info,
        Some("table_"),
        Some(&gsi_schema),
    )
    .unwrap();
    assert_eq!(
        keys.get("pk"),
        Some(&AttributeValue::S("entity#1".to_string()))
    );
    assert_eq!(
        keys.get("gsi_pk"),
        Some(&AttributeValue::S("active".to_string()))
    );

    // postgres/turso origin shape: primary plus `table_`-prefixed secondary.
    let origin_view = {
        let (residual, slots) = indexed.clone().into_parts();
        let mut texts = HashMap::from([(
            "attributes_blob".to_string(),
            Some(String::from_utf8(residual).unwrap()),
        )]);
        for (ordinal, slot) in slots.iter().enumerate() {
            texts.insert(format!("__aux_indexer_{ordinal}"), slot.clone());
        }
        TestRowView {
            keys: HashMap::from([
                ("gsi_pk".to_string(), "active".to_string()),
                ("table_pk".to_string(), "entity#1".to_string()),
            ]),
            texts,
        }
    };
    let origin_decoded = IndexedWireItem::decode_row_for_origin(
        &origin_view,
        &table_info,
        &gsi_schema,
        Some(table_info.key_schema.as_slice()),
    )
    .unwrap();
    assert_eq!(
        decoded.item.to_attribute_map().unwrap(),
        origin_decoded.item.to_attribute_map().unwrap()
    );
}

#[test]
fn given_bad_slots_or_mismatched_keys_when_reconstructed_then_corruption_is_preserved() {
    let keys = KeyAttributes::from([("pk".to_string(), AttributeValue::S("a".to_string()))]);
    let capacity = MaxIndexers::try_new(1).unwrap();
    assert_corruption(
        IndexedWireItem::reconstruct_with_key_attributes(
            "{}".to_string(),
            Vec::new(),
            &keys,
            capacity,
        )
        .err()
        .unwrap(),
        "sql_slot_count",
    );

    let item = attributes([("pk".to_string(), AttributeValue::S("a".to_string()))]);
    let indexed = IndexedWireItem::extract(&item, &declaration(&[])).unwrap();
    let (residual, slots) = indexed.into_parts();
    let wrong_keys = KeyAttributes::from([("pk".to_string(), AttributeValue::S("b".to_string()))]);
    assert_corruption(
        IndexedWireItem::reconstruct_with_key_attributes(
            String::from_utf8(residual).unwrap(),
            slots,
            &wrong_keys,
            MaxIndexers::try_new(0).unwrap(),
        )
        .err()
        .unwrap(),
        "indexed_key_mismatch",
    );
}
