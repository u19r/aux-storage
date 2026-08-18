use std::collections::HashMap;

use crate::{
    AttributeValue, INDEXED_VALUE_LZ4_HEADER, INDEXED_VALUE_RAW_HEADER, INDEXER_TUPLE_OFFSET,
    IndexedWireItem, IndexerDeclaration, MAX_INDEXERS_CAPACITY, MaxIndexers, StorageEnum,
    context::WrappedError,
};

fn declaration(names: &[&str]) -> IndexerDeclaration {
    IndexerDeclaration::try_new(
        names.iter().map(|name| (*name).to_string()).collect(),
        MaxIndexers::try_new(32).expect("capacity"),
    )
    .expect("declaration")
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
    let item = HashMap::from([
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
    .expect("extract");

    assert_eq!(
        indexed.slots(),
        &[Some("c-1".to_string()), None, Some("eu".to_string())]
    );
    let (logical, reconstructed_declaration) = indexed
        .into_attribute_map_with_declaration()
        .expect("reconstruct");
    assert_eq!(logical, item);
    assert_eq!(
        reconstructed_declaration.names(),
        &["customer_id", "optional_id", "region_id"]
    );
}

#[test]
fn given_non_string_indexed_value_when_extracted_then_validation_fails() {
    let item = HashMap::from([(
        "customer_id".to_string(),
        AttributeValue::N("1".to_string()),
    )]);

    let error = IndexedWireItem::extract(&item, &declaration(&["customer_id"]))
        .expect_err("non-string must fail");

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
        let item = HashMap::from([("indexed".to_string(), value)]);
        let error = IndexedWireItem::extract(&item, &declaration(&["indexed"]))
            .expect_err("only a non-empty S value may be indexed");
        assert!(
            error
                .to_string()
                .contains("Indexers:attribute_must_be_string")
        );
    }
}

#[test]
fn given_empty_indexed_string_when_extracted_then_validation_fails() {
    let item = HashMap::from([("customer_id".to_string(), AttributeValue::S(String::new()))]);

    let error = IndexedWireItem::extract(&item, &declaration(&["customer_id"]))
        .expect_err("empty string must fail");

    assert!(error.to_string().contains("Indexers:empty_string"));
}

#[test]
fn given_duplicate_declaration_when_validated_then_validation_fails() {
    let error = IndexerDeclaration::try_new(
        vec!["id".to_string(), "id".to_string()],
        MaxIndexers::try_new(2).expect("capacity"),
    )
    .expect_err("duplicate must fail");

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
        IndexerDeclaration::try_new(names.clone(), MaxIndexers::try_new(4).expect("capacity"))
            .expect("valid declaration");

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
            MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("maximum capacity")
        )
        .is_ok()
    );
    let mut too_many = names;
    too_many.push("overflow".to_string());
    let error = IndexerDeclaration::try_new(
        too_many,
        MaxIndexers::try_new(MAX_INDEXERS_CAPACITY).expect("maximum capacity"),
    )
    .expect_err("maximum plus one must fail");
    assert!(error.to_string().contains("Indexers:too_many"));
}

#[test]
fn given_gapped_markers_when_decoded_then_corruption_is_reported() {
    let residual = serde_json::to_vec(&serde_json::json!({
        "a": {"I": INDEXER_TUPLE_OFFSET},
        "b": {"I": INDEXER_TUPLE_OFFSET + 2}
    }))
    .expect("residual");

    let error = IndexedWireItem::from_parts(residual, vec![None, None])
        .expect_err("gapped markers must fail");

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
        let residual = serde_json::to_vec(&residual).expect("residual JSON");
        assert_corruption(
            IndexedWireItem::from_parts(residual, vec![None])
                .expect_err("malformed marker must fail"),
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
    let item = HashMap::from([("id".to_string(), AttributeValue::S("42".to_string()))]);
    let indexed =
        IndexedWireItem::extract(&item, &declaration(&["id", "missing"])).expect("extract");

    let bytes = indexed.encode_envelope().expect("encode");
    let decoded = IndexedWireItem::decode_envelope(&bytes).expect("decode");

    assert_eq!(decoded, indexed);
    assert_eq!(decoded.to_attribute_map().expect("logical item"), item);
}

#[test]
fn given_raw_and_compressible_items_when_enveloped_then_header_and_payload_round_trip() {
    let raw = IndexedWireItem::extract(
        &HashMap::from([("id".to_string(), AttributeValue::S("42".to_string()))]),
        &declaration(&[]),
    )
    .expect("raw item");
    let raw_bytes = raw.encode_envelope().expect("raw envelope");
    assert_eq!(raw_bytes[0], INDEXED_VALUE_RAW_HEADER);
    assert_eq!(IndexedWireItem::decode_envelope(&raw_bytes).unwrap(), raw);

    let compressed = IndexedWireItem::extract(
        &HashMap::from([
            ("id".to_string(), AttributeValue::S("42".to_string())),
            (
                "payload".to_string(),
                AttributeValue::S("compressible".repeat(512)),
            ),
        ]),
        &declaration(&[]),
    )
    .expect("compressible item");
    let compressed_bytes = compressed.encode_envelope().expect("compressed envelope");
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
            IndexedWireItem::decode_envelope(&bytes).expect_err("invalid envelope must fail"),
            invariant,
        );
    }
}

#[test]
fn given_present_missing_unicode_nul_and_indexed_key_when_round_tripped_then_logical_item_is_exact()
{
    let item = HashMap::from([
        (
            "pk".to_string(),
            AttributeValue::S("entity\0顧客".to_string()),
        ),
        ("middle".to_string(), AttributeValue::S("value".to_string())),
        ("tail".to_string(), AttributeValue::S("終".to_string())),
    ]);
    let indexed =
        IndexedWireItem::extract(&item, &declaration(&["pk", "absent", "middle", "tail"]))
            .expect("indexed item");

    assert_eq!(indexed.slots()[1], None);
    assert_eq!(indexed.to_attribute_map().expect("logical item"), item);
    assert_eq!(
        IndexedWireItem::decode_envelope(&indexed.encode_envelope().expect("envelope"))
            .expect("decode")
            .to_attribute_map()
            .expect("decoded logical item"),
        item
    );
}
