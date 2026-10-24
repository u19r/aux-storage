use alloc_counter::AllocationGuard;
use serde::Serialize;
use storage_derive::{SingleTableKeys, WireItemEncode};

use crate::{
    AttributeValue, ReadSequenceInputCardinality, ReadSequenceOnMissing, SingleTableEntity,
    TimestampMillis, WireItem,
    single_table_entity::{to_item_map, to_wire_entity, to_wire_item_fast},
};

#[derive(Debug, Serialize, SingleTableKeys, WireItemEncode)]
#[single_table(
    entity_type = "ORDERED",
    pk_lit = "ORDERED",
    sk_expr = "self.id.clone()",
    gsi1_pk_expr = "Some(self.id.clone())",
    gsi1_sk_expr = "Some(self.id.clone())",
    gsi2_pk_expr = "self.middle_gsis.then(|| self.id.clone())",
    gsi2_sk_expr = "self.middle_gsis.then(|| self.id.clone())",
    gsi3_pk_expr = "Some(self.id.clone())",
    gsi3_sk_expr = "Some(self.id.clone())",
    gsi4_pk_expr = "self.middle_gsis.then(|| self.id.clone())",
    gsi4_sk_expr = "self.middle_gsis.then(|| self.id.clone())",
    gsi5_pk_expr = "Some(self.id.clone())",
    gsi5_sk_expr = "Some(self.id.clone())"
)]
struct OrderedEntity {
    payload: String,
    middle_gsis: bool,
    updated_at: TimestampMillis,
    #[single_table(indexer = 1)]
    #[serde(rename = "second", skip_serializing_if = "Option::is_none")]
    later: Option<String>,
    id: String,
    expires_at: crate::TimestampSeconds,
    #[single_table(indexer = 0)]
    #[serde(rename = "first")]
    earlier: String,
}

fn ordered_entity() -> OrderedEntity {
    OrderedEntity {
        payload: "large payload".repeat(128),
        middle_gsis: false,
        updated_at: TimestampMillis::from_timestamp(1),
        later: Some("second value".to_string()),
        id: "item".to_string(),
        expires_at: crate::TimestampSeconds::from_timestamp(4_102_444_800),
        earlier: "first value".to_string(),
    }
}

#[test]
fn given_scrambled_entity_when_encoded_then_lookup_attributes_precede_payload() {
    let entity = ordered_entity();
    let (item, indexers) = to_wire_entity(&entity).unwrap().into_write_parts();
    let attributes = item.to_attribute_map().unwrap();
    assert_eq!(indexers.unwrap(), ["first", "second"]);
    assert_eq!(
        attributes.keys().collect::<Vec<_>>(),
        [
            "pk",
            "sk",
            "gsi1pk",
            "gsi1sk",
            "gsi3pk",
            "gsi3sk",
            "gsi5pk",
            "gsi5sk",
            "e_at",
            "first",
            "second",
            "id",
            "middle_gsis",
            "payload",
            "u_at",
            "et"
        ]
    );
}

#[test]
fn given_scrambled_entity_when_manually_serialized_then_lookup_attributes_precede_payload() {
    let item = super::single_table_entity::to_wire_item(&ordered_entity()).unwrap();
    let attributes = item.to_attribute_map().unwrap();
    assert_eq!(
        attributes.keys().take(11).collect::<Vec<_>>(),
        [
            "pk", "sk", "gsi1pk", "gsi1sk", "gsi3pk", "gsi3sk", "gsi5pk", "gsi5sk", "e_at",
            "first", "second"
        ]
    );
}

#[test]
fn given_all_gsis_and_absent_indexer_when_encoded_then_pairs_precede_ttl_and_remaining_indexer() {
    let mut entity = ordered_entity();
    entity.middle_gsis = true;
    entity.later = None;
    for item in [
        to_wire_item_fast(&entity).unwrap(),
        super::single_table_entity::to_wire_item(&entity).unwrap(),
    ] {
        let attributes = item.to_attribute_map().unwrap();
        assert_eq!(
            attributes.keys().take(14).collect::<Vec<_>>(),
            [
                "pk", "sk", "gsi1pk", "gsi1sk", "gsi2pk", "gsi2sk", "gsi3pk", "gsi3sk", "gsi4pk",
                "gsi4sk", "gsi5pk", "gsi5sk", "e_at", "first"
            ]
        );
        assert!(!attributes.contains_key("second"));
    }
}

#[test]
fn given_ordered_entity_when_stored_and_restored_then_lookup_order_survives() {
    let (item, indexers) = to_wire_entity(&ordered_entity())
        .unwrap()
        .into_write_parts();
    let declaration = crate::IndexerDeclaration::try_new(
        indexers.unwrap(),
        crate::MaxIndexers::try_from(2).unwrap(),
    )
    .unwrap();
    let stored = crate::IndexedWireItem::extract_wire_items(&item, &item, &declaration).unwrap();
    let persisted: crate::VecMap<String, serde_json::Value> =
        serde_json::from_slice(stored.residual_json()).unwrap();
    assert_eq!(
        persisted
            .keys()
            .map(String::as_str)
            .take(11)
            .collect::<Vec<_>>(),
        [
            "pk", "sk", "gsi1pk", "gsi1sk", "gsi3pk", "gsi3sk", "gsi5pk", "gsi5sk", "e_at",
            "first", "second"
        ]
    );
    let bytes = stored.encode_envelope().unwrap();
    let restored = crate::IndexedWireItem::decode_envelope(&bytes)
        .unwrap()
        .into_wire_item_with_declaration()
        .unwrap()
        .0;
    assert_eq!(
        restored.to_attribute_map().unwrap(),
        item.to_attribute_map().unwrap()
    );
    assert_eq!(
        restored
            .to_attribute_map()
            .unwrap()
            .keys()
            .take(11)
            .collect::<Vec<_>>(),
        persisted
            .keys()
            .map(String::as_str)
            .take(11)
            .collect::<Vec<_>>()
    );
}

#[derive(Debug, Serialize, SingleTableKeys, WireItemEncode)]
#[single_table(
    entity_type = "ORDER",
    pk_lit = "ORDER",
    sk_expr = "format!(\"ORDER#{}\", self.order_id)"
)]
struct Order {
    order_id: String,
    #[single_table(indexer = 0)]
    customer_id: String,
    #[single_table(indexer = 1)]
    #[wire_item(rename = "related_order_id")]
    related_id: Option<String>,
    updated_at: TimestampMillis,
}

#[test]
fn derived_indexers_use_ordered_wire_names_tests() {
    assert_eq!(Order::customer_id_indexer().attribute_name(), "customer_id");
    assert_eq!(Order::customer_id_indexer().ordinal(), 0);
    assert_eq!(
        Order::related_id_indexer().attribute_name(),
        "related_order_id"
    );
    assert_eq!(Order::related_id_indexer().ordinal(), 1);
    assert_eq!(Order::INDEXERS.len(), 2);
}

#[test]
fn wire_entity_carries_generated_declaration_into_write_parts_tests() {
    let entity = Order {
        order_id: "123".to_string(),
        customer_id: "customer#456".to_string(),
        related_id: None,
        updated_at: TimestampMillis::from_timestamp(1),
    };

    let wire_entity = to_wire_entity(&entity).expect("encode entity");
    let (item, indexers) = wire_entity.into_write_parts();
    let item = item.to_attribute_map().expect("decode entity");

    assert_eq!(
        indexers,
        Some(vec![
            "customer_id".to_string(),
            "related_order_id".to_string()
        ])
    );
    assert_eq!(
        item.get("customer_id"),
        Some(&AttributeValue::S("customer#456".to_string()))
    );
    assert_eq!(
        item.get("pk"),
        Some(&AttributeValue::S("ORDER".to_string()))
    );
    assert_eq!(
        item.get("sk"),
        Some(&AttributeValue::S("ORDER#123".to_string()))
    );
}

fn legacy_item_only_encoding(entity: &Order) -> WireItem {
    to_wire_entity(entity)
        .expect("encode entity")
        .into_write_parts()
        .0
}

fn measure_item_only_encoding(
    label: &'static str,
    entity: &Order,
    encode: fn(&Order) -> Result<WireItem, crate::ConversionError>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "single_table_item_only_allocation_profile_tests",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let item = encode(entity).expect("encode item");
        std::hint::black_box(item.payload_len());
    }
    guard.finish()
}

#[test]
fn given_indexed_entity_when_encoding_item_only_then_names_are_not_allocated() {
    let entity = Order {
        order_id: "123".to_string(),
        customer_id: "customer#456".to_string(),
        related_id: None,
        updated_at: TimestampMillis::from_timestamp(1),
    };
    let legacy = measure_item_only_encoding("legacy_write_parts", &entity, |entity| {
        Ok(legacy_item_only_encoding(entity))
    });
    let direct = measure_item_only_encoding("direct_item", &entity, to_wire_item_fast::<Order>);

    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&direct);
    assert!(direct.allocation_count < legacy.allocation_count);
    assert!(direct.allocated_bytes < legacy.allocated_bytes);
}

#[test]
fn item_map_uses_canonical_timestamp_aliases_tests() {
    let entity = Order {
        order_id: "123".to_string(),
        customer_id: "customer#456".to_string(),
        related_id: None,
        updated_at: TimestampMillis::from_timestamp(1),
    };

    let item = to_item_map(&entity).expect("encode item map");

    assert_eq!(item.get("u_at"), Some(&AttributeValue::N("1".to_string())));
    assert!(!item.contains_key("updated_at"));
}

#[test]
fn entity_indexer_builds_matching_one_and_many_inputs_tests() {
    let input = Order::customer_id_indexer().many_from_query("orders", ReadSequenceOnMissing::Skip);

    assert_eq!(input.from.node, "orders");
    assert_eq!(input.from.select.0, "$.Query.Items[*].customer_id");
    assert_eq!(input.cardinality, ReadSequenceInputCardinality::Many);
    assert_eq!(input.on_missing, ReadSequenceOnMissing::Skip);
    let source = input.mapped_key_source.expect("mapped source");
    assert_eq!(source.attribute_name(), "customer_id");
    assert_eq!(source.indexer(), 0);

    let one = Order::customer_id_indexer().one_from_query("order", ReadSequenceOnMissing::Error);
    assert_eq!(one.from.select.0, "$.Query.Items[0].customer_id");
    assert_eq!(one.cardinality, ReadSequenceInputCardinality::One);
    assert_eq!(one.on_missing, ReadSequenceOnMissing::Error);
}

#[derive(Debug, Serialize, SingleTableKeys, WireItemEncode)]
#[single_table(entity_type = "LOOKUP", pk_lit = "LOOKUP", sk_expr = "self.sk.clone()")]
struct Lookup {
    sk: String,
    assignments: Vec<String>,
    updated_at: TimestampMillis,
}

#[test]
fn given_entity_payload_key_when_encoding_envelope_then_key_is_unique_and_payload_is_preserved() {
    let entity = Lookup {
        sk: "C#000001".to_string(),
        assignments: vec!["role\"one".to_string(), "role_two".to_string()],
        updated_at: TimestampMillis::from_timestamp(1),
    };
    let item = to_wire_entity(&entity)
        .expect("encode lookup")
        .into_write_parts()
        .0;
    let bytes = item.dynamo_json_bytes().expect("wire bytes");
    let text = std::str::from_utf8(&bytes).expect("UTF-8");
    assert_eq!(text.matches("\"sk\":").count(), 1);
    assert_eq!(
        item.string_attribute("sk").expect("sort key").as_deref(),
        Some("C#000001")
    );
    assert_eq!(
        item.attribute_value("assignments").expect("assignments"),
        Some(AttributeValue::L(vec![
            AttributeValue::S("role\"one".to_string()),
            AttributeValue::S("role_two".to_string()),
        ]))
    );
}

#[test]
fn given_a_stored_entity_when_its_type_or_keys_disagree_then_identity_validation_rejects_it() {
    let entity = ordered_entity();
    let canonical = to_wire_item_fast(&entity).unwrap();
    entity.validate_stored_identity(&canonical).unwrap();
    for (field, replacement) in [
        ("pk", Some(AttributeValue::S("another-entity".into()))),
        ("sk", Some(AttributeValue::S("another-item".into()))),
        ("et", Some(AttributeValue::S("another-type".into()))),
        ("pk", None),
        ("et", None),
    ] {
        let mut attributes = canonical.to_attribute_map().unwrap();
        if let Some(replacement) = replacement {
            attributes.insert(field, replacement);
        } else {
            attributes.remove(field);
        }
        let source = WireItem::dynamo_json(serde_json::to_vec(&attributes).unwrap());
        assert!(
            entity.validate_stored_identity(&source).is_err(),
            "invalid {field}"
        );
    }
}

#[test]
fn given_a_legacy_entity_type_when_the_stored_identity_is_validated_then_the_named_format_remains_readable()
 {
    let entity = ordered_entity();
    let mut attributes = to_wire_item_fast(&entity)
        .unwrap()
        .to_attribute_map()
        .unwrap();
    let value = attributes.remove("et").unwrap();
    attributes.insert("entity_type", value);
    let source = WireItem::dynamo_json(serde_json::to_vec(&attributes).unwrap());
    entity.validate_stored_identity(&source).unwrap();
}

#[test]
fn given_a_split_row_with_substituted_physical_keys_when_identity_is_validated_then_residual_metadata_cannot_hide_the_substitution()
 {
    let entity = ordered_entity();
    let source = WireItem::local_split(
        crate::WireItemKeyAttributes::new(
            "pk".into(),
            AttributeValue::S("substituted-physical-key".into()),
            Some("sk".into()),
            Some(AttributeValue::S(entity.sk())),
        ),
        None,
        Some(
            serde_json::to_vec(
                &to_wire_item_fast(&entity)
                    .unwrap()
                    .to_attribute_map()
                    .unwrap(),
            )
            .unwrap(),
        ),
    );
    assert!(entity.validate_stored_identity(&source).is_err());
}

#[test]
fn given_a_valid_entity_when_an_independent_address_disagrees_then_destination_identity_is_rejected()
 {
    let entity = ordered_entity();
    let table = crate::TableName::new("identity_source");
    let canonical = crate::TableKey::new(
        table.clone(),
        AttributeValue::S(entity.pk()),
        Some(AttributeValue::S(entity.sk())),
    );
    entity
        .validate_addressed_identity(&table, &crate::ItemKey::Table(canonical.clone()))
        .unwrap();
    let mut foreign_table = canonical.clone();
    foreign_table.table_name = crate::TableName::new("another_tenant");
    let mut foreign_pk = canonical.clone();
    foreign_pk.hash_key = AttributeValue::S("another_entity".into());
    let mut foreign_sk = canonical.clone();
    foreign_sk.range_key = Some(AttributeValue::S("another_item".into()));
    let mut missing_sk = canonical;
    missing_sk.range_key = None;
    for key in [foreign_table, foreign_pk, foreign_sk, missing_sk] {
        assert!(
            entity
                .validate_addressed_identity(&table, &crate::ItemKey::Table(key))
                .is_err()
        );
    }
}
