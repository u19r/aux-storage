use serde::Serialize;
use storage_derive::{SingleTableKeys, WireItemEncode};

use crate::{
    AttributeValue, ReadSequenceInputCardinality, ReadSequenceOnMissing, SingleTableEntity,
    TimestampMillis,
    single_table_entity::{to_item_map, to_wire_entity},
};

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
