use std::collections::HashMap;

use foundationdb::tuple::{Element, unpack};
use storage_provider::{
    ReadSequenceFlatResult, ReadSequenceMappedEntry, ReadSequenceMappedKeyValue,
    ReadSequenceMappedOptions, ReadSequenceMappedRangePage, ReadSequenceMappedRejectionReason,
    ReadSequenceUnsupportedReason, select_read_sequence_mapped_edges,
};
use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexKey, IndexName, ItemKey,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, QueryRequest,
    ReadSequenceConsistency, ReadSequenceNodeOperation, ReadSequenceRequest, StoredTableInfo,
    TableKey, TableName, TableStatus, TimestampMillis, context::WrappedError as _,
    plan_read_sequence,
};

use crate::{
    keyspace::{
        compact::TableStorageId,
        table_identity::{StoredTableMetadata, TableIdentity},
        table_keys,
    },
    storage_ops::provider_impl::{
        read_sequence_mapped::{MappedGetQueryShape, MappedSequenceShape},
        read_sequence_mapped_bounds::mapped_query_bounds,
        read_sequence_mapped_descriptors::mapped_get_query_descriptors,
        read_sequence_mapped_layout::{mapped_get_query_physical_layout, mapped_physical_layout},
        read_sequence_mapped_rows::{
            mapped_edge_rows, mapped_get_query_rows, mapped_template_matches,
        },
    },
};

fn schema(hash: &str, range: Option<&str>) -> Vec<KeySchemaElement> {
    let mut schema = vec![KeySchemaElement {
        attribute_name: hash.to_string(),
        key_type: KeyType::Hash,
    }];
    if let Some(range) = range {
        schema.push(KeySchemaElement {
            attribute_name: range.to_string(),
            key_type: KeyType::Range,
        });
    }
    schema
}

fn metadata(
    name: &str,
    id: u16,
    table_schema: Vec<KeySchemaElement>,
    gsi: Option<GlobalSecondaryIndex>,
) -> StoredTableMetadata {
    let mut definitions = table_schema
        .iter()
        .map(|key| AttributeDefinition {
            attribute_name: key.attribute_name.clone(),
            attribute_type: KeyAttributeType::S,
        })
        .collect::<Vec<_>>();
    if let Some(index) = gsi.as_ref() {
        for key in &index.key_schema {
            if !definitions
                .iter()
                .any(|definition| definition.attribute_name == key.attribute_name)
            {
                definitions.push(AttributeDefinition {
                    attribute_name: key.attribute_name.clone(),
                    attribute_type: KeyAttributeType::S,
                });
            }
        }
    }
    let table_name = TableName::new(name);
    let indexes = gsi.clone().map(|index| vec![index]);
    StoredTableMetadata::active(
        TableIdentity::user_indexes_for_table(
            TableStorageId::new(u32::from(id)),
            &table_name,
            indexes.as_deref(),
        ),
        StoredTableInfo {
            table_name,
            table_status: TableStatus::Active,
            created_at: TimestampMillis::from_timestamp(0),
            attribute_definitions: definitions,
            key_schema: table_schema,
            max_indexers: storage_types::MaxIndexers::ZERO,
            global_secondary_indexes: indexes,
            table_size_bytes: 0,
            item_count: 0,
            stream_specification: None,
            table_stream_duration: storage_types::StreamRetentionDuration::default(),
            default_item_stream_duration: storage_types::StreamRetentionDuration::default(),
            deletion_protection_enabled: false,
        },
    )
}

fn gsi(projection_type: ProjectionType) -> GlobalSecondaryIndex {
    GlobalSecondaryIndex {
        index_name: IndexName::new("status"),
        key_schema: schema("gsi_pk", Some("created")),
        projection: Projection {
            projection_type: Some(projection_type),
            non_key_attributes: None,
        },
    }
}

fn query(expression: &str, values: &[(&str, AttributeValue)]) -> QueryRequest {
    let mut request = QueryRequest::new(TableName::new("orders"), expression.to_string());
    request.index_name = Some(IndexName::new("status"));
    request.expression_attribute_values = Some(
        values
            .iter()
            .map(|(name, value)| ((*name).to_string(), value.clone()))
            .collect::<HashMap<_, _>>(),
    );
    request
}

#[test]
fn gsi_between_bounds_include_every_base_key_at_the_upper_value() {
    let metadata = metadata(
        "orders",
        1,
        schema("pk", Some("sk")),
        Some(gsi(ProjectionType::All)),
    );
    let request = query(
        "gsi_pk = :status AND created BETWEEN :lo AND :hi",
        &[
            (":status", AttributeValue::S("open".to_string())),
            (":lo", AttributeValue::S("a".to_string())),
            (":hi", AttributeValue::S("z".to_string())),
        ],
    );
    let range = mapped_query_bounds(&metadata, &request)
        .expect("bounds")
        .expect("supported between");
    let upper = gsi_item_key(&metadata, "z", "tenant", "last");
    let after = gsi_item_key(&metadata, "z2", "tenant", "first");
    assert!(upper >= range.begin && upper < range.end);
    assert!(after >= range.end);
}

#[test]
fn gsi_comparison_bounds_apply_to_every_base_key_suffix() {
    let metadata = metadata(
        "orders",
        1,
        schema("pk", Some("sk")),
        Some(gsi(ProjectionType::All)),
    );
    let before = gsi_item_key(&metadata, "a", "tenant", "first");
    let equal_first = gsi_item_key(&metadata, "m", "tenant", "first");
    let equal_last = gsi_item_key(&metadata, "m", "tenant", "last");
    let after = gsi_item_key(&metadata, "z", "tenant", "first");
    for (operator, expected) in [
        ("<", [true, false, false, false]),
        ("<=", [true, true, true, false]),
        (">", [false, false, false, true]),
        (">=", [false, true, true, true]),
    ] {
        let request = query(
            &format!("gsi_pk = :status AND created {operator} :value"),
            &[
                (":status", AttributeValue::S("open".to_string())),
                (":value", AttributeValue::S("m".to_string())),
            ],
        );
        let range = mapped_query_bounds(&metadata, &request)
            .expect("bounds")
            .expect("comparison");
        let actual = [&before, &equal_first, &equal_last, &after]
            .map(|key| key >= &range.begin && key < &range.end);
        assert_eq!(actual, expected, "operator {operator}");
    }
}

#[test]
fn gsi_bounded_comparison_preserves_inclusive_and_exclusive_bounds() {
    let metadata = metadata(
        "orders",
        1,
        schema("pk", Some("sk")),
        Some(gsi(ProjectionType::All)),
    );
    let request = query(
        "gsi_pk = :status AND created > :lower AND created <= :upper",
        &[
            (":status", AttributeValue::S("open".to_string())),
            (":lower", AttributeValue::S("a".to_string())),
            (":upper", AttributeValue::S("z".to_string())),
        ],
    );
    let range = mapped_query_bounds(&metadata, &request)
        .expect("bounds")
        .expect("bounded comparison");
    assert!(!contains(
        &range,
        &gsi_item_key(&metadata, "a", "tenant", "last")
    ));
    assert!(contains(
        &range,
        &gsi_item_key(&metadata, "m", "tenant", "first")
    ));
    assert!(contains(
        &range,
        &gsi_item_key(&metadata, "z", "tenant", "last")
    ));
}

#[test]
fn reverse_base_range_uses_the_full_exclusive_start_key() {
    let metadata = metadata("orders", 1, schema("pk", Some("sk")), None);
    let mut request = QueryRequest::new(TableName::new("orders"), "pk = :pk".to_string());
    request.expression_attribute_values = Some(HashMap::from([(
        ":pk".to_string(),
        AttributeValue::S("tenant".to_string()),
    )]));
    request.scan_index_forward = Some(false);
    request.exclusive_start_key = Some(
        storage_types::KeyAttributes::from_iter([
            ("pk".to_string(), AttributeValue::S("tenant".to_string())),
            ("sk".to_string(), AttributeValue::S("event-2".to_string())),
        ])
        .into(),
    );
    let range = mapped_query_bounds(&metadata, &request)
        .expect("bounds")
        .expect("base range");
    assert!(range.reverse);
    assert_eq!(
        range.exclusive_start,
        Some(
            table_keys::item_key(
                &metadata.identity,
                &ItemKey::Table(TableKey::new(
                    TableName::new("orders"),
                    AttributeValue::S("tenant".to_string()),
                    Some(AttributeValue::S("event-2".to_string())),
                )),
            )
            .expect("physical key")
        )
    );
}

#[test]
fn base_begins_with_range_matches_only_the_requested_sort_key_prefix() {
    let metadata = metadata("orders", 1, schema("pk", Some("sk")), None);
    let mut request = QueryRequest::new(
        TableName::new("orders"),
        "pk = :pk AND begins_with(sk, :prefix)".to_string(),
    );
    request.expression_attribute_values = Some(HashMap::from([
        (":pk".to_string(), AttributeValue::S("tenant".to_string())),
        (
            ":prefix".to_string(),
            AttributeValue::S("event".to_string()),
        ),
    ]));
    let range = mapped_query_bounds(&metadata, &request)
        .expect("bounds")
        .expect("begins_with range");
    let item_key = |sort_key: &str| {
        table_keys::item_key(
            &metadata.identity,
            &ItemKey::Table(TableKey::new(
                TableName::new("orders"),
                AttributeValue::S("tenant".to_string()),
                Some(AttributeValue::S(sort_key.to_string())),
            )),
        )
        .expect("physical key")
    };
    assert!(contains(&range, &item_key("event")));
    assert!(contains(&range, &item_key("eventual")));
    assert!(!contains(&range, &item_key("even")));
    assert!(!contains(&range, &item_key("evf")));
}

#[test]
fn composite_cross_table_child_maps_both_base_key_elements() {
    let plan = cross_table_plan();
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped shape");
    let parent = metadata("orders", 1, schema("pk", Some("sk")), None);
    let child = metadata("archive", 2, schema("account", Some("event")), None);
    let layout = mapped_physical_layout(&shape, &parent, &child)
        .expect("layout")
        .expect("eligible layout");
    assert!(!layout.same_item);
    let elements = unpack::<Vec<Element<'_>>>(layout.mapper.as_deref().expect("mapper"))
        .expect("tuple mapper");
    assert!(matches!(elements[3], Element::Int(2)));
    assert!(matches!(&elements[4], Element::String(value) if value == "{K[5]}"));
    assert!(matches!(&elements[5], Element::String(value) if value == "{K[6]}"));
    assert!(matches!(&elements[6], Element::String(value) if value == "{K[7]}"));
    assert!(matches!(&elements[7], Element::String(value) if value == "{K[8]}"));
}

#[test]
fn static_child_key_component_is_encoded_in_the_mapped_tuple() {
    let plan = plan(serde_json::json!({
        "Nodes": [
            {"Name": "phone_lookup", "Operation": {"Query": {
                "TableName": "phone_lookup", "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "UPI#+14155550123"}}
            }}},
            {"Name": "users", "Operation": {"Get": {
                "TableName": "users", "Key": {
                    "pk": {"FromInput": "user_pk"}, "sk": {"S": "META"}
                }
            }}, "Inputs": {
                "user_pk": {"From": {"Node": "phone_lookup", "Select": "$.Query.Items[*].sk"},
                    "Cardinality": "MANY", "OnMissing": "SKIP"}
            }, "Iterate": "user_pk"}
        ]
    }));
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped shape");
    let parent = metadata("phone_lookup", 1, schema("pk", Some("sk")), None);
    let child = metadata("users", 2, schema("pk", Some("sk")), None);
    let layout = mapped_physical_layout(&shape, &parent, &child)
        .expect("layout")
        .expect("eligible layout");
    let elements = unpack::<Vec<Element<'_>>>(layout.mapper.as_deref().expect("mapper"))
        .expect("tuple mapper");

    assert!(matches!(&elements[4], Element::String(value) if value == "{K[7]}"));
    assert!(matches!(&elements[5], Element::String(value) if value == "{K[8]}"));
    assert!(matches!(&elements[6], Element::String(value) if value == "S"));
    assert!(matches!(&elements[7], Element::Bytes(value) if value.as_ref() == b"META"));
}

#[test]
fn point_get_source_maps_to_primary_partition_query() {
    let plan = get_query_plan();
    let shape = MappedGetQueryShape::from_plan(&plan).expect("get/query mapped shape");
    let parent = metadata("accounts", 1, schema("pk", None), None);
    let child = metadata("events", 2, schema("account", Some("event")), None);
    let layout = mapped_get_query_physical_layout(&shape, &parent, &child)
        .expect("layout")
        .expect("eligible layout");
    let elements = unpack::<Vec<Element<'_>>>(layout.mapper.as_deref().expect("mapper"))
        .expect("tuple mapper");
    assert!(matches!(&elements[4], Element::String(value) if value == "{K[5]}"));
    assert!(matches!(&elements[5], Element::String(value) if value == "{K[6]}"));
    assert!(matches!(&elements[6], Element::String(value) if value == "{...}"));

    let parent_key = ItemKey::Table(TableKey::new(
        TableName::new("accounts"),
        AttributeValue::S("account-1".to_string()),
        None,
    ));
    let child_values = ["a", "b"]
        .into_iter()
        .map(|event| {
            let key = ItemKey::Table(TableKey::new(
                TableName::new("events"),
                AttributeValue::S("account-1".to_string()),
                Some(AttributeValue::S(event.to_string())),
            ));
            ReadSequenceMappedKeyValue {
                key: table_keys::item_key(&child.identity, &key).expect("child key"),
                value: encoded_item(serde_json::json!({
                    "account": {"S": "account-1"}, "event": {"S": event},
                    "payload": {"S": format!("payload-{event}")}
                })),
            }
        })
        .collect();
    let page = ReadSequenceMappedRangePage {
        entries: vec![ReadSequenceMappedEntry {
            parent_key: table_keys::item_key(&parent.identity, &parent_key).expect("parent key"),
            parent_value: encoded_item(serde_json::json!({
                "pk": {"S": "account-1"}, "name": {"S": "Acme"}
            })),
            begin: Vec::new(),
            end: Vec::new(),
            key_values: child_values,
        }],
        more: false,
    };
    let rows = mapped_get_query_rows(&shape, page, plan.nodes.len(), &parent, &child)
        .expect("mapped rows")
        .expect("physical keys match");
    assert!(matches!(
        &rows[shape.parent_id.index()][0].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("pk") == Some(&AttributeValue::S("account-1".to_string()))
    ));
    let ReadSequenceFlatResult::Query {
        items,
        count,
        scanned_count,
        last_evaluated_key,
    } = &rows[shape.child_id.index()][0].result
    else {
        panic!("child row is a query");
    };
    assert_eq!((*count, *scanned_count, items.len()), (2, 2, 2));
    assert!(last_evaluated_key.is_none());
    assert_eq!(
        rows[shape.child_id.index()][0].input_refs["account"].node,
        "account"
    );
}

#[test]
fn mapped_lowering_rejects_parent_projection_that_hides_dependency_input() {
    let mut get_query = get_query_plan();
    let ReadSequenceNodeOperation::Get(parent) = &mut get_query.nodes[0].operation else {
        panic!("expected Get parent");
    };
    parent.projection_expression = Some("name".to_string());
    assert!(MappedGetQueryShape::from_plan(&get_query).is_err());

    let mut query_get = cross_table_plan();
    let ReadSequenceNodeOperation::Query(parent) = &mut query_get.nodes[0].operation else {
        panic!("expected Query parent");
    };
    parent.projection_expression = Some("other".to_string());
    assert!(MappedSequenceShape::from_plan(&query_get).is_err());
}

#[test]
fn point_get_to_query_accepts_hash_alias_without_expression_spacing() {
    let mut plan = get_query_plan();
    let ReadSequenceNodeOperation::Query(query) = &mut plan.nodes[1].operation else {
        panic!("expected Query child");
    };
    query.key_condition_expression = "#account=:account".to_string();
    query.expression_attribute_names = Some(HashMap::from([(
        "#account".to_string(),
        "account".to_string(),
    )]));
    let shape = MappedGetQueryShape::from_plan(&plan).expect("aliased get/query shape");
    let parent = metadata("accounts", 1, schema("pk", None), None);
    let child = metadata("events", 2, schema("account", Some("event")), None);
    assert!(
        mapped_get_query_physical_layout(&shape, &parent, &child)
            .expect("layout")
            .is_ok()
    );
}

#[test]
fn point_get_to_query_rejects_bounded_reverse_and_gsi_shapes() {
    let options = ReadSequenceMappedOptions {
        foundationdb: true,
        api_version: 740,
        enabled: true,
        consistency: ReadSequenceConsistency::Eventual,
    };

    let mut bounded = get_query_plan();
    let ReadSequenceNodeOperation::Query(query) = &mut bounded.nodes[1].operation else {
        panic!("expected Query child");
    };
    query.limit = Some(1);
    let shape = MappedGetQueryShape::from_plan(&bounded).expect("bounded shape");
    let descriptors = mapped_get_query_descriptors(&shape, true);
    let selection = select_read_sequence_mapped_edges(&bounded, &descriptors, options);
    assert!(selection.selected.is_empty());
    assert_eq!(
        selection.assessments[0].reason,
        Some(ReadSequenceMappedRejectionReason::SecondaryLimit)
    );

    let mut reverse = get_query_plan();
    let ReadSequenceNodeOperation::Query(query) = &mut reverse.nodes[1].operation else {
        panic!("expected Query child");
    };
    query.scan_index_forward = Some(false);
    let shape = MappedGetQueryShape::from_plan(&reverse).expect("reverse shape");
    let descriptors = mapped_get_query_descriptors(&shape, true);
    let selection = select_read_sequence_mapped_edges(&reverse, &descriptors, options);
    assert!(selection.selected.is_empty());
    assert_eq!(
        selection.assessments[0].reason,
        Some(ReadSequenceMappedRejectionReason::Continuation)
    );

    let mut gsi_plan = get_query_plan();
    let ReadSequenceNodeOperation::Query(query) = &mut gsi_plan.nodes[1].operation else {
        panic!("expected Query child");
    };
    query.index_name = Some(IndexName::new("status"));
    let shape = MappedGetQueryShape::from_plan(&gsi_plan).expect("gsi shape");
    let parent = metadata("accounts", 1, schema("pk", None), None);
    let child = metadata(
        "events",
        2,
        schema("account", Some("event")),
        Some(gsi(ProjectionType::All)),
    );
    assert!(matches!(
        mapped_get_query_physical_layout(&shape, &parent, &child).expect("layout"),
        Err(ReadSequenceUnsupportedReason::PhysicalLayout)
    ));
}

#[test]
fn point_get_to_query_falls_back_when_the_dependency_attribute_is_missing() {
    let plan = get_query_plan();
    let shape = MappedGetQueryShape::from_plan(&plan).expect("get/query shape");
    let parent = metadata("accounts", 1, schema("pk", None), None);
    let child = metadata("events", 2, schema("account", Some("event")), None);
    let page = ReadSequenceMappedRangePage {
        entries: vec![ReadSequenceMappedEntry {
            parent_key: b"parent".to_vec(),
            parent_value: encoded_item(serde_json::json!({"name": {"S": "Acme"}})),
            begin: b"begin".to_vec(),
            end: b"end".to_vec(),
            key_values: Vec::new(),
        }],
        more: false,
    };
    assert!(
        mapped_get_query_rows(&shape, page, plan.nodes.len(), &parent, &child)
            .expect("mapped row validation")
            .is_none()
    );
}

#[test]
fn indexed_source_compiles_public_ordinal_to_foundationdb_value_slot() {
    let plan = indexed_source_plan(0, "customer_id");
    let shape = MappedSequenceShape::from_plan(&plan).expect("indexed mapped shape");
    let mut parent = metadata("orders", 1, schema("pk", Some("sk")), None);
    parent.table_info.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    let child = metadata("customers", 2, schema("pk", None), None);

    let layout = mapped_physical_layout(&shape, &parent, &child)
        .expect("layout")
        .expect("indexed source layout");
    let elements = unpack::<Vec<Element<'_>>>(layout.mapper.as_deref().expect("mapper"))
        .expect("tuple mapper");

    assert!(matches!(&elements[5], Element::String(value) if value == "{V[2]}"));
}

#[test]
fn indexed_source_present_nil_and_incompatible_rows_are_verified_before_publication() {
    let plan = indexed_source_plan(0, "customer_id");
    let shape = MappedSequenceShape::from_plan(&plan).expect("indexed mapped shape");
    let mut parent = metadata("orders", 1, schema("pk", Some("sk")), None);
    parent.table_info.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    let child = metadata("customers", 2, schema("pk", None), None);

    let present = mapped_edge_rows(
        &shape,
        indexed_source_page(
            &child,
            &["customer_id"],
            Some("customer-1"),
            Some("customer-1"),
        ),
        plan.nodes.len(),
        false,
        &parent,
        &child,
    )
    .expect("present indexed source")
    .expect("verified present indexed source");
    assert!(matches!(
        &present[shape.child_id.index()][0].result,
        ReadSequenceFlatResult::Get { item: Some(item) }
            if item.get("pk") == Some(&AttributeValue::S("customer-1".to_string()))
    ));

    let nil = mapped_edge_rows(
        &shape,
        indexed_source_page(&child, &["customer_id"], None, None),
        plan.nodes.len(),
        false,
        &parent,
        &child,
    )
    .expect("nil indexed source")
    .expect("verified nil indexed source");
    assert!(matches!(
        &nil[shape.child_id.index()][0].result,
        ReadSequenceFlatResult::Get { item: None }
    ));

    for page in [
        indexed_source_page(&child, &[], None, None),
        indexed_source_page(&child, &["account_id"], Some("customer-1"), None),
        indexed_source_page(
            &child,
            &["customer_id"],
            Some("customer-1"),
            Some("customer-2"),
        ),
    ] {
        assert!(
            mapped_edge_rows(&shape, page, plan.nodes.len(), false, &parent, &child)
                .expect("incompatible mapped row")
                .is_none(),
            "incompatible indexed rows must restart through ordinary reads"
        );
    }
}

#[test]
fn given_mapped_rows_exceed_table_capacity_when_decoded_then_corruption_is_returned() {
    let plan = indexed_source_plan(0, "customer_id");
    let shape = MappedSequenceShape::from_plan(&plan).expect("indexed mapped shape");
    let mut parent = metadata("orders", 1, schema("pk", Some("sk")), None);
    parent.table_info.max_indexers = storage_types::MaxIndexers::try_new(1).expect("capacity");
    let child = metadata("customers", 2, schema("pk", None), None);

    let parent_error = mapped_edge_rows(
        &shape,
        indexed_source_page(
            &child,
            &["customer_id", "account_id"],
            Some("customer-1"),
            Some("customer-1"),
        ),
        plan.nodes.len(),
        false,
        &parent,
        &child,
    )
    .expect_err("parent declaration exceeds table capacity");
    assert!(matches!(
        parent_error.to_enum(),
        storage_types::StorageEnum::InternalServerError { message }
            if message == "stored_item_corruption:declaration_exceeds_table_capacity"
    ));

    let mut page = indexed_source_page(
        &child,
        &["customer_id"],
        Some("customer-1"),
        Some("customer-1"),
    );
    page.entries[0].key_values[0].value = encoded_indexed_item(
        &HashMap::from([
            (
                "pk".to_string(),
                AttributeValue::S("customer-1".to_string()),
            ),
            (
                "alias".to_string(),
                AttributeValue::S("primary".to_string()),
            ),
        ]),
        &["alias"],
    );
    let child_error = mapped_edge_rows(&shape, page, plan.nodes.len(), false, &parent, &child)
        .expect_err("child declaration exceeds table capacity");
    assert!(matches!(
        child_error.to_enum(),
        storage_types::StorageEnum::InternalServerError { message }
            if message == "stored_item_corruption:declaration_exceeds_table_capacity"
    ));
}

#[test]
fn same_item_gsi_uses_its_projection_without_a_secondary_mapper() {
    let plan = template_plan(false, false);
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped template shape");
    let metadata = metadata(
        "orders",
        1,
        schema("pk", None),
        Some(gsi(ProjectionType::All)),
    );
    let layout = mapped_physical_layout(&shape, &metadata, &metadata)
        .expect("layout")
        .expect("eligible layout");
    assert!(layout.same_item);
    assert!(layout.mapper.is_none());
}

#[test]
fn keys_only_gsi_rejects_a_child_that_requests_the_full_base_item() {
    let plan = template_plan(false, false);
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped template shape");
    let metadata = metadata(
        "orders",
        1,
        schema("pk", None),
        Some(gsi(ProjectionType::KeysOnly)),
    );
    assert!(mapped_physical_layout(&shape, &metadata, &metadata).is_err());
}

#[test]
fn keys_only_gsi_accepts_a_child_projection_covered_by_the_index() {
    let plan = covered_keys_only_plan();
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped direct-key shape");
    let metadata = metadata(
        "orders",
        1,
        schema("pk", None),
        Some(gsi(ProjectionType::KeysOnly)),
    );
    let layout = mapped_physical_layout(&shape, &metadata, &metadata)
        .expect("layout")
        .expect("covered child projection");
    assert!(layout.same_item);
    assert!(layout.mapper.is_none());
}

#[test]
fn mapped_rows_apply_filter_and_parent_and_child_projections_before_fanout() {
    let plan = template_plan(true, true);
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped template shape");
    let child = metadata("orders", 1, schema("pk", None), None);
    let rows = mapped_edge_rows(
        &shape,
        template_page(),
        plan.nodes.len(),
        true,
        &child,
        &child,
    )
    .expect("mapped rows")
    .expect("physical keys match templates");
    let ReadSequenceFlatResult::Query {
        items,
        count,
        scanned_count,
        ..
    } = &rows[shape.parent_id.index()][0].result
    else {
        panic!("parent row is a query");
    };
    assert_eq!((*count, *scanned_count, items.len()), (1, 2, 1));
    assert!(items[0].get("payload").is_none());
    let children = &rows[shape.child_id.index()];
    assert_eq!(children.len(), 1);
    assert_eq!(children[0].input_refs["sub_id"].item_ordinal, Some(0));
    let ReadSequenceFlatResult::Get { item: Some(item) } = &children[0].result else {
        panic!("child row is a present Get");
    };
    assert_eq!(item.len(), 1);
    assert!(item.get("pk").is_some());
}

#[test]
fn one_cardinality_materializes_one_child_for_many_parent_rows() {
    let plan = one_child_plan();
    let shape = MappedSequenceShape::from_plan(&plan).expect("ONE mapped shape");
    let child = metadata("orders", 1, schema("pk", None), None);
    let rows = mapped_edge_rows(
        &shape,
        template_page(),
        plan.nodes.len(),
        true,
        &child,
        &child,
    )
    .expect("mapped rows")
    .expect("binding match");
    assert_eq!(rows[shape.child_id.index()].len(), 1);
    assert_eq!(
        rows[shape.child_id.index()][0].input_refs["pk"].item_ordinal,
        Some(0)
    );
}

#[test]
#[ignore = "allocation counters require an isolated test process"]
fn string_template_physical_key_validation_does_not_allocate() {
    let plan = template_plan(false, false);
    let shape = MappedSequenceShape::from_plan(&plan).expect("mapped template shape");
    let raw = HashMap::from([
        (
            "entity_id".to_string(),
            AttributeValue::S("account-1".to_string()),
        ),
        ("sub_id".to_string(), AttributeValue::S("a".to_string())),
    ]);
    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "string_template_physical_key_validation_does_not_allocate",
        file!(),
        line!(),
        Some("fdb_mapped_template_validation"),
    );
    for _ in 0..4_096 {
        assert!(mapped_template_matches(
            &shape,
            &[],
            None,
            &raw,
            0,
            "entity#{entity_id}#sub_model#{sub_id}#v1",
            "entity#account-1#sub_model#a#v1",
        ));
    }
    let report = guard.finish();
    alloc_counter::emit_report(&report);
    assert_eq!(report.allocation_count, 0, "{report:?}");
}

fn cross_table_plan() -> storage_types::ReadSequencePlan {
    plan(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders", "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "tenant"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": "archive", "Key": {
                    "account": {"FromInput": "pk"}, "event": {"FromInput": "sk"}
                }
            }}, "Inputs": {
                "pk": {"From": {"Node": "parents", "Select": "$.Query.Items[0].pk"}, "Cardinality": "ONE", "OnMissing": "ERROR"},
                "sk": {"From": {"Node": "parents", "Select": "$.Query.Items[*].sk"}, "Cardinality": "MANY", "OnMissing": "SKIP"}
            }, "Iterate": "sk"}
        ]
    }))
}

fn get_query_plan() -> storage_types::ReadSequencePlan {
    plan(serde_json::json!({
        "Nodes": [
            {"Name": "account", "Operation": {"Get": {
                "TableName": "accounts", "Key": {"pk": {"S": "account-1"}}
            }}},
            {"Name": "events", "Operation": {"Query": {
                "TableName": "events", "KeyConditionExpression": "account = :account",
                "ExpressionAttributeValues": {":account": {"FromInput": "account"}}
            }}, "Inputs": {
                "account": {"From": {"Node": "account", "Select": "$.Get.Item.pk"},
                    "Cardinality": "ONE", "OnMissing": "ERROR"}
            }}
        ]
    }))
}

fn template_plan(filter: bool, projections: bool) -> storage_types::ReadSequencePlan {
    let mut value = serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders", "IndexName": "status",
                "KeyConditionExpression": "gsi_pk = :status",
                "ExpressionAttributeValues": {":status": {"S": "open"}}
            }}},
            {"Name": "children", "Operation": {"Get": {
                "TableName": "orders",
                "Key": {"pk": {"StringTemplate": "entity#{entity_id}#sub_model#{sub_id}#v1"}}
            }}, "Inputs": {
                "entity_id": {"From": {"Node": "parents", "Select": "$.Query.Items[0].entity_id"}, "Cardinality": "ONE", "OnMissing": "ERROR"},
                "sub_id": {"From": {"Node": "parents", "Select": "$.Query.Items[*].sub_id"}, "Cardinality": "MANY", "OnMissing": "SKIP"}
            }, "Iterate": "sub_id"}
        ]
    });
    if filter {
        value["Nodes"][0]["Operation"]["Query"]["FilterExpression"] =
            serde_json::json!("sub_id <> :skip");
        value["Nodes"][0]["Operation"]["Query"]["ExpressionAttributeValues"][":skip"] =
            serde_json::json!({"S": "a"});
    }
    if projections {
        value["Nodes"][0]["Operation"]["Query"]["ProjectionExpression"] =
            serde_json::json!("pk, entity_id, sub_id");
        value["Nodes"][1]["Operation"]["Get"]["ProjectionExpression"] = serde_json::json!("pk");
    }
    plan(value)
}

fn one_child_plan() -> storage_types::ReadSequencePlan {
    plan(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders", "IndexName": "status",
                "KeyConditionExpression": "gsi_pk = :status",
                "ExpressionAttributeValues": {":status": {"S": "open"}}
            }}},
            {"Name": "child", "Operation": {"Get": {
                "TableName": "orders", "Key": {"pk": {"FromInput": "pk"}}
            }}, "Inputs": {
                "pk": {"From": {"Node": "parents", "Select": "$.Query.Items[0].pk"}, "Cardinality": "ONE", "OnMissing": "ERROR"}
            }}
        ]
    }))
}

fn covered_keys_only_plan() -> storage_types::ReadSequencePlan {
    plan(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders", "IndexName": "status",
                "KeyConditionExpression": "gsi_pk = :status",
                "ExpressionAttributeValues": {":status": {"S": "open"}}
            }}},
            {"Name": "child", "Operation": {"Get": {
                "TableName": "orders", "Key": {"pk": {"FromInput": "pk"}},
                "ProjectionExpression": "pk"
            }}, "Inputs": {
                "pk": {"From": {"Node": "parents", "Select": "$.Query.Items[0].pk"}, "Cardinality": "ONE", "OnMissing": "ERROR"}
            }}
        ]
    }))
}

fn indexed_source_plan(indexer: u8, attribute_name: &str) -> storage_types::ReadSequencePlan {
    plan(serde_json::json!({
        "Nodes": [
            {"Name": "parents", "Operation": {"Query": {
                "TableName": "orders", "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": {":pk": {"S": "tenant"}}
            }}},
            {"Name": "child", "Operation": {"Get": {
                "TableName": "customers", "Key": {"pk": {"FromInput": "customer"}}
            }}, "Inputs": {
                "customer": {
                    "From": {"Node": "parents", "Select": "$.Query.Items[*].customer_id"},
                    "MappedKeySource": {"AttributeName": attribute_name, "Indexer": indexer},
                    "Cardinality": "MANY", "OnMissing": "SKIP"
                }
            }, "Iterate": "customer"}
        ]
    }))
}

fn plan(value: serde_json::Value) -> storage_types::ReadSequencePlan {
    let request: ReadSequenceRequest = serde_json::from_value(value).expect("request");
    plan_read_sequence(&request).expect("plan")
}

fn template_page() -> ReadSequenceMappedRangePage {
    ReadSequenceMappedRangePage {
        entries: ["a", "b"]
            .into_iter()
            .map(|sub_id| ReadSequenceMappedEntry {
                parent_key: format!("parent-{sub_id}").into_bytes(),
                parent_value: encoded_item(serde_json::json!({
                    "pk": {"S": format!("entity#account-1#sub_model#{sub_id}#v1")},
                    "gsi_pk": {"S": "open"}, "created": {"S": sub_id},
                    "entity_id": {"S": "account-1"}, "sub_id": {"S": sub_id},
                    "payload": {"S": "not projected"}
                })),
                begin: b"unused".to_vec(),
                end: Vec::new(),
                key_values: Vec::new(),
            })
            .collect(),
        more: false,
    }
}

fn gsi_item_key(metadata: &StoredTableMetadata, created: &str, pk: &str, sk: &str) -> Vec<u8> {
    table_keys::item_key(
        &metadata.identity,
        &ItemKey::Index(IndexKey {
            table_name: TableName::new("orders"),
            index_id: IndexName::new("status"),
            hash_key: AttributeValue::S("open".to_string()),
            range_key: Some(AttributeValue::S(created.to_string())),
            table_key: TableKey::new(
                TableName::new("orders"),
                AttributeValue::S(pk.to_string()),
                Some(AttributeValue::S(sk.to_string())),
            ),
        }),
    )
    .expect("GSI item key")
}

fn contains(range: &super::read_sequence_mapped_bounds::MappedQueryRange, key: &[u8]) -> bool {
    key >= range.begin.as_slice() && key < range.end.as_slice()
}

fn encoded_item(value: serde_json::Value) -> Vec<u8> {
    let item =
        storage_types::WireItem::dynamo_json(serde_json::to_vec(&value).expect("wire item JSON"));
    crate::storage_ops::encode_wire_item_storage_bytes(
        crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
        &item,
        None,
        storage_types::MaxIndexers::ZERO,
    )
    .expect("storage item")
}

fn encoded_indexed_item(item: &HashMap<String, AttributeValue>, declaration: &[&str]) -> Vec<u8> {
    let declaration = declaration
        .iter()
        .map(|name| (*name).to_string())
        .collect::<Vec<_>>();
    let capacity = storage_types::MaxIndexers::try_new(
        u8::try_from(declaration.len()).expect("test declaration length"),
    )
    .expect("test declaration capacity");
    crate::storage_ops::encode_wire_item_storage_bytes(
        crate::sorted_kv_store::ItemValueCodec::FoundationDbTuple,
        &storage_types::WireItem::from_attribute_map(item).expect("indexed item"),
        Some(&declaration),
        capacity,
    )
    .expect("indexed storage item")
}

fn indexed_source_page(
    child: &StoredTableMetadata,
    declaration: &[&str],
    value: Option<&str>,
    child_key: Option<&str>,
) -> ReadSequenceMappedRangePage {
    let mut parent = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant".to_string())),
        ("sk".to_string(), AttributeValue::S("parent-1".to_string())),
    ]);
    if let Some(value) = value {
        parent.insert(
            declaration
                .first()
                .copied()
                .unwrap_or("customer_id")
                .to_string(),
            AttributeValue::S(value.to_string()),
        );
    }
    let parent_value = encoded_indexed_item(&parent, declaration);
    let key_values = child_key.map_or_else(Vec::new, |key| {
        let logical = HashMap::from([("pk".to_string(), AttributeValue::S(key.to_string()))]);
        let item_key = ItemKey::Table(TableKey::new(
            TableName::new("customers"),
            AttributeValue::S(key.to_string()),
            None,
        ));
        vec![ReadSequenceMappedKeyValue {
            key: table_keys::item_key(&child.identity, &item_key).expect("child key"),
            value: encoded_item(serde_json::to_value(logical).expect("child JSON")),
        }]
    });
    ReadSequenceMappedRangePage {
        entries: vec![ReadSequenceMappedEntry {
            parent_key: b"parent-key".to_vec(),
            parent_value,
            begin: b"child-begin".to_vec(),
            end: b"child-end".to_vec(),
            key_values,
        }],
        more: false,
    }
}
