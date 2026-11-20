use std::collections::HashMap;

use crate::{
    AttributeDefinition, AttributeMap, AttributeProjection, AttributeValue, GlobalSecondaryIndex,
    IndexName, KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType,
    StorageResult, StoredTableInfo, StreamRetentionDuration, TableName, TableStatus,
    TimestampMillis, WireItem, validate_gsi_filter_attributes, validate_gsi_projection,
    validate_gsi_required_attributes, validate_gsi_select,
};

fn projection_wire_variants() -> Vec<WireItem> {
    let attributes = HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        ("sk".to_string(), AttributeValue::N("01".to_string())),
        (
            "unused".to_string(),
            AttributeValue::S("private".to_string()),
        ),
        ("binary".to_string(), AttributeValue::B("AQID".to_string())),
        (
            "profile".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
                (
                    "secret".to_string(),
                    AttributeValue::S("hidden".to_string()),
                ),
            ]))),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::S("second".to_string()),
            ]),
        ),
    ]);
    vec![WireItem::try_from(&attributes).unwrap()]
}

#[test]
fn given_both_wire_representations_when_projecting_paths_then_map_semantics_hold() {
    let aliases = crate::VecMap::from_iter([
        ("#p".to_string(), "profile".to_string()),
        ("#n".to_string(), "name".to_string()),
    ]);
    let cases = [
        (
            "pk, sk",
            HashMap::from([
                ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
                ("sk".to_string(), AttributeValue::N("01".to_string())),
            ]),
        ),
        (
            "binary",
            HashMap::from([("binary".to_string(), AttributeValue::B("AQID".to_string()))]),
        ),
        (
            "#p.#n, tags[1], profile.secret",
            HashMap::from([
                (
                    "profile".to_string(),
                    AttributeValue::M(AttributeMap::from(HashMap::from([
                        ("name".to_string(), AttributeValue::S("Ada".to_string())),
                        (
                            "secret".to_string(),
                            AttributeValue::S("hidden".to_string()),
                        ),
                    ]))),
                ),
                (
                    "tags".to_string(),
                    AttributeValue::L(vec![AttributeValue::S("second".to_string())]),
                ),
            ]),
        ),
        (
            "profile, profile.name",
            HashMap::from([(
                "profile".to_string(),
                AttributeValue::M(AttributeMap::from(HashMap::from([(
                    "name".to_string(),
                    AttributeValue::S("Ada".to_string()),
                )]))),
            )]),
        ),
        (
            "profile.name, profile",
            HashMap::from([(
                "profile".to_string(),
                AttributeValue::M(AttributeMap::from(HashMap::from([
                    ("name".to_string(), AttributeValue::S("Ada".to_string())),
                    (
                        "secret".to_string(),
                        AttributeValue::S("hidden".to_string()),
                    ),
                ]))),
            )]),
        ),
        ("absent, tags[7]", HashMap::new()),
    ];
    for item in projection_wire_variants() {
        let decoded = item.to_attribute_map().unwrap();
        for (expression, expected) in &cases {
            let projection = AttributeProjection::from_expression(expression, Some(&aliases));
            let projected = projection
                .project_wire(&item)
                .unwrap()
                .into_attribute_map()
                .unwrap();
            assert_eq!(
                projected,
                crate::AttributeMap::from(expected.clone()),
                "expression {expression}"
            );
            let map_projected: HashMap<String, AttributeValue> =
                projection.project(&decoded).into();
            assert_eq!(projected, crate::AttributeMap::from(map_projected));
        }
        let attributes = ["pk".to_string(), "absent".to_string(), "binary".to_string()];
        let projected = item
            .project_wire_attributes(&attributes.iter().map(String::as_str).collect::<Vec<_>>())
            .unwrap()
            .into_attribute_map()
            .unwrap();
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.get("pk"), decoded.get("pk"));
        assert_eq!(projected.get("binary"), decoded.get("binary"));
    }
}

fn include_projection_table() -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("tenant_data"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        max_indexers: crate::MaxIndexers::ZERO,
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi1pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("gsi1"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "gsi1pk".to_string(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                projection_type: Some(ProjectionType::Include),
                non_key_attributes: Some(vec!["profile".to_string()]),
            },
        }]),
        table_size_bytes: 0,
        item_count: 0,
        stream_specification: None,
        stream_enabled_at: None,
        table_stream_duration: StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn gsi_projection_accepts_keys_included_attributes_and_nested_paths() {
    let table = include_projection_table();
    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("#pk, #gsi, #profile.display_name"),
        None,
        Some(&crate::VecMap::from_iter([
            ("#pk".to_string(), "pk".to_string()),
            ("#gsi".to_string(), "gsi1pk".to_string()),
            ("#profile".to_string(), "profile".to_string()),
        ])),
    )
    .unwrap();
}

#[test]
fn gsi_projection_rejects_aliased_unprojected_attributes_in_expression_order() {
    let table = include_projection_table();
    let error = validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("#secret.value, missing, #secret.other"),
        None,
        Some(&crate::VecMap::from_iter([(
            "#secret".to_string(),
            "private_note".to_string(),
        )])),
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "One or more parameter values were invalid: Global secondary index gsi1 does not project \
         [private_note, missing]"
    );
}

#[test]
fn gsi_filter_rejects_unprojected_attributes_with_the_filter_message() {
    let table = include_projection_table();
    let error = validate_gsi_filter_attributes(
        &table,
        Some(&IndexName::new("gsi1")),
        ["private_note", "pk", "missing", "private_note"],
    )
    .unwrap_err();

    assert_eq!(
        error.to_string(),
        "One or more parameter values were invalid: Secondary index gsi1 does not project one or \
         more filter attributes: [private_note, missing]"
    );
}

#[test]
fn gsi_select_all_attributes_requires_all_projection() {
    let mut table = include_projection_table();
    let index_name = IndexName::new("gsi1");
    let error = validate_gsi_select(&table, Some(&index_name), Some("ALL_ATTRIBUTES")).unwrap_err();
    assert_eq!(
        error.to_string(),
        "One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported \
         for global secondary index gsi1 because its projection type is not ALL"
    );
    assert!(validate_gsi_select(&table, Some(&index_name), Some("COUNT")).is_ok());
    assert!(validate_gsi_select(&table, None, Some("ALL_ATTRIBUTES")).is_ok());
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::All);
    assert!(validate_gsi_select(&table, Some(&index_name), Some("ALL_ATTRIBUTES")).is_ok());
}

fn legacy_validate_all_projection(
    table: &StoredTableInfo,
    index_name: &IndexName,
    attributes: &[String],
) -> StorageResult<()> {
    let requested = attributes.to_vec();
    validate_gsi_required_attributes(
        table,
        Some(index_name),
        requested.iter().map(String::as_str),
    )
}

fn current_validate_all_projection(
    table: &StoredTableInfo,
    index_name: &IndexName,
    attributes: &[String],
) -> StorageResult<()> {
    validate_gsi_projection(table, Some(index_name), None, Some(attributes), None)
}

#[test]
fn all_projection_guard_preserves_results_for_empty_and_wide_attribute_lists() {
    let mut table = include_projection_table();
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::All);
    let index_name = IndexName::new("gsi1");

    for width in [0, 1, 10, 100] {
        let attributes = (0..width)
            .map(|index| format!("attribute-{index}"))
            .collect::<Vec<_>>();
        let expected = legacy_validate_all_projection(&table, &index_name, &attributes)
            .map_err(|error| error.to_string());
        let actual = current_validate_all_projection(&table, &index_name, &attributes)
            .map_err(|error| error.to_string());
        assert_eq!(actual, expected, "attribute width {width}");
    }

    let malformed = Some("invalid[".to_string());
    assert!(validate_gsi_projection(&table, None, malformed.as_deref(), None, None,).is_ok());
    assert!(
        validate_gsi_projection(
            &table,
            Some(&IndexName::new("missing")),
            malformed.as_deref(),
            None,
            None,
        )
        .is_ok()
    );
}

#[test]
fn gsi_projection_validates_attributes_to_get_without_building_an_expression() {
    let table = include_projection_table();
    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        None,
        Some(&["pk".to_string(), "profile".to_string()]),
        None,
    )
    .unwrap();

    let error = validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        None,
        Some(&["private_note".to_string()]),
        None,
    )
    .unwrap_err();
    assert!(error.to_string().contains("private_note"));
}

fn wire_projection_item() -> AttributeMap {
    AttributeMap::from(HashMap::from([
        ("a".to_string(), AttributeValue::S("A".to_string())),
        (
            "b".to_string(),
            AttributeValue::M(AttributeMap::from(HashMap::from([
                ("c".to_string(), AttributeValue::N("1".to_string())),
                ("d".to_string(), AttributeValue::S("D".to_string())),
            ]))),
        ),
        (
            "e".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("e0".to_string()),
                AttributeValue::S("e1".to_string()),
            ]),
        ),
        ("f".to_string(), AttributeValue::S("F".to_string())),
        ("f.g".to_string(), AttributeValue::S("G".to_string())),
    ]))
}

#[test]
fn project_wire_equals_full_decode_projection() {
    let map = wire_projection_item();
    let item = WireItem::try_from(&map).expect("wire item");
    let attributes = ["f".to_string(), "missing".to_string()];
    let names = crate::VecMap::from_iter([
        ("#b".to_string(), "b".to_string()),
        ("#lit".to_string(), "f.g".to_string()),
    ]);
    let aliased = AttributeProjection::from_expression("#b.c, #lit", Some(&names));
    for projection in [
        AttributeProjection::from_expression("a, b.c, e[1]", None),
        AttributeProjection::from_attributes(&attributes),
        AttributeProjection::from_expression("#b.c, #lit", Some(&names)),
    ] {
        let wire = projection.project_wire(&item).expect("project wire");
        let expected = projection.project(&AttributeMap::try_from(&item).expect("decode"));
        assert_eq!(
            AttributeMap::try_from(&wire).expect("decode projected"),
            expected
        );
    }
    let aliased = AttributeMap::try_from(&aliased.project_wire(&item).expect("project wire"))
        .expect("decode projected");
    assert_eq!(
        aliased.get("f.g"),
        Some(&AttributeValue::S("G".to_string()))
    );
    assert_eq!(
        aliased.get("b"),
        Some(&AttributeValue::M(AttributeMap::from_distinct([(
            "c".to_string(),
            AttributeValue::N("1".to_string())
        )])))
    );
}

#[test]
fn project_wire_allocates_less_than_full_decode() {
    let map = (0..32)
        .map(|index| {
            (
                if index == 0 {
                    "a".to_string()
                } else {
                    format!("attr{index:02}")
                },
                AttributeValue::S(format!("{index:02}").repeat(128)),
            )
        })
        .collect::<AttributeMap>();
    let item = WireItem::try_from(&map).expect("wire item");
    let projection = AttributeProjection::from_expression("a", None);

    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "project_wire_current",
        file!(),
        line!(),
        Some("project_wire"),
    );
    for _ in 0..256 {
        std::hint::black_box(projection.project_wire(&item).expect("project wire"));
    }
    let current = guard.finish();

    let guard = alloc_counter::AllocationGuard::start(
        module_path!(),
        "project_wire_full_decode_legacy",
        file!(),
        line!(),
        Some("full_decode"),
    );
    for _ in 0..256 {
        let decoded = AttributeMap::try_from(&item).expect("decode");
        std::hint::black_box(WireItem::try_from(&projection.project(&decoded)).expect("encode"));
    }
    let legacy = guard.finish();

    let _ = alloc_counter::emit_report(&current);
    let _ = alloc_counter::emit_report(&legacy);
    assert!(
        current.allocation_count < legacy.allocation_count,
        "legacy={} current={}",
        legacy.allocation_count,
        current.allocation_count
    );
}

#[test]
fn given_list_index_duplicates_and_out_of_range_when_projecting_wire_then_matches_selection() {
    let attributes = AttributeMap::from(HashMap::from([
        (
            "étiquette".to_string(),
            AttributeValue::S("value".to_string()),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::S("second".to_string()),
            ]),
        ),
    ]));
    let item = WireItem::try_from(&attributes).expect("encode item");
    let names = crate::VecMap::from_iter([("#é".to_string(), "étiquette".to_string())]);

    let projected =
        AttributeProjection::from_expression("#é, tags[1], absent, tags[bad]", Some(&names))
            .project_wire_nonempty(&item)
            .expect("project")
            .expect("selected attributes")
            .to_attribute_map()
            .expect("decode projected");
    assert_eq!(projected.len(), 2);
    assert_eq!(
        projected.get("étiquette"),
        Some(&AttributeValue::S("value".to_string()))
    );
    assert_eq!(
        projected.get("tags"),
        Some(&AttributeValue::L(vec![AttributeValue::S(
            "second".to_string()
        )]))
    );

    for expression in ["", " , ", "tags[99]", "tags[", "[0]", "absent"] {
        let projection = AttributeProjection::from_expression(expression, None);
        assert!(
            projection
                .project_wire_nonempty(&item)
                .expect("project")
                .is_none()
        );
        assert!(
            projection
                .project_wire(&item)
                .expect("project wire")
                .to_attribute_map()
                .expect("decode empty")
                .is_empty()
        );
    }

    let duplicate = AttributeProjection::from_expression("étiquette, étiquette", None)
        .project_wire_nonempty(&item)
        .expect("project")
        .expect("selected attribute")
        .to_attribute_map()
        .expect("decode duplicate");
    assert_eq!(duplicate.len(), 1);

    // Repeated and out-of-order list indexes keep list order, as GetItem does.
    let reordered = AttributeProjection::from_expression("tags[1], tags[0]", None)
        .project_wire_nonempty(&item)
        .expect("project")
        .expect("selected attributes")
        .to_attribute_map()
        .expect("decode reordered");
    assert_eq!(
        reordered.get("tags"),
        Some(&AttributeValue::L(vec![
            AttributeValue::S("first".to_string()),
            AttributeValue::S("second".to_string()),
        ]))
    );
}

#[test]
fn given_two_paths_into_one_list_element_when_projecting_wire_then_one_merged_map() {
    let attributes = AttributeMap::from(HashMap::from([(
        "m".to_string(),
        AttributeValue::L(vec![AttributeValue::M(AttributeMap::from(HashMap::from(
            [
                ("a".to_string(), AttributeValue::S("one".to_string())),
                ("b".to_string(), AttributeValue::S("two".to_string())),
                ("c".to_string(), AttributeValue::S("three".to_string())),
            ],
        )))]),
    )]));
    let item = WireItem::try_from(&attributes).expect("encode item");

    let projected = AttributeProjection::from_expression("m[0].a, m[0].b", None)
        .project_wire_nonempty(&item)
        .expect("project")
        .expect("selected attributes")
        .to_attribute_map()
        .expect("decode projected");
    assert_eq!(
        projected.get("m"),
        Some(&AttributeValue::L(vec![AttributeValue::M(
            AttributeMap::from(HashMap::from([
                ("a".to_string(), AttributeValue::S("one".to_string())),
                ("b".to_string(), AttributeValue::S("two".to_string())),
            ]))
        )]))
    );
}
