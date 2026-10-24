use std::collections::HashMap;

use alloc_counter::AllocationGuard;

use crate::{
    AttributeDefinition, AttributeProjection, AttributeValue, GlobalSecondaryIndex, IndexName,
    KeyAttributeType, KeySchemaElement, KeyType, Projection, ProjectionType, QueryTableRequest,
    StorageResult, StoredTableInfo, StreamRetentionDuration, TableName, TableStatus,
    TimestampMillis, WireItem, WireItemKeyAttributes, project_wire_items, validate_gsi_projection,
    validate_gsi_required_attributes,
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
            AttributeValue::M(HashMap::from([
                ("name".to_string(), AttributeValue::S("Ada".to_string())),
                (
                    "secret".to_string(),
                    AttributeValue::S("hidden".to_string()),
                ),
            ])),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::S("second".to_string()),
            ]),
        ),
    ]);
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        attributes["pk"].clone(),
        Some("sk".to_string()),
        Some(attributes["sk"].clone()),
    );
    let non_keys = attributes
        .iter()
        .filter(|(name, _)| name.as_str() != "pk" && name.as_str() != "sk")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    vec![
        WireItem::try_from(&attributes).unwrap(),
        WireItem::local_split(primary, None, Some(serde_json::to_vec(&non_keys).unwrap())),
    ]
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
                    AttributeValue::M(HashMap::from([
                        ("name".to_string(), AttributeValue::S("Ada".to_string())),
                        (
                            "secret".to_string(),
                            AttributeValue::S("hidden".to_string()),
                        ),
                    ])),
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
                AttributeValue::M(HashMap::from([(
                    "name".to_string(),
                    AttributeValue::S("Ada".to_string()),
                )])),
            )]),
        ),
        (
            "profile.name, profile",
            HashMap::from([(
                "profile".to_string(),
                AttributeValue::M(HashMap::from([
                    ("name".to_string(), AttributeValue::S("Ada".to_string())),
                    (
                        "secret".to_string(),
                        AttributeValue::S("hidden".to_string()),
                    ),
                ])),
            )]),
        ),
        ("absent, tags[7]", HashMap::new()),
    ];
    for item in projection_wire_variants() {
        let decoded = item.to_attribute_map().unwrap();
        for (expression, expected) in &cases {
            let projection = AttributeProjection::from_expression(expression, Some(&aliases));
            let projected =
                project_wire_items(vec![item.clone()], Some(expression), Some(&aliases))
                    .unwrap()
                    .pop()
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

#[test]
fn given_malformed_selected_or_unselected_bytes_when_projecting_then_error_is_preserved() {
    for data in [
        br#"{"pk":{"wrong":"value"},"unused":{"S":"valid"}}"#.to_vec(),
        br#"{"pk":{"S":"valid"},"unused":{"wrong":"value"}}"#.to_vec(),
        br#"{"pk":{"S":"valid"}} trailing"#.to_vec(),
    ] {
        let item = WireItem::DynamoJson { data };
        let old_error = item.to_attribute_map().unwrap_err();
        let wire_error = project_wire_items(vec![item.clone()], Some("pk"), None).unwrap_err();
        assert_eq!(wire_error.to_string(), old_error.to_string());
    }
    let primary = WireItemKeyAttributes::new(
        "pk".to_string(),
        AttributeValue::S("valid".to_string()),
        None,
        None,
    );
    let item = WireItem::local_split(
        primary,
        None,
        Some(br#"{"unused":{"wrong":"value"}}"#.to_vec()),
    );
    let old_error = item.to_attribute_map().unwrap_err();
    let wire_error = project_wire_items(vec![item.clone()], Some("pk"), None).unwrap_err();
    assert_eq!(wire_error.to_string(), old_error.to_string());
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
        table_stream_duration: StreamRetentionDuration::default(),
        default_item_stream_duration: StreamRetentionDuration::default(),
        deletion_protection_enabled: false,
    }
}

#[test]
fn projection_expression_preserves_alias_and_document_path_semantics() {
    let item = WireItem::try_from(&HashMap::from([
        ("pk".to_string(), AttributeValue::S("tenant#1".to_string())),
        (
            "profile".to_string(),
            AttributeValue::M(HashMap::from([
                (
                    "display_name".to_string(),
                    AttributeValue::S("Ada".to_string()),
                ),
                (
                    "private_note".to_string(),
                    AttributeValue::S("hidden".to_string()),
                ),
            ])),
        ),
        (
            "tags".to_string(),
            AttributeValue::L(vec![
                AttributeValue::S("first".to_string()),
                AttributeValue::S("second".to_string()),
            ]),
        ),
    ]))
    .unwrap();

    let projected = project_wire_items(
        vec![item],
        Some("#pk, profile.#name, tags[1]"),
        Some(&crate::VecMap::from_iter([
            ("#pk".to_string(), "pk".to_string()),
            ("#name".to_string(), "display_name".to_string()),
        ])),
    )
    .unwrap()
    .pop()
    .unwrap()
    .into_attribute_map()
    .unwrap();

    assert_eq!(projected.len(), 3);
    assert_eq!(
        projected.get("pk"),
        Some(&AttributeValue::S("tenant#1".to_string()))
    );
    assert_eq!(
        projected.get("profile"),
        Some(&AttributeValue::M(HashMap::from([(
            "display_name".to_string(),
            AttributeValue::S("Ada".to_string()),
        )])))
    );
    assert_eq!(
        projected.get("tags"),
        Some(&AttributeValue::L(vec![AttributeValue::S(
            "second".to_string()
        )]))
    );
}

#[test]
fn query_table_request_rejects_reserved_projection_attribute_name() {
    let request = QueryTableRequest {
        table_name: TableName::new("tenant_data"),
        index_name: Some(IndexName::new("gsi1")),
        key_condition_expression: "#pk = :pk".to_string(),
        expression_attribute_names: Some(crate::VecMap::from_iter([(
            "#pk".to_string(),
            "gsi1pk".to_string(),
        )])),
        expression_attribute_values: Some(crate::AttributeMap::from_iter([(
            ":pk".to_string(),
            AttributeValue::S("tenant#1".to_string()),
        )])),
        projection_expression: Some("COMMENT".to_string()),
        limit: None,
        exclusive_start_key: None,
        scan_index_forward: None,
        consistent_read: false,
    };

    let error = request.validate_for_dynamodb().unwrap_err();
    assert_eq!(
        error.to_string(),
        "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved keyword: \
         COMMENT"
    );
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
fn all_projection_accepts_any_requested_attribute() {
    let mut table = include_projection_table();
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::All);

    validate_gsi_projection(
        &table,
        Some(&IndexName::new("gsi1")),
        Some("private_note"),
        None,
        None,
    )
    .unwrap();
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

fn measure_all_projection_validation_allocations(
    label: &'static str,
    table: &StoredTableInfo,
    index_name: &IndexName,
    attributes: &[String],
    validate: fn(&StoredTableInfo, &IndexName, &[String]) -> StorageResult<()>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "gsi_all_projection_validation_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..128 {
        let _ = std::hint::black_box(validate(table, index_name, attributes));
    }
    guard.finish()
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
#[ignore = "manual paired allocation probe; run in an isolated process"]
fn all_projection_guard_allocation_profile() {
    let mut table = include_projection_table();
    table.global_secondary_indexes.as_mut().unwrap()[0]
        .projection
        .projection_type = Some(ProjectionType::All);
    let index_name = IndexName::new("gsi1");

    for width in [1, 10, 100] {
        let attributes = (0..width)
            .map(|index| format!("attribute-{index}"))
            .collect::<Vec<_>>();
        let legacy = measure_all_projection_validation_allocations(
            "legacy_materialize_requested_attributes",
            &table,
            &index_name,
            &attributes,
            legacy_validate_all_projection,
        );
        let current = measure_all_projection_validation_allocations(
            "guard_before_projection_materialization",
            &table,
            &index_name,
            &attributes,
            current_validate_all_projection,
        );
        let _ = alloc_counter::emit_report(&legacy);
        let _ = alloc_counter::emit_report(&current);
        println!(
            "gsi_all_projection width={width} legacy_allocations={} current_allocations={} \
             legacy_bytes={} current_bytes={}",
            legacy.allocation_count / 128,
            current.allocation_count / 128,
            legacy.allocated_bytes / 128,
            current.allocated_bytes / 128,
        );
        assert!(current.allocation_count < legacy.allocation_count);
        assert!(current.allocated_bytes < legacy.allocated_bytes);
    }
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

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_projection_scenarios_when_repeated_then_report_cpu_and_allocations() {
    use std::hint::black_box;

    const ITERATIONS: usize = 10_000;
    let aliases = crate::VecMap::from_iter([
        ("#p".to_owned(), "profile".to_owned()),
        ("#n".to_owned(), "name".to_owned()),
    ]);
    for (label, expression) in [
        ("parse_flat", "pk, sk, binary, unused"),
        ("parse_nested", "#p.#n, tags[1], profile.secret, absent"),
        ("parse_missing", "absent, missing.deep[4].value"),
    ] {
        measure_projection_cost(label, ITERATIONS, || {
            black_box(AttributeProjection::from_expression(
                black_box(expression),
                Some(&aliases),
            ));
        });
    }
    let variants = projection_wire_variants();
    for (label, expression, count) in [
        ("wire_flat", "pk, sk, binary, unused", 1),
        ("wire_nested", "#p.#n, tags[1], profile.secret, absent", 1),
        ("wire_page", "#p.#n, tags[1], profile.secret, absent", 16),
        ("wire_missing", "absent, missing.deep[4].value", 1),
    ] {
        for (variant, item) in variants.iter().enumerate() {
            let items = vec![item.clone(); count];
            let label = format!("{label}_{variant}");
            measure_projection_cost(&label, ITERATIONS, || {
                let projected = project_wire_items(
                    black_box(items.clone()),
                    Some(black_box(expression)),
                    Some(&aliases),
                )
                .unwrap();
                black_box(serde_json::to_vec(&projected).unwrap());
            });
        }
    }
}

fn measure_projection_cost(label: &str, iterations: usize, mut operation: impl FnMut()) {
    for _ in 0..100 {
        operation();
    }
    let guard = AllocationGuard::start(module_path!(), "projection", file!(), line!(), Some(label));
    let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
    for _ in 0..iterations {
        operation();
    }
    let cpu_ns = cpu_started.try_elapsed().unwrap().as_nanos();
    alloc_counter::emit_report(&guard.finish()).unwrap();
    eprintln!("projection label={label} iterations={iterations} cpu_ns={cpu_ns}");
}

#[test]
fn given_unicode_aliases_and_invalid_paths_when_projecting_then_preserves_selection() {
    let item = crate::AttributeMap::from_iter([
        (
            "étiquette".to_owned(),
            AttributeValue::S("value".to_owned()),
        ),
        ("other".to_owned(), AttributeValue::S("hidden".to_owned())),
    ]);
    let names = crate::VecMap::from_iter([("#é".to_owned(), "étiquette".to_owned())]);
    for expression in ["étiquette", "#é", "#é, other[", "#é, other[wrong]"] {
        let projected =
            AttributeProjection::from_expression(expression, Some(&names)).project(&item);
        assert_eq!(projected.len(), 1);
        assert_eq!(projected.get("étiquette"), item.get("étiquette"));
    }
    for expression in ["", " , ", "[0]", "other[", "other[wrong]", "absent"] {
        assert!(
            AttributeProjection::from_expression(expression, Some(&names))
                .project(&item)
                .is_empty()
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_projection_path_shapes_when_parsed_then_report_cpu_and_allocations() {
    use std::hint::black_box;
    let aliases = crate::VecMap::from_iter([("#alias".to_owned(), "étiquette".to_owned())]);
    for (label, path) in [
        ("path_short", "field".to_owned()),
        ("path_alias", "#alias.child[12]".to_owned()),
        ("path_long", format!("field{}", "_x".repeat(1000))),
        (
            "path_unicode",
            format!("{}.child[12]", "étiquette".repeat(64)),
        ),
        (
            "path_deep",
            (0..16)
                .map(|n| format!("field_{n}"))
                .collect::<Vec<_>>()
                .join("."),
        ),
        (
            "path_indices",
            format!(
                "matrix{}",
                (0..16).map(|n| format!("[{n}]")).collect::<String>()
            ),
        ),
        (
            "path_invalid_index",
            "field[99999999999999999999999999999999999]".to_owned(),
        ),
    ] {
        measure_projection_cost(label, 20_000, || {
            black_box(crate::parse_projection_path(
                black_box(&path),
                Some(&aliases),
            ));
        });
    }
}

#[test]
fn given_unicode_and_invalid_indices_when_parsed_then_preserve_path_rules() {
    use crate::{
        ProjectionSegment::{Index, Key},
        VecMap, parse_projection_path,
    };
    let aliases = VecMap::from_iter([("#name".to_owned(), "étiquette".to_owned())]);
    let parsed = parse_projection_path("#name.child[+12][0]", Some(&aliases)).unwrap();
    assert_eq!(
        &*parsed,
        &[Key("étiquette"), Key("child"), Index(12), Index(0)]
    );
    assert_eq!(
        &*parse_projection_path("..étiquette..", None).unwrap(),
        &[Key("étiquette")]
    );
    assert!(parse_projection_path("", None).unwrap().is_empty());
    for invalid in [
        "field[",
        "field[]",
        "field[-1]",
        "field[1x]",
        "field[99999999999999999999999999999999999]",
    ] {
        assert!(parse_projection_path(invalid, None).is_none(), "{invalid}");
    }
}
