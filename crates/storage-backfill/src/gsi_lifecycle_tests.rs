use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, IndexName, KeyAttributeType,
    KeyAttributes, KeySchemaElement, KeyType, MaxIndexers, Projection, ProjectionType,
    StoredTableInfo, StreamRetentionDuration, StreamSpecification, TableName, TableStatus,
    TimestampMillis, WireItem, maintenance::TargetIncarnation,
};

use crate::gsi::{
    GsiBuild, GsiBuildTarget, GsiLifecycleError, GsiPageLimit, GsiProjectionLifecycle,
    GsiWriteAction,
};

#[test]
fn page_limit_reserves_one_native_row_for_lookahead() {
    assert!(GsiPageLimit::try_from(0).is_err());
    assert!(GsiPageLimit::try_from(u32::MAX as usize).is_err());

    let largest =
        GsiPageLimit::try_from((u32::MAX - 1) as usize).expect("largest page limit with lookahead");
    assert_eq!(largest.including_lookahead(), u32::MAX);
}

fn target() -> GsiBuildTarget {
    GsiBuildTarget {
        history_id: 1u32.into(),
        table_name: TableName::new("orders"),
        index_name: IndexName::new("by-status"),
        incarnation: TargetIncarnation::try_from(b"orders/by-status/v1".to_vec())
            .expect("valid target incarnation"),
    }
}

#[test]
fn given_recreated_table_when_old_gsi_target_is_validated_then_matching_names_do_not_grant_authority()
 {
    let original = storage_types::BoundTableInfo {
        binding: Default::default(),
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    target().validate_table(&original).unwrap();
    let recreated = storage_types::BoundTableInfo {
        binding: Default::default(),
        history_id: 2u32.into(),
        info: original.info,
    };
    assert!(target().validate_table(&recreated).is_err());
}

fn checkpoint(offset: u64) -> storage_types::SystemStreamCheckpoint {
    storage_types::SystemStreamCheckpoint::try_new(
        storage_types::SystemStreamScope::try_new("gsi-test", 1).unwrap(),
        vec![
            storage_types::SystemStreamPosition::try_new("root", "root", format!("{offset:020}"))
                .unwrap(),
        ],
    )
    .unwrap()
}

fn projection_table(projection_type: ProjectionType) -> StoredTableInfo {
    StoredTableInfo {
        table_name: TableName::new("orders"),
        table_status: TableStatus::Active,
        created_at: TimestampMillis::from_timestamp(0),
        attribute_definitions: vec![
            AttributeDefinition {
                attribute_name: "order_id".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "status".to_owned(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        key_schema: vec![KeySchemaElement {
            attribute_name: "order_id".to_owned(),
            key_type: KeyType::Hash,
        }],
        max_indexers: MaxIndexers::ZERO,
        global_secondary_indexes: Some(vec![GlobalSecondaryIndex {
            index_name: IndexName::new("by-status"),
            key_schema: vec![KeySchemaElement {
                attribute_name: "status".to_owned(),
                key_type: KeyType::Hash,
            }],
            projection: Projection {
                non_key_attributes: (projection_type == ProjectionType::Include)
                    .then(|| vec!["note".to_owned()]),
                projection_type: Some(projection_type),
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

fn projection_lifecycle() -> GsiProjectionLifecycle {
    GsiProjectionLifecycle::try_from(vec![GsiBuild::<String>::new(target(), checkpoint(1))])
        .expect("valid projection lifecycle")
}

#[test]
fn projection_lifecycle_try_from_rejects_ambiguous_build_identity() {
    let low = checkpoint(1);
    let duplicate_index = vec![
        GsiBuild::<String>::new(target(), low.clone()),
        GsiBuild::<String>::new(target(), low.clone()),
    ];
    assert!(matches!(
        GsiProjectionLifecycle::try_from(duplicate_index),
        Err(GsiLifecycleError::DuplicateIndex)
    ));

    let duplicate_incarnation = TargetIncarnation::try_from(b"orders/by-status/v1".to_vec())
        .expect("valid target incarnation");
    let duplicate_incarnation = vec![
        GsiBuild::<String>::new(target(), low.clone()),
        GsiBuild::<String>::new(
            GsiBuildTarget {
                history_id: 1u32.into(),
                table_name: TableName::new("orders"),
                index_name: IndexName::new("by-customer"),
                incarnation: duplicate_incarnation,
            },
            low.clone(),
        ),
    ];
    assert!(matches!(
        GsiProjectionLifecycle::try_from(duplicate_incarnation),
        Err(GsiLifecycleError::DuplicateIncarnation)
    ));
}

#[test]
fn include_projection_ignores_unprojected_changes_and_emits_wire_items() {
    let table = projection_table(ProjectionType::Include);
    let old_item = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"},"ignored":{"S":"old"}}"#
            .to_vec(),
    );
    let unprojected_change = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"},"ignored":{"S":"new"}}"#
            .to_vec(),
    );
    assert!(
        projection_lifecycle()
            .plan_write_actions(&table, Some(&old_item), Some(&unprojected_change))
            .expect("plan unprojected change")
            .is_empty()
    );

    let projected_change = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"changed"},"ignored":{"S":"new"}}"#
            .to_vec(),
    );
    let mut actions = projection_lifecycle()
        .plan_write_actions(&table, Some(&old_item), Some(&projected_change))
        .expect("plan projected change");
    let GsiWriteAction::Put { projection, .. } = actions.pop().expect("one put") else {
        panic!("projected change must write");
    };
    assert!(actions.is_empty());
    assert_eq!(
        projection
            .project(&projected_change)
            .unwrap()
            .select_attribute_values(&["order_id", "status", "note", "ignored"])
            .expect("decode projected fields"),
        vec![
            Some(AttributeValue::S("1".to_owned())),
            Some(AttributeValue::S("open".to_owned())),
            Some(AttributeValue::S("changed".to_owned())),
            None,
        ]
    );
}

#[test]
fn given_all_projection_when_wire_attributes_change_then_exact_images_are_refreshed() {
    let table = projection_table(ProjectionType::All);
    let old_item = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"}}"#.to_vec(),
    );
    let repeated = projection_lifecycle()
        .plan_write_actions(&table, Some(&old_item), Some(&old_item.clone()))
        .unwrap();
    assert_eq!(repeated.len(), 1);
    assert!(matches!(&repeated[0], GsiWriteAction::Put { .. }));
    for bytes in [
        br#"{"note":{"S":"same"},"status":{"S":"open"},"order_id":{"S":"1"}}"#.as_slice(),
        br#"{ "order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"}}"#,
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"diff"}}"#,
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"longer"}}"#,
    ] {
        let changed = WireItem::dynamo_json(bytes.to_vec());
        let mut actions = projection_lifecycle()
            .plan_write_actions(&table, Some(&old_item), Some(&changed))
            .unwrap();
        let GsiWriteAction::Put { projection, .. } = actions.pop().unwrap() else {
            panic!("changed representation must refresh the projection");
        };
        assert!(actions.is_empty());
        assert_eq!(
            projection
                .project(&changed)
                .unwrap()
                .dynamo_json_bytes()
                .unwrap()
                .as_ref(),
            bytes
        );
    }
}

#[test]
fn immediate_projection_plan_carries_exact_wire_row_and_revision() {
    let mut table = projection_table(ProjectionType::All);
    table.max_indexers = MaxIndexers::try_from(1_u8).expect("one indexer");
    let item = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"new"}}"#.to_vec(),
    );
    let mut writes = projection_lifecycle()
        .plan_immediate_writes(&table, None, Some(&item), &["cover".to_owned()])
        .expect("plan immediate projection")
        .into_iter();
    let write = writes.next().expect("one projection write");
    assert!(writes.next().is_none());
    let row = write.row.expect("projection row");
    assert!(
        row.source_item
            .select_attribute_values(&["order_id", "status", "note"])
            .expect("stored source item")
            == item
                .select_attribute_values(&["order_id", "status", "note"])
                .expect("input source item")
    );
    assert!(row.indexers.names() == ["cover"]);
}

#[test]
fn given_disabled_stream_when_index_exists_then_index_does_not_enable_history() {
    let mut table = projection_table(ProjectionType::All);
    assert!(!table.requires_stream_entry());

    table.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: None,
    });
    assert!(table.requires_stream_entry());
}

#[test]
fn given_prepared_put_when_old_image_and_lifecycle_change_then_projection_effects_match() {
    let declaration =
        storage_types::IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap();
    for projection in [
        ProjectionType::All,
        ProjectionType::Include,
        ProjectionType::KeysOnly,
    ] {
        let table = projection_table(projection);
        let old = WireItem::dynamo_json(
            br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"}}"#.to_vec(),
        );
        for bytes in [
            br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"same"}}"#.as_slice(),
            br#"{"note":{"S":"same"},"status":{"S":"open"},"order_id":{"S":"1"}}"#,
            br#"{"order_id":{"S":"1"},"status":{"S":"open"},"note":{"S":"changed"}}"#,
            br#"{"order_id":{"S":"1"},"status":{"S":"closed"},"note":{"S":"same"}}"#,
            br#"{"order_id":{"S":"1"},"note":{"S":"same"}}"#,
        ] {
            let new = WireItem::dynamo_json(bytes.to_vec());
            let lifecycle = projection_lifecycle();
            let expected = lifecycle
                .plan_write_actions(&table, Some(&old), Some(&new))
                .unwrap();
            let writes = crate::PreparedGsiPut::try_new(&table, &new, &declaration)
                .unwrap()
                .finish(&lifecycle, &table, Some(&old))
                .unwrap()
                .into_iter()
                .collect::<Vec<_>>();
            assert_eq!(
                writes.len(),
                expected.len(),
                "projection {:?}: {:?}",
                table.global_secondary_indexes.as_ref().unwrap()[0].projection,
                bytes
            );
            for action in expected {
                match action {
                    GsiWriteAction::Put {
                        gsi_key,
                        projection,
                        ..
                    } => {
                        let write = writes.iter().find(|write| write.row.is_some()).unwrap();
                        assert_eq!(
                            write.key.index_key,
                            KeyAttributes::from_iter(
                                gsi_key
                                    .into_iter()
                                    .map(|part| (part.name.to_owned(), part.value))
                            )
                        );
                        assert_eq!(
                            write
                                .row
                                .as_ref()
                                .unwrap()
                                .projected_image()
                                .unwrap()
                                .dynamo_json_bytes()
                                .unwrap(),
                            projection
                                .project(&new)
                                .unwrap()
                                .dynamo_json_bytes()
                                .unwrap()
                        );
                    }
                    GsiWriteAction::Delete { gsi_key, .. } => {
                        let write = writes.iter().find(|write| write.row.is_none()).unwrap();
                        assert_eq!(
                            write.key.index_key,
                            KeyAttributes::from_iter(
                                gsi_key
                                    .into_iter()
                                    .map(|part| (part.name.to_owned(), part.value))
                            )
                        );
                    }
                }
            }
            let writes = crate::PreparedGsiPut::try_new(&table, &new, &declaration)
                .unwrap()
                .finish(&GsiProjectionLifecycle::default(), &table, Some(&old))
                .unwrap();
            assert_eq!(writes.into_iter().count(), 0);
        }
    }
}

#[test]
fn given_wrong_typed_gsi_key_when_preparing_put_then_it_is_rejected_before_any_row() {
    let table = projection_table(ProjectionType::All);
    let declaration =
        storage_types::IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap();
    let wrong_type =
        WireItem::dynamo_json(br#"{"order_id":{"S":"1"},"status":{"N":"1"}}"#.to_vec());
    let Err(error) = crate::PreparedGsiPut::try_new(&table, &wrong_type, &declaration) else {
        panic!("wrong-typed GSI key must fail closed");
    };
    assert!(
        error
            .to_string()
            .contains("Type mismatch for Index Key status"),
        "{error}"
    );

    let empty = WireItem::dynamo_json(br#"{"order_id":{"S":"1"},"status":{"S":""}}"#.to_vec());
    let Err(error) = crate::PreparedGsiPut::try_new(&table, &empty, &declaration) else {
        panic!("empty GSI key must fail closed");
    };
    assert!(error.to_string().contains("empty string value"), "{error}");
}
