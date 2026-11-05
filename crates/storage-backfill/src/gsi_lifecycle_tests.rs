use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, HIDDEN_TTL_INDEX_PREFIX, IndexName,
    ItemRevision, ItemStreamVersion, KeyAttributeType, KeyAttributes, KeySchemaElement, KeyType,
    MaxIndexers, Projection, ProjectionType, StoredTableInfo, StreamName, StreamRetentionDuration,
    StreamSpecification, TableName, TableStatus, TimestampMillis, WireItem,
    maintenance::TargetIncarnation,
};

use crate::gsi::{
    GsiActivationObservation, GsiActive, GsiBuild, GsiBuildCandidates, GsiBuildDiscovery,
    GsiBuildProjectionObservation, GsiBuildTarget, GsiChange, GsiFailureReason, GsiLifecycleError,
    GsiObservation, GsiPageLimit, GsiProjection, GsiProjectionChange, GsiProjectionKey,
    GsiProjectionLifecycle, GsiProjectionStateRead, GsiReadRequest, GsiRecord, GsiRecordError,
    GsiScanObservation, GsiScanPageEnd, GsiSourceProjection, GsiState, GsiStateRecord,
    GsiStreamCandidate, GsiStreamObservation, GsiStreamPageEnd, GsiWriteAction,
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
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    target().validate_table(&original).unwrap();
    let recreated = storage_types::BoundTableInfo {
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
            capture_for_build: false,
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
fn projection_observation_attachment_rejects_missing_native_results() {
    assert!(matches!(
        GsiBuildProjectionObservation::attach_all(Vec::new(), vec![None]),
        Err(GsiLifecycleError::ObservationMismatch)
    ));
}

#[test]
fn hidden_ttl_projection_builds_typed_destination_without_gsi_attributes() {
    let mut table = projection_table(ProjectionType::All);
    let index_name = IndexName::new(&format!("{HIDDEN_TTL_INDEX_PREFIX}orders"));
    table.global_secondary_indexes = Some(vec![GlobalSecondaryIndex {
        capture_for_build: false,
        index_name: index_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "ttl_partition".to_owned(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "expires_at".to_owned(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            non_key_attributes: None,
            projection_type: Some(ProjectionType::All),
        },
    }]);
    let item = WireItem::dynamo_json(
        br#"{"order_id":{"S":"1"},"expires_at":{"N":"1700000000"}}"#.to_vec(),
    );

    let projection = GsiSourceProjection::from_item(
        &table,
        &index_name,
        item,
        Vec::new(),
        ItemRevision::try_from(1_u64).expect("valid revision"),
    )
    .expect("TTL projection should build");
    let GsiSourceProjection::Present { row, .. } = projection else {
        panic!("numeric TTL must produce a destination")
    };
    assert!(row.key.index_key.is_empty());
    assert!(row.key.table_item_key.is_empty());
    let (ttl_seconds, _): (i64, String) =
        row.key.ttl_index_key.expect("typed TTL destination").into();
    assert_eq!(ttl_seconds, 1_700_000_000);

    let invalid =
        WireItem::dynamo_json(br#"{"order_id":{"S":"1"},"expires_at":{"N":"invalid"}}"#.to_vec());
    assert!(matches!(
        GsiSourceProjection::from_item(
            &table,
            &index_name,
            invalid,
            Vec::new(),
            ItemRevision::try_from(2_u64).expect("valid revision"),
        )
        .expect("invalid TTL should be skipped"),
        GsiSourceProjection::Missing {
            previous_key: None,
            ..
        }
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
    let revision = ItemRevision::try_from(7_u64).expect("valid revision");
    let mut writes = projection_lifecycle()
        .plan_immediate_writes(&table, None, Some(&item), &["cover".to_owned()], revision)
        .expect("plan immediate projection")
        .into_iter();
    let write = writes.next().expect("one projection write");
    assert!(writes.next().is_none());
    assert!(write.item_version == revision);
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
fn projection_state_read_plan_skips_sources_without_a_destination_key() {
    let destination_key = GsiProjectionKey {
        index_name: IndexName::new("by-status"),
        index_key: KeyAttributes::from([(
            "status".to_owned(),
            AttributeValue::S("open".to_owned()),
        )]),
        table_item_key: KeyAttributes::from([(
            "order_id".to_owned(),
            AttributeValue::S("1".to_owned()),
        )]),
        ttl_index_key: None,
    };
    let sources = [
        GsiSourceProjection::Missing {
            item_version: ItemRevision::try_from(1_u64).expect("valid revision"),
            previous_key: None,
        },
        GsiSourceProjection::Missing {
            item_version: ItemRevision::try_from(2_u64).expect("valid revision"),
            previous_key: Some(Box::new(destination_key)),
        },
    ];
    let reads = GsiProjectionStateRead::plan(&sources);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].source_ordinal, 1);
    assert!(
        reads[0].key
            == match &sources[1] {
                GsiSourceProjection::Missing {
                    previous_key: Some(key),
                    ..
                } => key.as_ref(),
                GsiSourceProjection::Missing {
                    previous_key: None, ..
                }
                | GsiSourceProjection::Present { .. } => unreachable!(),
            }
    );
}

fn persisted_state<Cursor>(change: GsiChange<Cursor>) -> GsiState<Cursor> {
    match change {
        GsiChange::Persist { next_state, .. } => next_state,
        GsiChange::Superseded | GsiChange::Idle => panic!("expected persisted GSI state"),
    }
}

fn record_for(state: GsiState<String>) -> GsiRecord {
    let target = target();
    let state = GsiStateRecord::try_from(&state).unwrap();
    GsiRecord {
        history_id: 1u32.into(),
        table_name: target.table_name.to_string(),
        index_name: target.index_name.to_string(),
        incarnation: target.incarnation.as_ref().to_vec(),
        phase: state.phase.to_owned(),
        state: state.state,
    }
}

#[test]
fn complete_scan_and_catch_up_enforce_phase_order() {
    let low = checkpoint(1);
    let middle = checkpoint(2);
    let high = checkpoint(3);
    let limit = GsiPageLimit::try_from(100).expect("positive page limit");
    let mut build = GsiBuild::<String>::new(target(), low.clone());
    assert!(matches!(
        build.read_request(limit),
        Ok(GsiReadRequest::Scan { after: None, .. })
    ));

    let state = persisted_state(
        build
            .decide_next_change(GsiObservation::Scan(GsiScanObservation {
                expected_after: None,
                end: GsiScanPageEnd::Continue("page-one".to_owned()),
                projections: Vec::new(),
            }))
            .expect("first scan page advances"),
    );
    build = GsiBuild {
        target: target(),
        state,
    };
    let state = persisted_state(
        build
            .decide_next_change(GsiObservation::Scan(GsiScanObservation {
                expected_after: Some("page-one".to_owned()),
                end: GsiScanPageEnd::Complete(high.clone()),
                projections: Vec::new(),
            }))
            .expect("scan completion captures high boundary"),
    );
    build = GsiBuild {
        target: target(),
        state,
    };
    assert!(matches!(
        build.read_request(limit),
        Ok(GsiReadRequest::CatchUp { after, through, .. })
            if after == low && through == high
    ));

    let state = persisted_state(
        build
            .decide_next_change(GsiObservation::Stream(GsiStreamObservation {
                expected_cursor: low.clone(),
                through: high.clone(),

                end: GsiStreamPageEnd::Continue(middle.clone()),
                projections: Vec::new(),
            }))
            .expect("catch-up page advances"),
    );
    build = GsiBuild {
        target: target(),
        state,
    };
    let state = persisted_state(
        build
            .decide_next_change(GsiObservation::Stream(GsiStreamObservation {
                expected_cursor: middle.clone(),
                through: high.clone(),

                end: GsiStreamPageEnd::ReachedThrough,
                projections: Vec::new(),
            }))
            .expect("catch-up reaches the closed high boundary"),
    );
    build = GsiBuild {
        target: target(),
        state,
    };
    assert!(matches!(
        build.read_request(limit),
        Ok(GsiReadRequest::InspectActivation {
            required_from,
            through,
        }) if required_from == high && through == high
    ));

    let activation = build
        .decide_next_change(GsiObservation::Activation(GsiActivationObservation {
            target_incarnation: target().incarnation,

            projected_through: high.clone(),
        }))
        .expect("activation facts satisfy the build");
    let GsiChange::Persist {
        projections,
        next_state: GsiState::Active(state),
    } = activation
    else {
        panic!("activation must persist the active state");
    };
    assert!(projections.into_iter().next().is_none());
    assert_eq!(state.cursor, high);
}

#[test]
fn wrong_observation_or_cursor_is_rejected() {
    let low = checkpoint(1);
    let high = checkpoint(3);
    let build = GsiBuild::<String>::new(target(), low.clone());
    assert!(matches!(
        build.decide_next_change(GsiObservation::Activation(GsiActivationObservation {
            target_incarnation: target().incarnation,

            projected_through: high.clone(),
        })),
        Err(GsiLifecycleError::ObservationMismatch)
    ));
    assert!(matches!(
        build.decide_next_change(GsiObservation::Scan(GsiScanObservation {
            expected_after: Some("stale".to_owned()),
            end: GsiScanPageEnd::Complete(high.clone()),
            projections: Vec::new(),
        })),
        Err(GsiLifecycleError::ProgressConflict)
    ));
}

#[test]
fn projection_versions_select_replace_stale_replay_and_conflict() {
    let moved = GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-y".to_owned()),
    }
    .decide_against(Some(GsiProjection {
        item_version: ItemRevision::try_from(3_u64).expect("valid revision"),
        projection: Some("group-x".to_owned()),
    }));
    assert!(matches!(
        moved,
        GsiProjectionChange::Replace {
            remove: Some(remove),
            write: Some(write),
            item_version,
        } if remove == "group-x" && write == "group-y" && item_version == ItemRevision::try_from(4_u64).expect("valid revision")
    ));

    let stale = GsiProjection {
        item_version: ItemRevision::try_from(3_u64).expect("valid revision"),
        projection: Some("group-x".to_owned()),
    }
    .decide_against(Some(GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-y".to_owned()),
    }));
    assert!(matches!(stale, GsiProjectionChange::IgnoreStale));

    let replay = GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-y".to_owned()),
    }
    .decide_against(Some(GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-y".to_owned()),
    }));
    assert!(matches!(replay, GsiProjectionChange::Replay));

    let conflict = GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-z".to_owned()),
    }
    .decide_against(Some(GsiProjection {
        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
        projection: Some("group-y".to_owned()),
    }));
    assert!(matches!(
        conflict,
        GsiProjectionChange::FailClosed(GsiFailureReason::ProjectionConflict)
    ));
}

#[test]
fn unavailable_required_history_blocks_the_build() {
    let low = checkpoint(1);
    let build = GsiBuild::<String>::new_active(target(), low.clone());
    assert!(
        matches!(persisted_state(build.block_unavailable_history()), GsiState::Blocked(state) if state.reason == GsiFailureReason::HistoryUnavailable && state.retained_from == low)
    );
}

#[test]
fn projection_writes_are_normalized_before_provider_persistence() {
    let low = checkpoint(1);
    let key = || GsiProjectionKey {
        index_name: IndexName::new("by-status"),
        index_key: KeyAttributes::from([(
            "status".to_owned(),
            AttributeValue::S("pending".to_owned()),
        )]),
        table_item_key: KeyAttributes::from([(
            "order_id".to_owned(),
            AttributeValue::S("order-1".to_owned()),
        )]),
        ttl_index_key: None,
    };
    let build = GsiBuild::<String>::new(target(), low.clone());
    let change = build
        .decide_next_change(GsiObservation::Scan(GsiScanObservation {
            expected_after: None,
            end: GsiScanPageEnd::Continue("next".to_owned()),
            projections: vec![
                GsiBuildProjectionObservation {
                    source: GsiSourceProjection::Missing {
                        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
                        previous_key: None,
                    },
                    current: Some(GsiProjection {
                        item_version: ItemRevision::try_from(3_u64).expect("valid revision"),
                        projection: Some(key()),
                    }),
                },
                GsiBuildProjectionObservation {
                    source: GsiSourceProjection::Missing {
                        item_version: ItemRevision::try_from(5_u64).expect("valid revision"),
                        previous_key: None,
                    },
                    current: Some(GsiProjection {
                        item_version: ItemRevision::try_from(4_u64).expect("valid revision"),
                        projection: Some(key()),
                    }),
                },
            ],
        }))
        .expect("the shared lifecycle normalizes projection writes");
    let GsiChange::Persist { projections, .. } = change else {
        panic!("scan page must persist");
    };
    let mut writes = projections.into_iter();
    let write = writes.next().expect("one final physical write");
    assert!(writes.next().is_none());
    assert!(write.key == key());
    assert!(write.row.is_none());
    assert!(write.item_version == ItemRevision::try_from(5_u64).expect("valid revision"));
}

#[test]
fn shared_record_conversion_owns_persisted_phase_validation() {
    let low = checkpoint(1);
    let high = checkpoint(3);
    let scanning =
        GsiBuild::<String>::try_from(record_for(GsiState::Scanning(crate::gsi::GsiScanning {
            low: low.clone(),
            after: Some("page".to_owned()),
        })))
        .expect("shared scanning record is valid");
    assert!(matches!(
        scanning.state,
        GsiState::Scanning(state) if state.low == low && state.after.as_deref() == Some("page")
    ));

    let catching_up = GsiBuild::<String>::try_from(record_for(GsiState::CatchingUp(
        crate::gsi::GsiCatchingUp {
            cursor: low.clone(),
            high: high.clone(),
        },
    )))
    .expect("shared catch-up record is valid");
    assert!(matches!(
        catching_up.state,
        GsiState::CatchingUp(state)
            if state.cursor == low && state.high == high
    ));

    let active = GsiBuild::<String>::try_from(record_for(GsiState::Active(GsiActive {
        cursor: high.clone(),
    })))
    .expect("shared active record is valid");
    assert!(matches!(active.state, GsiState::Active(state) if state.cursor == high));

    let blocked =
        GsiBuild::<String>::try_from(record_for(GsiState::Blocked(crate::gsi::GsiBlocked {
            reason: GsiFailureReason::HistoryUnavailable,
            retained_from: low.clone(),
        })))
        .expect("shared blocked record is valid");
    assert!(matches!(
        blocked.state,
        GsiState::Blocked(state)
            if state.reason == GsiFailureReason::HistoryUnavailable
                && state.retained_from == low
    ));

    let mut invalid_active = record_for(GsiState::Active(GsiActive {
        cursor: high.clone(),
    }));
    invalid_active.phase = "scanning".to_owned();
    assert!(matches!(
        GsiBuild::<String>::try_from(invalid_active),
        Err(GsiRecordError::Phase)
    ));
}

#[test]
fn given_scheduled_builds_when_selecting_then_first_runnable_target_stops_discovery() {
    let mut candidates = GsiBuildCandidates::new();
    let blocked = GsiBuild::<String> {
        target: target(),
        state: GsiState::Blocked(crate::gsi::GsiBlocked {
            reason: GsiFailureReason::HistoryUnavailable,
            retained_from: checkpoint(0),
        }),
    };
    assert!(matches!(
        candidates.select_next_build_candidate(blocked),
        GsiBuildDiscovery::Continue
    ));
    assert!(candidates.scanned_through.as_ref().unwrap() == &target());
    assert!(matches!(
        candidates.select_next_build_candidate(GsiBuild::new_active(target(), checkpoint(7))),
        GsiBuildDiscovery::Continue
    ));
    assert!(matches!(
        candidates.select_next_build_candidate(GsiBuild::new(target(), checkpoint(8))),
        GsiBuildDiscovery::SelectionComplete
    ));
    assert!(
        matches!(candidates.schedule(), Some(GsiBuild { state: GsiState::Scanning(state), .. }) if state.low == checkpoint(8))
    );
}

#[test]
fn given_exact_history_revision_when_projected_then_projection_uses_that_revision() {
    let table = storage_types::BoundTableInfo {
        history_id: 1_u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    let candidate = resolved_candidate(&table, 3);
    let projections = candidate
        .into_projections(&table, &IndexName::new("by-status"))
        .unwrap();
    assert_eq!(projections.len(), 1);
    assert!(
        matches!(&projections[0], GsiSourceProjection::Present { item_version, .. } if item_version.get() == 3)
    );
}

#[test]
fn given_unavailable_history_when_blocked_then_no_projection_write_is_kept() {
    let low = checkpoint(2);
    let build = GsiBuild::<String>::new(target(), low.clone());
    let GsiChange::Persist {
        projections,
        next_state: GsiState::Blocked(blocked),
    } = build.block_unavailable_history()
    else {
        panic!("history loss must block");
    };
    assert_eq!(projections.into_iter().len(), 0);
    assert_eq!(blocked.retained_from, low);
    assert!(matches!(
        blocked.reason,
        GsiFailureReason::HistoryUnavailable
    ));
}

#[test]
fn given_resolved_history_for_another_table_when_projected_then_incarnation_mismatch_is_rejected() {
    let table = storage_types::BoundTableInfo {
        history_id: 1_u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    let candidate = resolved_candidate(&table, 7);
    let replacement = storage_types::BoundTableInfo {
        history_id: 2_u32.into(),
        info: table.info.clone(),
    };
    assert!(
        candidate
            .into_projections(&replacement, &IndexName::new("by-status"))
            .is_err()
    );
}

fn resolved_candidate(table: &storage_types::BoundTableInfo, version: u64) -> GsiStreamCandidate {
    let metadata = storage_types::ItemHistoryMetadata::try_new(
        storage_types::ItemHistoryAddress {
            history_id: table.history_id,
            stream_name: StreamName::from("orders/stream-item/1"),
        },
        ItemStreamVersion::new(version),
        None,
        storage_types::ItemHistoryPublication::Event {
            kind: storage_types::ItemHistoryKind::Image,
            source_scope: storage_types::SystemStreamScope::try_new("source", 1).unwrap(),
            table_position: storage_types::SystemStreamPosition::try_new("root", "root", "1")
                .unwrap(),
            created_at: TimestampMillis::from_timestamp(1),
            user_identity: None,
        },
    )
    .unwrap();
    let entry = storage_types::ItemHistoryEntry::try_new(
        metadata,
        storage_types::ItemHistoryChange::Image {
            item: WireItem::dynamo_json(
                br#"{"order_id":{"S":"1"},"status":{"S":"open"}}"#.to_vec(),
            ),
            indexers: storage_types::IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO)
                .unwrap(),
        },
    )
    .unwrap();
    GsiStreamCandidate {
        history: stream_provider::ResolvedItemHistory {
            table: std::sync::Arc::new(storage_types::TableHistorySchema {
                source_table_name: storage_types::SystemStreamSourceTableName::try_from(
                    &table.table_name,
                )
                .unwrap(),
                key_schema: table.key_schema.clone(),
            }),
            current: std::sync::Arc::new(entry),
            predecessor: None,
        },
    }
}

#[test]
fn given_prepared_put_when_old_image_and_lifecycle_change_then_projection_effects_match() {
    let revision = ItemRevision::try_from(7_u64).unwrap();
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
                .finish(&lifecycle, &table, Some(&old), revision)
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
            for write in &writes {
                assert_eq!(write.item_version, revision);
            }
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
                .finish(
                    &GsiProjectionLifecycle::default(),
                    &table,
                    Some(&old),
                    revision,
                )
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
