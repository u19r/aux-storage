use storage_types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, HIDDEN_TTL_INDEX_PREFIX, IndexName,
    ItemRevision, ItemStreamVersion, ItemWriteVersions, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, MaxIndexers, Projection, ProjectionType, StoredTableInfo,
    StreamItemId, StreamName, StreamRetentionDuration, StreamSpecification, TableName, TableStatus,
    TimestampMillis, WireItem, maintenance::TargetIncarnation,
};
use stream_provider::StoredStreamPointer;

use crate::gsi::{
    GsiActivationObservation, GsiActive, GsiBuild, GsiBuildCandidates, GsiBuildDiscovery,
    GsiBuildProjectionObservation, GsiBuildTarget, GsiChange, GsiFailureReason, GsiLifecycleError,
    GsiObservation, GsiPageLimit, GsiProjection, GsiProjectionChange, GsiProjectionKey,
    GsiProjectionLifecycle, GsiProjectionStateRead, GsiReadRequest, GsiRecord, GsiRecordError,
    GsiReferencedHistory, GsiReferencedHistoryRead, GsiScanObservation, GsiScanPageEnd,
    GsiSourceProjection, GsiState, GsiStateRecord, GsiStreamCandidate, GsiStreamObservation,
    GsiStreamPageEnd, GsiWriteAction,
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
        table_name: TableName::new("orders"),
        index_name: IndexName::new("by-status"),
        incarnation: TargetIncarnation::try_from(b"orders/by-status/v1".to_vec())
            .expect("valid target incarnation"),
    }
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
        table_stream_duration: StreamRetentionDuration::default(),
        default_item_stream_duration: StreamRetentionDuration::default(),
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
fn table_stream_requirement_combines_public_stream_and_deferred_projection() {
    let mut table = projection_table(ProjectionType::All);
    assert!(table.requires_stream_entry(false));
    assert!(!table.requires_stream_entry(true));

    table.stream_specification = Some(StreamSpecification {
        stream_enabled: true,
        stream_view_type: None,
    });
    assert!(table.requires_stream_entry(true));
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

#[test]
fn referenced_history_plan_and_attachment_own_pointer_correlation() {
    let table_name = TableName::new("orders");
    let item_version = ItemStreamVersion::new(7);
    let mut candidates = vec![
        GsiStreamCandidate {
            pointer_id: StreamItemId::from([1; 12]),
            created_at: TimestampMillis::from_timestamp(1),
            pointer: StoredStreamPointer::pointer(
                storage_types::ItemHistoryAddress {
                    history_id: 1u32.into(),
                    stream_name: StreamName::from("orders/stream-item/1"),
                },
                table_name.clone(),
                ItemWriteVersions {
                    item_revision: ItemRevision::initial(),
                    item_stream_version: item_version,
                },
            ),
            referenced_items: None,
        },
        GsiStreamCandidate {
            pointer_id: StreamItemId::from([2; 12]),
            created_at: TimestampMillis::from_timestamp(2),
            pointer: StoredStreamPointer::embedded(
                storage_types::ItemHistoryAddress {
                    history_id: 1u32.into(),
                    stream_name: StreamName::from("orders/stream-item/2"),
                },
                table_name.clone(),
                ItemWriteVersions {
                    item_revision: ItemRevision::initial(),
                    item_stream_version: item_version,
                },
                Vec::new(),
            ),
            referenced_items: None,
        },
        GsiStreamCandidate {
            pointer_id: StreamItemId::from([3; 12]),
            created_at: TimestampMillis::from_timestamp(3),
            pointer: StoredStreamPointer::pointer(
                storage_types::ItemHistoryAddress {
                    history_id: 1u32.into(),
                    stream_name: StreamName::from("other/stream-item/1"),
                },
                TableName::new("other"),
                ItemWriteVersions {
                    item_revision: ItemRevision::initial(),
                    item_stream_version: item_version,
                },
            ),
            referenced_items: None,
        },
    ];

    let table = storage_types::BoundTableInfo {
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    let reads = GsiReferencedHistoryRead::plan(&table, &candidates);
    assert_eq!(reads.len(), 1);
    assert_eq!(reads[0].candidate_ordinal, 0);
    assert_eq!(reads[0].stream_name.as_ref(), b"orders/stream-item/1");
    assert_eq!(reads[0].through, item_version);
    let candidate_ordinal = reads[0].candidate_ordinal;
    drop(reads);

    GsiReferencedHistory::attach_all(
        &mut candidates,
        [GsiReferencedHistory {
            candidate_ordinal,
            items: Vec::new(),
        }],
    )
    .expect("attach referenced history");
    assert!(candidates[0].referenced_items.is_some());
    assert!(candidates[1].referenced_items.is_none());
    assert!(candidates[2].referenced_items.is_none());
    assert!(
        GsiReferencedHistory::attach_all(
            &mut candidates,
            [GsiReferencedHistory {
                candidate_ordinal: 0,
                items: Vec::new(),
            }],
        )
        .is_err()
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
fn active_build_requests_and_advances_continuing_projection() {
    let current = checkpoint(3);
    let next = checkpoint(4);
    let high = checkpoint(5);
    let limit = GsiPageLimit::try_from(100).expect("positive page limit");
    let build = GsiBuild::<String> {
        target: target(),
        state: GsiState::Active(GsiActive {
            cursor: current.clone(),
            pending_high: None,
        }),
    };
    assert_eq!(build.state.required_stream_history(), &current);
    assert!(matches!(
        build.read_request(limit),
        Ok(GsiReadRequest::Continue { after, .. }) if after == current
    ));

    let state = persisted_state(
        build
            .decide_next_change(GsiObservation::Stream(GsiStreamObservation {
                expected_cursor: current.clone(),
                through: high.clone(),

                end: GsiStreamPageEnd::Continue(next.clone()),
                projections: Vec::new(),
            }))
            .expect("continuing projection advances within the observed frontier"),
    );
    assert!(matches!(state, GsiState::Active(state) if state.cursor == next));
}

#[test]
fn active_build_is_idle_only_at_its_observed_stream_frontier() {
    let current = checkpoint(3);
    let build = GsiBuild::<String> {
        target: target(),
        state: GsiState::Active(GsiActive {
            cursor: current.clone(),
            pending_high: None,
        }),
    };
    let change = build
        .decide_next_change(GsiObservation::Stream(GsiStreamObservation {
            expected_cursor: current.clone(),
            through: current.clone(),

            end: GsiStreamPageEnd::ReachedThrough,
            projections: Vec::new(),
        }))
        .expect("an active build at the observed stream tail is idle");
    assert!(matches!(change, GsiChange::Idle));
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
        pending_high: None,
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
        pending_high: None,
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
        GsiBuildDiscovery::SelectionComplete
    ));
    assert!(
        matches!(candidates.schedule(), Some(GsiBuild { state: GsiState::Active(state), .. }) if state.cursor == checkpoint(7))
    );
}

#[test]
fn stream_projection_fences_with_item_revision_for_both_pointer_forms() {
    let table = storage_types::BoundTableInfo {
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    let wire = WireItem::dynamo_json(br#"{"order_id":{"S":"1"},"status":{"S":"open"}}"#.to_vec());
    let data =
        storage_types::storage_serde::to_bytes(&wire.to_attribute_map().expect("attributes"))
            .expect("encode stream image");
    let revision = ItemRevision::try_from(3_u64).expect("revision");
    let history_version = ItemStreamVersion::new(900_000);
    let versions = ItemWriteVersions {
        item_revision: revision,
        item_stream_version: history_version,
    };
    for embedded in [false, true] {
        let stream = StreamName::new(b"orders/stream-item/1");
        let pointer = if embedded {
            StoredStreamPointer::embedded(
                storage_types::ItemHistoryAddress {
                    history_id: 1u32.into(),
                    stream_name: stream,
                },
                table.table_name.clone(),
                versions,
                vec![stream_provider::EmbeddedStreamItem {
                    data: data.clone(),
                    data_type: stream_provider::StreamDataType::DynamoDbJson,
                }],
            )
        } else {
            StoredStreamPointer::pointer(
                storage_types::ItemHistoryAddress {
                    history_id: 1u32.into(),
                    stream_name: stream,
                },
                table.table_name.clone(),
                versions,
            )
        };
        let candidate = GsiStreamCandidate {
            pointer_id: StreamItemId::from([1; 12]),
            created_at: TimestampMillis::from_timestamp(1),
            pointer,
            referenced_items: (!embedded).then(|| {
                vec![stream_provider::StreamItem {
                    id: history_version.into(),
                    stream_name: None,
                    data: data.clone(),
                    data_type: stream_provider::StreamDataType::DynamoDbJson,
                    created_at: TimestampMillis::from_timestamp(1),
                }]
            }),
        };
        let projections = candidate
            .into_projections(&table, &IndexName::new("by-status"))
            .expect("project stream record");
        assert_eq!(projections.len(), 1);
        assert!(
            matches!(&projections[0], GsiSourceProjection::Present { item_version, .. } if *item_version == revision)
        );
    }
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
fn given_other_incarnation_when_gsi_reads_both_pointer_forms_then_no_history_or_projection_is_required()
 {
    let table = storage_types::BoundTableInfo {
        history_id: 2u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    for embedded in [false, true] {
        let history = storage_types::ItemHistoryAddress {
            history_id: 1u32.into(),
            stream_name: StreamName::from("orders/stream-item/1"),
        };
        let versions = ItemWriteVersions {
            item_revision: ItemRevision::initial(),
            item_stream_version: ItemStreamVersion::new(1),
        };
        let pointer = if embedded {
            StoredStreamPointer::embedded(
                history,
                table.table_name.clone(),
                versions,
                vec![stream_provider::EmbeddedStreamItem {
                    data: b"deliberately invalid image".to_vec(),
                    data_type: stream_provider::StreamDataType::DynamoDbJson,
                }],
            )
        } else {
            StoredStreamPointer::pointer(history, table.table_name.clone(), versions)
        };
        let candidate = GsiStreamCandidate {
            pointer_id: StreamItemId::from([1; 12]),
            created_at: TimestampMillis::from_timestamp(1),
            pointer,
            referenced_items: None,
        };
        assert!(
            GsiReferencedHistoryRead::plan(&table, std::slice::from_ref(&candidate)).is_empty()
        );
        assert!(
            candidate
                .into_projections(&table, &IndexName::new("by-status"))
                .expect("old incarnation bypasses decode and image reads")
                .is_empty()
        );
    }
}

#[test]
fn given_referenced_pointer_when_current_image_is_missing_then_nearest_older_is_rejected() {
    let table = storage_types::BoundTableInfo {
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    for versions in [vec![], vec![6], vec![6, 5]] {
        let candidate = referenced_candidate_with_images(&table, &versions);
        assert!(
            candidate
                .into_projections(&table, &IndexName::new("by-status"))
                .is_err()
        );
    }
    for versions in [vec![7], vec![7, 6]] {
        let candidate = referenced_candidate_with_images(&table, &versions);
        let projected = candidate
            .into_projections(&table, &IndexName::new("by-status"))
            .expect("exact current image with optional predecessor");
        assert_eq!(projected.len(), 1);
    }
}

#[test]
fn given_history_attachment_when_ordinal_is_invalid_or_embedded_then_it_is_rejected() {
    let table = storage_types::BoundTableInfo {
        history_id: 1u32.into(),
        info: std::sync::Arc::new(projection_table(ProjectionType::All)),
    };
    let mut candidates = vec![referenced_candidate_with_images(&table, &[])];
    candidates[0].referenced_items = None;
    assert!(
        GsiReferencedHistory::attach_all(
            &mut candidates,
            [GsiReferencedHistory {
                candidate_ordinal: 1,
                items: Vec::new(),
            }]
        )
        .is_err()
    );
    candidates[0].pointer = StoredStreamPointer::embedded(
        storage_types::ItemHistoryAddress {
            history_id: table.history_id,
            stream_name: StreamName::from("orders/stream-item/1"),
        },
        table.table_name.clone(),
        ItemWriteVersions {
            item_revision: ItemRevision::initial(),
            item_stream_version: ItemStreamVersion::new(7),
        },
        Vec::new(),
    );
    assert!(
        GsiReferencedHistory::attach_all(
            &mut candidates,
            [GsiReferencedHistory {
                candidate_ordinal: 0,
                items: Vec::new(),
            }]
        )
        .is_err()
    );
}

fn referenced_candidate_with_images(
    table: &storage_types::BoundTableInfo,
    versions: &[u64],
) -> GsiStreamCandidate {
    let wire = WireItem::dynamo_json(br#"{"order_id":{"S":"1"},"status":{"S":"open"}}"#.to_vec());
    let data = storage_types::storage_serde::to_bytes(&wire.to_attribute_map().unwrap()).unwrap();
    GsiStreamCandidate {
        pointer_id: StreamItemId::from([1; 12]),
        created_at: TimestampMillis::from_timestamp(1),
        pointer: StoredStreamPointer::pointer(
            storage_types::ItemHistoryAddress {
                history_id: table.history_id,
                stream_name: StreamName::from("orders/stream-item/1"),
            },
            table.table_name.clone(),
            ItemWriteVersions {
                item_revision: ItemRevision::initial(),
                item_stream_version: ItemStreamVersion::new(7),
            },
        ),
        referenced_items: Some(
            versions
                .iter()
                .map(|version| stream_provider::StreamItem {
                    id: ItemStreamVersion::new(*version).into(),
                    stream_name: None,
                    data: data.clone(),
                    data_type: stream_provider::StreamDataType::DynamoDbJson,
                    created_at: TimestampMillis::from_timestamp(1),
                })
                .collect(),
        ),
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
