use std::collections::HashMap;

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateReplicaAction, CreateTableRequest,
    ItemKey, ItemStreamVersion, KeyAttributeType, KeySchemaElement, KeyType,
    MultiRegionConsistency, ReplicaDescription, ReplicaStatus, ReplicaUpdate,
    ReplicationEventMetadata, ReplicationHybridLogicalClock, ReplicationMutation,
    ReplicationWriteSource, StreamItemId, StreamName, StreamSpecification, StreamViewType,
    TableName, TimestampMillis, UpdateTableRequest,
};
use stream::{StreamDataType, StreamItem};

use super::model::decode_stream_record_from_item_images;
use crate::{
    DatabaseManager, DeleteItemInput, PeerCheckpointRecord, PeerReplicationStatusRecord,
    PutItemInput, TableBootstrapCursorRecord, TableReplicationConfigRecord, Tables,
};

#[tokio::test]
async fn new_for_test_bootstraps_multi_region_control_plane_table() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");

    let exists = db
        .table_exists(&Tables::sys_storage_replication())
        .await
        .expect("query r existence");

    assert!(exists, "new_for_test should bootstrap r");
}

#[tokio::test]
async fn table_replication_config_round_trips() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_orders");
    let record = TableReplicationConfigRecord {
        table_name: table_name.clone(),
        multi_region_consistency: MultiRegionConsistency::Eventual,
        replica_epoch: 7,
        replicas: vec![
            ReplicaDescription {
                region_name: "us-east-1".to_string(),
                replica_status: ReplicaStatus::Active,
                replica_status_description: None,
                replica_inaccessible_date_time: None,
            },
            ReplicaDescription {
                region_name: "eu-west-1".to_string(),
                replica_status: ReplicaStatus::Creating,
                replica_status_description: Some("bootstrap".to_string()),
                replica_inaccessible_date_time: None,
            },
        ],
        updated_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    };

    db.put_table_replication_config(&record)
        .await
        .expect("store replication config");

    let loaded = db
        .get_table_replication_config(&table_name)
        .await
        .expect("load replication config");

    assert_eq!(loaded, Some(record));

    db.delete_table_replication_config(&table_name)
        .await
        .expect("delete replication config");
    assert!(
        db.get_table_replication_config(&table_name)
            .await
            .expect("load deleted replication config")
            .is_none()
    );
}

#[tokio::test]
async fn peer_checkpoint_round_trips() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let record = PeerCheckpointRecord {
        peer_region: "us-west-2".to_string(),
        last_system_stream_cursor: Some(StreamItemId::from([3; 12])),
        updated_at: TimestampMillis::from_timestamp(1_700_000_123_456),
    };

    db.put_peer_checkpoint(&record)
        .await
        .expect("store peer checkpoint");

    let loaded = db
        .get_peer_checkpoint(&record.peer_region)
        .await
        .expect("load peer checkpoint");

    assert_eq!(loaded, Some(record.clone()));

    db.delete_peer_checkpoint(&record.peer_region)
        .await
        .expect("delete peer checkpoint");
    assert!(
        db.get_peer_checkpoint(&record.peer_region)
            .await
            .expect("load deleted peer checkpoint")
            .is_none()
    );
}

#[tokio::test]
async fn bootstrap_cursor_round_trips() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let record = TableBootstrapCursorRecord {
        table_name: TableName::new("tenant_customers"),
        peer_region: "ap-southeast-2".to_string(),
        protected_stream_cursor: Some(StreamItemId::from([8; 12])),
        last_system_stream_cursor: Some(StreamItemId::from([9; 12])),
        activation_cursor: Some(StreamItemId::from([10; 12])),
        session_started_at: Some(TimestampMillis::from_timestamp(1_700_000_111_222)),
        logical_backfill_manifest_id: Some("manifest-1".to_string()),
        logical_backfill_domain: Some("item_records".to_string()),
        logical_backfill_cursor: Some("cursor-1".to_string()),
        updated_at: TimestampMillis::from_timestamp(1_700_000_222_333),
    };

    db.put_table_bootstrap_cursor(&record)
        .await
        .expect("store bootstrap cursor");

    let loaded = db
        .get_table_bootstrap_cursor(&record.table_name, &record.peer_region)
        .await
        .expect("load bootstrap cursor");

    assert_eq!(loaded, Some(record.clone()));

    db.delete_table_bootstrap_cursor(&record.table_name, &record.peer_region)
        .await
        .expect("delete bootstrap cursor");
    assert!(
        db.get_table_bootstrap_cursor(&record.table_name, &record.peer_region)
            .await
            .expect("load deleted bootstrap cursor")
            .is_none()
    );
}

#[tokio::test]
async fn peer_replication_status_round_trips() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let record = PeerReplicationStatusRecord {
        peer_region: "eu-central-1".to_string(),
        last_inbound_heartbeat_at: Some(TimestampMillis::from_timestamp(1_700_000_333_444)),
        last_heartbeat_rtt_ms: Some(42),
        clock_offset_estimate_ms: Some(-17),
        clock_offset_uncertainty_ms: Some(9),
        last_received_source_commit_ts: Some(TimestampMillis::from_timestamp(1_700_000_320_000)),
        last_received_commit_ts: Some(TimestampMillis::from_timestamp(1_700_000_300_000)),
        last_inbound_apply_at: Some(TimestampMillis::from_timestamp(1_700_000_333_555)),
        sender_queue_depth: Some(3),
        last_outbound_apply_at: Some(TimestampMillis::from_timestamp(1_700_000_333_666)),
        last_outbound_commit_ts: Some(TimestampMillis::from_timestamp(1_700_000_320_000)),
        last_remote_applied_commit_ts: Some(TimestampMillis::from_timestamp(1_700_000_300_000)),
        last_auth_failure_at: Some(TimestampMillis::from_timestamp(1_700_000_100_000)),
        updated_at: TimestampMillis::from_timestamp(1_700_000_333_777),
    };

    db.put_peer_replication_status(&record)
        .await
        .expect("store peer replication status");

    let loaded = db
        .get_peer_replication_status(&record.peer_region)
        .await
        .expect("load peer replication status");

    assert_eq!(loaded, Some(record.clone()));

    let listed = db
        .list_peer_replication_statuses()
        .await
        .expect("list peer replication statuses");
    assert_eq!(listed, vec![record]);
}

#[test]
fn multi_region_control_table_is_excluded_from_replication() {
    assert!(Tables::should_exclude_from_multi_region_replication(
        &Tables::sys_storage_replication()
    ));
    assert!(!Tables::should_exclude_from_multi_region_replication(
        &TableName::new("tenant_orders")
    ));
}

#[test]
fn decode_stream_record_uses_item_stream_version_as_sequence_number() {
    let item_stream_version = ItemStreamVersion::new(42);
    let image = HashMap::from([("pk".to_string(), AttributeValue::S("item#42".to_string()))]);
    let item_images = vec![StreamItem {
        id: StreamItemId::from(item_stream_version),
        stream_name: None,
        data: storage_types::storage_serde::to_bytes(&image).expect("image bytes"),
        data_type: StreamDataType::DynamoDbJson,
        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    }];
    let key_schema = vec![KeySchemaElement {
        attribute_name: "pk".to_string(),
        key_type: KeyType::Hash,
    }];

    let record = decode_stream_record_from_item_images(item_images, &key_schema)
        .expect("decode stream record")
        .expect("record");

    assert_eq!(record.sequence_number, item_stream_version.to_string());
    assert_eq!(
        record.keys.get("pk"),
        Some(&AttributeValue::S("item#42".to_string()))
    );
}

#[tokio::test]
async fn update_table_replica_updates_persist_desired_state() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_global_orders");
    create_test_table(&db, &table_name).await;

    let response = db
        .update_table(UpdateTableRequest {
            table_name: table_name.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: Some(vec![
                ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: "us-east-1".to_string(),
                    }),
                    update: None,
                    delete: None,
                },
                ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: "eu-west-1".to_string(),
                    }),
                    update: None,
                    delete: None,
                },
            ]),
            sse_specification: None,
            stream_specification: None,
            table_class: None,
        })
        .await
        .expect("update table with replica updates");

    assert_eq!(
        response.table_description.multi_region_consistency,
        Some(MultiRegionConsistency::Eventual)
    );
    let replicas = response
        .table_description
        .replicas
        .expect("replicas returned from update_table");
    assert_eq!(
        replicas
            .iter()
            .map(|replica| {
                (
                    replica.region_name.as_str(),
                    replica.replica_status.clone(),
                    replica.replica_status_description.as_deref(),
                )
            })
            .collect::<Vec<_>>(),
        vec![
            (
                "eu-west-1",
                ReplicaStatus::Creating,
                Some("Replica creation requested"),
            ),
            (
                "us-east-1",
                ReplicaStatus::Creating,
                Some("Replica creation requested"),
            ),
        ]
    );

    let config = db
        .get_table_replication_config(&table_name)
        .await
        .expect("load replication config")
        .expect("replication config should exist");
    assert_eq!(config.replica_epoch, 1);
    assert_eq!(
        config.multi_region_consistency,
        MultiRegionConsistency::Eventual
    );
    assert_eq!(config.replicas, replicas);

    for peer_region in ["eu-west-1", "us-east-1"] {
        let cursor = db
            .get_table_bootstrap_cursor(&table_name, peer_region)
            .await
            .expect("load bootstrap cursor")
            .expect("bootstrap cursor should exist for created replica");
        assert_eq!(cursor.table_name, table_name);
        assert_eq!(cursor.peer_region, peer_region);
        assert_eq!(
            cursor.protected_stream_cursor,
            cursor.last_system_stream_cursor
        );
        assert!(
            cursor.session_started_at.is_some(),
            "bootstrap session start should be durable"
        );
    }
}

#[tokio::test]
async fn update_table_keeps_stream_updates_working_with_replica_updates() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_global_streams");
    create_test_table(&db, &table_name).await;

    let response = db
        .update_table(UpdateTableRequest {
            table_name: table_name.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: Some(vec![ReplicaUpdate {
                create: Some(CreateReplicaAction {
                    region_name: "ap-southeast-2".to_string(),
                }),
                update: None,
                delete: None,
            }]),
            sse_specification: None,
            stream_specification: Some(StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(StreamViewType::NewAndOldImages),
            }),
            table_class: None,
        })
        .await
        .expect("update table");

    let response_stream_specification = response
        .table_description
        .stream_specification
        .expect("stream specification in update response");
    assert!(response_stream_specification.stream_enabled);
    assert!(matches!(
        response_stream_specification.stream_view_type,
        Some(StreamViewType::NewAndOldImages)
    ));
    assert_eq!(
        response.table_description.multi_region_consistency,
        Some(MultiRegionConsistency::Eventual)
    );
    assert_eq!(
        response
            .table_description
            .replicas
            .expect("replicas in update response")
            .into_iter()
            .map(|replica| replica.region_name)
            .collect::<Vec<_>>(),
        vec!["ap-southeast-2".to_string()]
    );

    let table_info = db
        .get_table_info(&table_name)
        .await
        .expect("load table info");
    let stored_stream_specification = table_info
        .stream_specification
        .expect("stored stream specification");
    assert!(stored_stream_specification.stream_enabled);
    assert!(matches!(
        stored_stream_specification.stream_view_type,
        Some(StreamViewType::NewAndOldImages)
    ));
}

#[tokio::test]
async fn invalid_replica_updates_do_not_mutate_other_table_settings() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_global_validation");
    create_test_table(&db, &table_name).await;

    let error = db
        .update_table(UpdateTableRequest {
            table_name: table_name.clone(),
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: Some(vec![
                ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: "us-east-1".to_string(),
                    }),
                    update: None,
                    delete: None,
                },
                ReplicaUpdate {
                    create: Some(CreateReplicaAction {
                        region_name: "us-east-1".to_string(),
                    }),
                    update: None,
                    delete: None,
                },
            ]),
            sse_specification: None,
            stream_specification: Some(StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(StreamViewType::NewAndOldImages),
            }),
            table_class: None,
        })
        .await
        .expect_err("duplicate regions should fail validation");

    assert!(
        error
            .to_string()
            .contains("duplicate replica update for region 'us-east-1'"),
        "unexpected error: {error}"
    );

    assert!(
        db.get_table_replication_config(&table_name)
            .await
            .expect("load replication config")
            .is_none()
    );
    let table_info = db
        .get_table_info(&table_name)
        .await
        .expect("load table info");
    assert!(table_info.stream_specification.is_none());
}

#[tokio::test]
async fn replica_updates_reject_empty_region_names() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_global_empty_region");
    create_test_table(&db, &table_name).await;

    let error = db
        .update_table(UpdateTableRequest {
            table_name,
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: Some(vec![ReplicaUpdate {
                create: Some(CreateReplicaAction {
                    region_name: "   ".to_string(),
                }),
                update: None,
                delete: None,
            }]),
            sse_specification: None,
            stream_specification: None,
            table_class: None,
        })
        .await
        .expect_err("empty region name should fail");

    assert!(
        error
            .to_string()
            .contains("region name must not be empty for multi-region control-plane records"),
        "unexpected error: {error}"
    );
}

async fn create_test_table(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(&CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "sk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
        ],
        vec![
            KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        BillingMode::PayPerRequest,
    ))
    .await
    .expect("create table");
}

async fn create_test_table_with_stream(db: &DatabaseManager, table_name: &TableName) {
    db.create_table(
        &CreateTableRequest::new(
            table_name.clone(),
            vec![
                AttributeDefinition {
                    attribute_name: "pk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
                AttributeDefinition {
                    attribute_name: "sk".to_string(),
                    attribute_type: KeyAttributeType::S,
                },
            ],
            vec![
                KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: KeyType::Hash,
                },
                KeySchemaElement {
                    attribute_name: "sk".to_string(),
                    key_type: KeyType::Range,
                },
            ],
            BillingMode::PayPerRequest,
        )
        .with_stream_specification(Some(StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        })),
    )
    .await
    .expect("create stream-enabled table");
}

#[tokio::test]
async fn apply_replication_mutation_skips_stale_remote_against_legacy_local_winner() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_stale");
    create_test_table_with_stream(&db, &table_name).await;

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("pk1", "sk1", "local"))
            .build(),
    )
    .await
    .expect("write local winner");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "stale-remote")),
        old_image: None,
        metadata: replication_metadata("us-east-1", 1, 1_000),
    })
    .await
    .expect("apply stale remote mutation");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should still exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("local".to_string()))
    );

    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 1);
}

#[tokio::test]
async fn apply_replication_mutation_applies_newer_remote_against_legacy_local_winner() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_newer");
    create_test_table_with_stream(&db, &table_name).await;

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("pk1", "sk1", "local"))
            .build(),
    )
    .await
    .expect("write local winner");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "remote")).map(|mut item| {
            item.insert(
                "extra".to_string(),
                storage_types::AttributeValue::S("from-remote".to_string()),
            );
            item
        }),
        old_image: None,
        metadata: replication_metadata(
            "us-west-2",
            2,
            TimestampMillis::now().timestamp_millis() + 60_000,
        ),
    })
    .await
    .expect("apply newer remote mutation");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("remote".to_string()))
    );
    assert_eq!(
        stored.get("extra"),
        Some(&storage_types::AttributeValue::S("from-remote".to_string()))
    );

    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 2);
}

#[tokio::test]
async fn apply_replication_mutation_skips_duplicate_remote_replay() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_duplicate");
    create_test_table_with_stream(&db, &table_name).await;

    let mutation = ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "remote")),
        old_image: None,
        metadata: replication_metadata("eu-west-1", 42, 2_000_000_000_000),
    };

    db.apply_replication_mutation(mutation.clone())
        .await
        .expect("apply first remote mutation");
    db.apply_replication_mutation(mutation)
        .await
        .expect("apply duplicate remote mutation");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("remote".to_string()))
    );

    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 1);
}

#[tokio::test]
async fn apply_replication_mutation_uses_region_tie_breaker_for_equal_hlc() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_region_tie");
    create_test_table_with_stream(&db, &table_name).await;

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "region-a")),
        old_image: None,
        metadata: replication_metadata("ap-southeast-1", 1, 2_000_000_100_000),
    })
    .await
    .expect("apply first remote mutation");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "region-u")),
        old_image: None,
        metadata: replication_metadata("us-east-1", 1, 2_000_000_100_000),
    })
    .await
    .expect("apply second remote mutation with equal hlc");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("region-u".to_string()))
    );

    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 2);
}

#[tokio::test]
async fn apply_replication_mutation_uses_origin_sequence_tie_breaker_within_same_region() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_sequence_tie");
    create_test_table_with_stream(&db, &table_name).await;

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "first")),
        old_image: None,
        metadata: replication_metadata("us-east-1", 1, 2_000_000_200_000),
    })
    .await
    .expect("apply first remote mutation");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "second")),
        old_image: None,
        metadata: replication_metadata("us-east-1", 2, 2_000_000_200_000),
    })
    .await
    .expect("apply second remote mutation with equal region and hlc");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("second".to_string()))
    );
}

#[tokio::test]
async fn apply_replication_mutation_converges_to_latest_winner_under_reordered_conflicts() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase4_reordered_conflicts");
    create_test_table_with_stream(&db, &table_name).await;

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "winner")),
        old_image: None,
        metadata: replication_metadata("eu-west-1", 3, 2_000_000_300_000),
    })
    .await
    .expect("apply latest winner first");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "stale-middle")),
        old_image: None,
        metadata: replication_metadata("us-west-2", 2, 2_000_000_200_000),
    })
    .await
    .expect("apply stale middle mutation");

    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk1", "sk1"),
        new_image: Some(item("pk1", "sk1", "stale-oldest")),
        old_image: None,
        metadata: replication_metadata("ap-southeast-1", 1, 2_000_000_100_000),
    })
    .await
    .expect("apply stale oldest mutation");

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("winner".to_string()))
    );
    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 1);
}

#[tokio::test]
async fn apply_replication_mutations_with_outcomes_reuses_current_winner_for_same_key_batch() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase7_grouped_apply");
    create_test_table_with_stream(&db, &table_name).await;

    let outcomes = db
        .apply_replication_mutations_with_outcomes(vec![
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk1", "sk1"),
                new_image: Some(item("pk1", "sk1", "first")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 1, 2_000_000_400_000),
            },
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk1", "sk1"),
                new_image: Some(item("pk1", "sk1", "second")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 2, 2_000_000_500_000),
            },
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk1", "sk1"),
                new_image: Some(item("pk1", "sk1", "stale")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 1, 2_000_000_300_000),
            },
        ])
        .await
        .expect("apply grouped same-key batch");

    assert_eq!(
        outcomes,
        vec![
            crate::ReplicationMutationApplyOutcome::Applied,
            crate::ReplicationMutationApplyOutcome::Applied,
            crate::ReplicationMutationApplyOutcome::SkippedStale,
        ]
    );

    let stored = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item")
        .expect("item should exist");
    assert_eq!(
        stored.get("data"),
        Some(&storage_types::AttributeValue::S("second".to_string()))
    );
    assert_eq!(item_stream_len(&db, &table_name, "pk1", "sk1").await, 2);
}

#[tokio::test]
async fn apply_replication_mutations_with_outcomes_preserves_original_order_across_keys() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase7_grouped_apply_distinct_keys");
    create_test_table_with_stream(&db, &table_name).await;

    let outcomes = db
        .apply_replication_mutations_with_outcomes(vec![
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk1", "sk1"),
                new_image: Some(item("pk1", "sk1", "first-a")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 1, 2_000_000_400_000),
            },
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk2", "sk2"),
                new_image: Some(item("pk2", "sk2", "first-b")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 2, 2_000_000_500_000),
            },
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk1", "sk1"),
                new_image: Some(item("pk1", "sk1", "stale-a")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 1, 2_000_000_300_000),
            },
            ReplicationMutation {
                table_name: table_name.clone(),
                key: key("pk2", "sk2"),
                new_image: Some(item("pk2", "sk2", "second-b")),
                old_image: None,
                metadata: replication_metadata("us-east-1", 3, 2_000_000_600_000),
            },
        ])
        .await
        .expect("apply grouped multi-key batch");

    assert_eq!(
        outcomes,
        vec![
            crate::ReplicationMutationApplyOutcome::Applied,
            crate::ReplicationMutationApplyOutcome::Applied,
            crate::ReplicationMutationApplyOutcome::SkippedStale,
            crate::ReplicationMutationApplyOutcome::Applied,
        ]
    );

    let stored_a = db
        .get_item_map(table_name.clone(), key("pk1", "sk1"))
        .await
        .expect("load item a")
        .expect("item a should exist");
    assert_eq!(
        stored_a.get("data"),
        Some(&storage_types::AttributeValue::S("first-a".to_string()))
    );

    let stored_b = db
        .get_item_map(table_name.clone(), key("pk2", "sk2"))
        .await
        .expect("load item b")
        .expect("item b should exist");
    assert_eq!(
        stored_b.get("data"),
        Some(&storage_types::AttributeValue::S("second-b".to_string()))
    );
}

#[tokio::test]
async fn read_outbound_replication_batch_returns_local_origin_mutations_only() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase6_outbound_batch");
    create_test_table_with_stream(&db, &table_name).await;

    db.update_table(UpdateTableRequest {
        table_name: table_name.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        replica_updates: Some(vec![ReplicaUpdate {
            create: Some(CreateReplicaAction {
                region_name: "region-b".to_string(),
            }),
            update: None,
            delete: None,
        }]),
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    })
    .await
    .expect("create replica config");
    db.mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("pk1", "sk1", "local"))
            .build(),
    )
    .await
    .expect("put local item");
    db.apply_replication_mutation(ReplicationMutation {
        table_name: table_name.clone(),
        key: key("pk2", "sk2"),
        new_image: Some(item("pk2", "sk2", "remote")),
        old_image: None,
        metadata: replication_metadata("region-c", 7, 2_000_000_500_000),
    })
    .await
    .expect("apply replicated item");

    let batch = db
        .read_outbound_replication_batch(
            "region-a",
            None,
            std::slice::from_ref(&table_name),
            &[],
            1_000,
            512 * 1024,
        )
        .await
        .expect("read outbound batch");

    assert_eq!(
        batch.records.len(),
        1,
        "only local-origin writes should fan out"
    );
    assert_eq!(batch.records[0].mutation.table_name, table_name);
    assert_eq!(
        batch.records[0]
            .mutation
            .new_image
            .as_ref()
            .and_then(|item| item.get("data"))
            .expect("data attr"),
        &storage_types::AttributeValue::S("local".to_string())
    );
    assert!(batch.checkpoint_cursor.is_some());
}

#[tokio::test]
async fn read_outbound_replication_batch_respects_byte_cap_without_skipping_future_progress() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase6_byte_cap");
    create_test_table_with_stream(&db, &table_name).await;

    db.update_table(UpdateTableRequest {
        table_name: table_name.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        replica_updates: Some(vec![ReplicaUpdate {
            create: Some(CreateReplicaAction {
                region_name: "region-b".to_string(),
            }),
            update: None,
            delete: None,
        }]),
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    })
    .await
    .expect("create replica config");
    db.mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("pk1", "sk1", "first"))
            .build(),
    )
    .await
    .expect("put first item");
    db.put_item(
        PutItemInput::builder()
            .table_name(table_name.clone())
            .item(item("pk2", "sk2", "second"))
            .build(),
    )
    .await
    .expect("put second item");

    let first_batch = db
        .read_outbound_replication_batch(
            "region-a",
            None,
            std::slice::from_ref(&table_name),
            &[],
            1_000,
            1,
        )
        .await
        .expect("read first capped batch");
    assert_eq!(
        first_batch.records.len(),
        1,
        "first oversized mutation must still be included"
    );
    assert!(
        !first_batch.reached_end,
        "reader should stop before scanning past the unsent next mutation"
    );

    let second_batch = db
        .read_outbound_replication_batch(
            "region-a",
            first_batch.checkpoint_cursor,
            std::slice::from_ref(&table_name),
            &[],
            1_000,
            1,
        )
        .await
        .expect("read second capped batch");
    assert_eq!(
        second_batch.records.len(),
        1,
        "second mutation should remain available"
    );
    assert!(
        second_batch.reached_end,
        "second batch should reach the current tail after replaying the remaining mutation"
    );
}

#[tokio::test]
async fn read_outbound_replication_batch_skips_missing_local_delete_stream_noop() {
    let db = DatabaseManager::new_for_test()
        .await
        .expect("create test database manager");
    let table_name = TableName::new("tenant_phase6_missing_delete_tombstone");
    create_test_table_with_stream(&db, &table_name).await;

    db.update_table(UpdateTableRequest {
        table_name: table_name.clone(),
        attribute_definitions: None,
        billing_mode: None,
        provisioned_throughput: None,
        on_demand_throughput: None,
        deletion_protection_enabled: None,
        global_secondary_index_updates: None,
        replica_updates: Some(vec![ReplicaUpdate {
            create: Some(CreateReplicaAction {
                region_name: "region-b".to_string(),
            }),
            update: None,
            delete: None,
        }]),
        sse_specification: None,
        stream_specification: None,
        table_class: None,
    })
    .await
    .expect("create replica config");
    db.mark_replica_active(&table_name, "region-b")
        .await
        .expect("mark replica active");

    db.delete_item(
        DeleteItemInput::builder()
            .table_name(table_name.clone())
            .key(key("pk1", "sk1"))
            .build(),
    )
    .await
    .expect("delete missing local item");

    let batch = db
        .read_outbound_replication_batch(
            "region-a",
            None,
            std::slice::from_ref(&table_name),
            &[],
            1_000,
            512 * 1024,
        )
        .await
        .expect("read outbound batch");

    assert!(
        batch.records.is_empty(),
        "missing-item delete is a stream no-op and must not fan out a tombstone"
    );
}

fn key(pk: &str, sk: &str) -> storage_types::KeyAttributes {
    std::collections::HashMap::from([
        (
            "pk".to_string(),
            storage_types::AttributeValue::S(pk.to_string()),
        ),
        (
            "sk".to_string(),
            storage_types::AttributeValue::S(sk.to_string()),
        ),
    ])
    .into()
}

fn item(
    pk: &str,
    sk: &str,
    data: &str,
) -> std::collections::HashMap<String, storage_types::AttributeValue> {
    let mut item = key(pk, sk).to_attribute_map();
    item.insert(
        "data".to_string(),
        storage_types::AttributeValue::S(data.to_string()),
    );
    item
}

fn replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
    physical_ms: i64,
) -> ReplicationEventMetadata {
    let mut sequence_bytes = [0_u8; 12];
    sequence_bytes[4..].copy_from_slice(&sequence_suffix.to_be_bytes());
    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: StreamItemId::from(sequence_bytes),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: TimestampMillis::from_timestamp(physical_ms),
            logical: 0,
        },
        origin_commit_ts: TimestampMillis::from_timestamp(physical_ms),
        table_replica_epoch: 1,
        write_source: ReplicationWriteSource::Replicated,
    }
}

async fn item_stream_len(
    db: &DatabaseManager,
    table_name: &TableName,
    pk: &str,
    sk: &str,
) -> usize {
    let item_key = ItemKey::table_key(
        table_name.clone(),
        storage_types::AttributeValue::S(pk.to_string()),
        Some(storage_types::AttributeValue::S(sk.to_string())),
    );
    let item_stream = StreamName::table_item_stream(table_name, &item_key).expect("item stream");
    db.stream_provider()
        .read_forward(item_stream, None, 10)
        .await
        .expect("read item stream")
        .items
        .len()
}
