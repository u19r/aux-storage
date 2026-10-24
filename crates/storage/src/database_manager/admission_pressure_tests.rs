#[cfg(feature = "sqlite")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "sqlite")]
use storage_provider::{StorageBackend, StorageConnectionConfig, StorageConnectionRegistry};
#[cfg(feature = "sqlite")]
use storage_types::{
    AttributeDefinition, CreateGlobalSecondaryIndex, CreateTableRequest, KeyAttributeType,
    KeySchemaElement, KeyType, Projection, ProjectionType, TableName,
};
#[cfg(feature = "sqlite")]
use storage_types::{CreateReplicaAction, ReplicaUpdate, UpdateTableRequest};
use storage_types::{StorageEnum, StorageError, context::WrappedError};

use super::core::DatabaseManager;
use crate::admission::is_admission_pressure;

#[cfg(feature = "sqlite")]
fn fanout_transaction(
    table_name: &TableName,
    delivery: usize,
) -> storage_types::TransactWriteItemsEncodeRequest {
    let transact_items = ["body", "outbox"]
        .into_iter()
        .enumerate()
        .map(|(row, kind)| {
            let item = storage_types::WireItem::try_from(&std::collections::HashMap::from([
                (
                    "pk".to_string(),
                    storage_types::AttributeValue::S(format!("delivery-{delivery}-{kind}")),
                ),
                (
                    "gsi_pk".to_string(),
                    storage_types::AttributeValue::S(format!("delivery-{delivery}")),
                ),
                (
                    "gsi_sk".to_string(),
                    storage_types::AttributeValue::N(row.to_string()),
                ),
                (
                    "payload".to_string(),
                    storage_types::AttributeValue::S(kind.to_string()),
                ),
            ]))
            .expect("fanout item should encode");
            storage_types::TransactEncodeItem {
                put: Some(storage_types::TransactEncodePutRequest {
                    table_name: table_name.clone(),
                    item: storage_types::WireEntity::unindexed(item),
                    condition_expression: None,
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..storage_types::TransactEncodeItem::default()
            }
        })
        .collect();
    storage_types::TransactWriteItemsEncodeRequest {
        transact_items,
        ..storage_types::TransactWriteItemsEncodeRequest::default()
    }
}

#[test]
fn only_explicit_capacity_and_service_pressure_is_retryable_pressure() {
    for error in [
        StorageError::service_unavailable(1),
        StorageEnum::ProvisionedThroughputExceeded {
            message: "throttled".to_string(),
        }
        .into(),
        StorageEnum::Throttled {
            message: "throttled".to_string(),
        }
        .into(),
        StorageEnum::LimitExceeded {
            message: "limited".to_string(),
        }
        .into(),
        StorageEnum::RequestLimitExceeded.into(),
        StorageEnum::AwsService {
            code: Some("ThrottlingException".to_string()),
            message: "throttled".to_string(),
        }
        .into(),
    ] {
        assert!(is_admission_pressure(&error));
    }
}

#[test]
fn generic_provider_failures_are_not_reclassified_as_pressure() {
    assert!(!is_admission_pressure(&StorageError::internal(
        "provider failure"
    )));
    assert!(!is_admission_pressure(
        &StorageEnum::AwsService {
            code: None,
            message: "provider failure".to_string(),
        }
        .into()
    ));
    assert!(!is_admission_pressure(
        &StorageEnum::AwsService {
            code: Some("InternalFailure".to_string()),
            message: "provider failure".to_string(),
        }
        .into()
    ));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn control_lane_releases_its_reservation_after_pressure() {
    let database = DatabaseManager::new_for_test_with_runtime_options(
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .build(),
    )
    .await
    .expect("test database");
    let before = database.default_admission_controller().snapshot();
    let result = database
        .run_dynamo_control("default", |_provider| async {
            Err::<(), StorageError>(StorageError::service_unavailable(1))
        })
        .await;

    assert!(
        result
            .as_ref()
            .is_err_and(|error| matches!(error.to_enum(), StorageEnum::ServiceUnavailable { .. }))
    );
    let after = database.default_admission_controller().snapshot();
    assert_eq!(after.in_flight, before.in_flight);
    assert_eq!(after.control_in_flight, before.control_in_flight);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn synthetic_default_routes_to_a_named_physical_default() {
    let database = DatabaseManager::new_with_connection_registry(StorageConnectionRegistry {
        default_connection_id: "primary".to_string(),
        connections: HashMap::from([(
            "primary".to_string(),
            StorageConnectionConfig {
                backend_type: StorageBackend::SQLite,
                connection_string: Some(":memory:".to_string()),
                file_path: None,
                sqlite: None,
                postgres: None,
                turso: None,
                rocksdb: None,
                foundationdb: None,
                remote: None,
            },
        )]),
    })
    .await
    .expect("named default database");

    let table_name = TableName::new("named-default-admission");
    database
        .create_table(&CreateTableRequest::new(
            table_name.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .expect("synthetic default should use the declared physical default");

    let table_info = database
        .get_table_info_for_control(&table_name)
        .await
        .expect("control lane should use the declared physical default");
    assert_eq!(table_info.table_name, table_name);

    let permit = database
        .acquire_admission("default", crate::AdmissionClass::PointRead)
        .await
        .expect("synthetic default admission");
    drop(permit);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn control_metadata_waits_behind_reserved_slots_without_foreground_admission_failure() {
    let database = DatabaseManager::new_for_test_with_runtime_options(
        super::DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .admission_config(crate::admission::AdmissionConfig {
                max_queue_wait_ms: 1000,
                ..Default::default()
            })
            .build(),
    )
    .await
    .expect("test database");
    let table_name = TableName::new("control-metadata-admission");
    database
        .create_table(&CreateTableRequest::new(
            table_name.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_string(),
                key_type: KeyType::Hash,
            }],
            storage_types::BillingMode::PayPerRequest,
        ))
        .await
        .expect("create metadata test table");

    let controller = database.default_admission_controller();
    let control_config = controller.config();
    let mut control_permits = (0..control_config.control_reserve_concurrency)
        .map(|_| {
            controller
                .try_acquire_control()
                .expect("reserve slot should be available")
        })
        .collect::<Vec<_>>();
    assert!(
        !control_permits.is_empty(),
        "test requires a control reserve"
    );

    let foreground_permits = (0..controller.snapshot().desired_limit)
        .map(|_| {
            controller
                .try_acquire(crate::AdmissionClass::PointRead)
                .expect("foreground capacity should be available")
        })
        .collect::<Vec<_>>();
    let waiter_database = std::sync::Arc::new(database);
    let waiter_database_clone = std::sync::Arc::clone(&waiter_database);
    let waiter_table_name = table_name.clone();
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let mut waiter = tokio::spawn(async move {
        let _ = started.send(());
        waiter_database_clone
            .get_table_info_for_control(&waiter_table_name)
            .await
    });
    started_rx
        .await
        .expect("control metadata task should start");
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut waiter)
            .await
            .is_err(),
        "control metadata should wait behind the reserved slots"
    );
    assert_eq!(
        waiter_database
            .default_admission_controller()
            .snapshot()
            .rejection_count,
        0,
        "waiting control work must not become a public rejection"
    );

    drop(control_permits.pop());
    let table_info = tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("control metadata should wake after a slot is released")
        .expect("control metadata task should remain healthy")
        .expect("control metadata read should succeed");
    assert_eq!(table_info.table_name, table_name);
    drop(foreground_permits);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn control_table_updates_reject_replica_metadata_before_provider_io() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let error = database
        .update_table_for_control(UpdateTableRequest {
            replica_updates: Some(vec![ReplicaUpdate {
                create: Some(CreateReplicaAction {
                    region_name: "eu-west-1".to_string(),
                }),
                update: None,
                delete: None,
            }]),
            ..UpdateTableRequest::new(TableName::new("control-replica-rejected"))
        })
        .await
        .expect_err("control path must not mutate replica metadata");
    assert!(
        matches!(error.to_enum(), storage_types::StorageEnum::Unsupported { message } if message == "multi-region replication is not available")
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_saturated_admission_when_encoded_fanout_retries_then_rows_and_gsi_recover() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("create SQLite manager");
    let database = Arc::new(database);
    let table_name = TableName::new("manager_encoded_fanout_gsi");
    let index_name = storage_types::IndexName::new("delivery-gsi");
    let mut gsi_request = CreateTableRequest::new(
        table_name.clone(),
        vec![
            AttributeDefinition {
                attribute_name: "pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_pk".to_string(),
                attribute_type: KeyAttributeType::S,
            },
            AttributeDefinition {
                attribute_name: "gsi_sk".to_string(),
                attribute_type: KeyAttributeType::N,
            },
        ],
        vec![KeySchemaElement {
            attribute_name: "pk".to_string(),
            key_type: KeyType::Hash,
        }],
        storage_types::BillingMode::PayPerRequest,
    );
    gsi_request.global_secondary_indexes = Some(vec![CreateGlobalSecondaryIndex {
        index_name: index_name.clone(),
        key_schema: vec![
            KeySchemaElement {
                attribute_name: "gsi_pk".to_string(),
                key_type: KeyType::Hash,
            },
            KeySchemaElement {
                attribute_name: "gsi_sk".to_string(),
                key_type: KeyType::Range,
            },
        ],
        projection: Projection {
            projection_type: Some(ProjectionType::All),
            non_key_attributes: None,
        },
        provisioned_throughput: None,
    }]);
    database
        .create_table(&gsi_request)
        .await
        .expect("create GSI table");
    // Resolve metadata before filling the shared foreground lane. The wave
    // must exercise transaction admission, not a cold metadata read.
    database
        .get_table_info(&table_name)
        .await
        .expect("warm table metadata");

    let controller = database.default_admission_controller();
    let held_permits = (0..controller.snapshot().desired_limit)
        .map(|_| {
            controller
                .try_acquire(crate::AdmissionClass::Write)
                .expect("foreground capacity should be available")
        })
        .collect::<Vec<_>>();

    let mut handles = Vec::with_capacity(8);
    for delivery in 0..8 {
        let database = Arc::clone(&database);
        let table_name = table_name.clone();
        handles.push(tokio::spawn(async move {
            database
                .transact_write_items_encode(fanout_transaction(&table_name, delivery))
                .await
        }));
    }

    for handle in handles {
        let result = handle.await.expect("fanout task should not panic");
        let error = result.expect_err("the held foreground lane must reject the wave");
        assert!(matches!(
            error.to_enum(),
            StorageEnum::ServiceUnavailable { .. }
        ));
    }

    let saturated = database.default_admission_controller().snapshot();
    assert_eq!(saturated.in_flight, held_permits.len());
    assert_eq!(saturated.queue_depth, 0);
    drop(held_permits);

    let drained = database.default_admission_controller().snapshot();
    assert_eq!(drained.in_flight, 0);
    assert_eq!(drained.queue_depth, 0);

    for delivery in 0..8 {
        for kind in ["body", "outbox"] {
            let item = database
                .get_item_decode::<_, storage_types::AttributeMap>(
                    table_name.clone(),
                    HashMap::from([(
                        "pk".to_string(),
                        storage_types::AttributeValue::S(format!("delivery-{delivery}-{kind}")),
                    )]),
                )
                .await
                .expect("read rejected transaction");
            assert!(
                item.is_none(),
                "rejected transaction must not partially commit"
            );
        }
    }

    for delivery in 0..8 {
        database
            .transact_write_items_encode(fanout_transaction(&table_name, delivery))
            .await
            .expect("retry should succeed after admission drains");
        for kind in ["body", "outbox"] {
            let item = database
                .get_item_decode::<_, storage_types::AttributeMap>(
                    table_name.clone(),
                    HashMap::from([(
                        "pk".to_string(),
                        storage_types::AttributeValue::S(format!("delivery-{delivery}-{kind}")),
                    )]),
                )
                .await
                .expect("read retried transaction");
            assert!(item.is_some(), "retried transaction must commit atomically");
        }
    }
}
