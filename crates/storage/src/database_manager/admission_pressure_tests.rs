#[cfg(feature = "sqlite")]
use std::collections::HashMap;

#[cfg(feature = "sqlite")]
use storage_provider::{StorageBackend, StorageConnectionConfig, StorageConnectionRegistry};
#[cfg(feature = "sqlite")]
use storage_types::{
    AttributeDefinition, CreateTableRequest, KeyAttributeType, KeySchemaElement, KeyType, TableName,
};
#[cfg(feature = "sqlite")]
use storage_types::{CreateReplicaAction, ReplicaUpdate, UpdateTableRequest};
use storage_types::{StorageEnum, StorageError, context::WrappedError};

use super::core::{DatabaseManager, is_admission_pressure};

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

#[tokio::test]
async fn control_lane_releases_its_reservation_after_pressure() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let before = database.default_admission_controller().snapshot();
    let result = database
        .run_control_admitted("default", |_provider| async {
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
async fn control_table_updates_reject_replica_metadata_before_provider_io() {
    let database = DatabaseManager::new_for_test()
        .await
        .expect("test database");
    let error = database
        .update_table_for_control(UpdateTableRequest {
            table_name: TableName::new("control-replica-rejected"),
            max_indexers: None,
            attribute_definitions: None,
            billing_mode: None,
            provisioned_throughput: None,
            on_demand_throughput: None,
            deletion_protection_enabled: None,
            global_secondary_index_updates: None,
            replica_updates: Some(vec![ReplicaUpdate {
                create: Some(CreateReplicaAction {
                    region_name: "eu-west-1".to_string(),
                }),
                update: None,
                delete: None,
            }]),
            sse_specification: None,
            stream_specification: None,
            table_class: None,
            aux_stream_duration_hours: None,
            aux_default_item_stream_duration_hours: None,
        })
        .await
        .expect_err("control path must not mutate replica metadata");
    assert!(
        error
            .to_string()
            .contains("control table updates cannot mutate replica metadata")
    );
}
