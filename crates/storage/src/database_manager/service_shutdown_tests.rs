//! DUR-F4: a native store that stops after a failed sync makes the process
//! unhealthy and starts its shutdown.

use storage_common::durability::{BarrierFailure, DurabilityBatch};
use storage_types::{NativeUnavailable, StorageEnum};

use super::{
    DatabaseManager, service_shutdown::BackgroundServiceState,
    test_database::immediate_sqlite_test_config,
};

#[tokio::test]
async fn given_native_sync_failure_when_health_is_checked_then_it_reports_unavailable_and_signals_shutdown()
 {
    let path = std::env::temp_dir().join(format!("sqlite-health-{}.db", uuid::Uuid::now_v7()));
    let mut config = immediate_sqlite_test_config();
    config.connection_string = Some(path.to_string_lossy().into_owned());
    let mut manager = DatabaseManager::new_with_config(config).await.unwrap();
    assert!(
        manager.default_connection.durability.is_some(),
        "the SQLite file store"
    );
    manager.check_background_services().unwrap();

    let failed = DurabilityBatch::new(|_start| async { Err(BarrierFailure::SyncFailed) });
    manager.default_connection.durability = Some(failed.health());
    manager.native_durability_failed().await;
    let error = manager.check_background_services().unwrap_err();
    assert!(matches!(
        error.as_ref(),
        StorageEnum::Attempt(storage_types::AttemptFailure::Unavailable(
            NativeUnavailable::DurabilityFailed
        ))
    ));
    let _ = manager.shutdown_background_services().await;
    drop(manager);
    for suffix in ["", "-wal", "-shm"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(file);
    }
}

#[tokio::test]
async fn given_started_delivery_when_services_shut_down_then_runner_drains_and_a_later_start_fails()
{
    let database = DatabaseManager::new_for_test().await.unwrap();
    let services = crate::messaging::MessagingServices::build(
        &database,
        crate::messaging::MessagingConfig {
            pubsub: Some(crate::messaging::PubsubServiceConfig {
                delivery: pubsub::PubsubDeliveryConfig::default(),
                runner: pubsub::DeliveryRunnerConfig::default(),
            }),
            ..Default::default()
        },
    )
    .unwrap();

    services.start_delivery(&database).await.unwrap();
    {
        let state = database.background_services.lock().await;
        let BackgroundServiceState::Running(running) = &*state else {
            panic!("background services are not running after start_delivery");
        };
        assert_eq!(running.delivery.len(), 1);
    }
    database.check_background_services().unwrap();

    database.shutdown_background_services().await.unwrap();
    {
        let state = database.background_services.lock().await;
        assert!(matches!(&*state, BackgroundServiceState::Stopped));
    }
    assert_eq!(
        database.default_admission_controller().snapshot().in_flight,
        0
    );

    let error = services.start_delivery(&database).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("cannot start services during shutdown"),
        "{error}"
    );
}
