use std::{sync::Arc, time::Duration};

use scheduled_bg_jobs::store::StoreKey;
use storage_types::StorageResult;

use super::{
    DatabaseManager, DatabaseManagerRuntimeOptions, construction::single_connection_registry,
    service_shutdown::BackgroundServiceState, test_database::immediate_sqlite_test_config,
};
use crate::{
    AdmissionClass,
    admission::{AdmissionController, AdmissionPermit},
};

/// Two in-memory SQLite connections, "primary" (the default) and "secondary";
/// database jobs off.
async fn two_connections() -> DatabaseManager {
    let mut registry = single_connection_registry(immediate_sqlite_test_config());
    let connection = registry
        .connections
        .remove("default")
        .expect("default connection");
    registry.default_connection_id = "primary".to_owned();
    registry
        .connections
        .insert("primary".to_owned(), connection.clone());
    registry
        .connections
        .insert("secondary".to_owned(), connection);
    let options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .admission_config(crate::admission::AdmissionConfig {
            max_queue_wait_ms: 1000,
            ..Default::default()
        })
        .build();
    DatabaseManager::new_with_connection_registry_and_runtime_options(registry, options)
        .await
        .expect("two-connection manager")
}

async fn stream_noop(manager: &DatabaseManager, id: &str) -> StorageResult<()> {
    manager
        .run_stream_admitted(
            id,
            AdmissionClass::RangeRead,
            |_stream| async move { Ok(()) },
        )
        .await
}

/// Every foreground slot of `controller`, held until dropped.
fn hold_foreground(controller: &AdmissionController) -> Vec<AdmissionPermit> {
    let permits: Vec<_> =
        std::iter::from_fn(|| controller.try_acquire(AdmissionClass::Write).ok()).collect();
    assert!(!permits.is_empty(), "test requires foreground capacity");
    permits
}

fn key(bytes: &[u8]) -> StoreKey {
    StoreKey::try_from(bytes).expect("non-empty scheduled-job key")
}

fn regional_config(directory: &std::path::Path) -> Arc<regional_replication::RegionalConfig> {
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let path = directory.join("regional.json");
    let regions = serde_json::json!([
        {"name": "a", "rank": 1, "address": format!("127.0.0.1:{port}")},
        {"name": "b", "rank": 2, "address": "127.0.0.1:9"},
    ]);
    std::fs::write(
        &path,
        serde_json::json!({"local_region": "a", "listen": format!("127.0.0.1:{port}"), "regions": regions}).to_string(),
    )
    .unwrap();
    Arc::new(regional_replication::RegionalConfig::load(&path).unwrap())
}

#[tokio::test]
async fn given_named_connection_when_stream_operation_runs_then_it_is_served() {
    let m = two_connections().await;
    stream_noop(&m, "secondary")
        .await
        .expect("named connection serves stream operations");
}

#[tokio::test]
async fn given_unknown_connection_when_admission_is_requested_then_validation_names_it() {
    let m = two_connections().await;
    let error = m
        .acquire_admission("missing", AdmissionClass::PointRead)
        .await
        .expect_err("unknown connection is rejected");
    assert!(
        format!("{error:?}").contains("storage connection 'missing' is not configured"),
        "{error:?}"
    );
}

#[tokio::test]
async fn given_registry_default_named_primary_when_default_is_resolved_then_primary_answers() {
    let m = two_connections().await;
    for id in ["default", "primary"] {
        let controller = m.admission_controller(id).expect("controller");
        assert_eq!(controller.snapshot().connection_id, "primary", "{id}");
    }
    stream_noop(&m, "default").await.expect("default stream");
}

#[tokio::test]
async fn given_full_control_reserve_when_job_store_reads_then_it_waits_for_admission() {
    let m = two_connections().await;
    let controller = m
        .admission_controller("primary")
        .expect("primary controller");
    let reserve = controller.config().control_reserve_concurrency;
    assert!(reserve > 0, "test requires a control reserve");
    let mut control = (0..reserve)
        .map(|_| controller.try_acquire_control().expect("reserve slot"))
        .collect::<Vec<_>>();
    let store = m.scheduled_job_store().expect("job store");
    let probe = key(b"bound-probe");
    let blocked = tokio::time::timeout(Duration::from_millis(50), store.get(probe.clone())).await;
    assert!(
        !matches!(blocked, Ok(Ok(_))),
        "an unbound job store would read at once"
    );
    control.clear();
    let read = tokio::time::timeout(Duration::from_secs(1), store.get(probe))
        .await
        .expect("read completes after release")
        .expect("read succeeds");
    assert_eq!(read, None);
}

#[tokio::test]
async fn given_full_foreground_when_foreground_queue_reads_then_it_waits_for_admission() {
    let m = two_connections().await;
    let controller = m
        .admission_controller("primary")
        .expect("primary controller");
    let held = hold_foreground(&controller);
    let queue = m
        .messaging_store(crate::messaging::MessagingLane::Customer)
        .map(|store| store.queue)
        .expect("native foreground queue");
    let blocked = tokio::time::timeout(
        Duration::from_millis(50),
        queue.get_queue_by_name("absent-queue"),
    )
    .await;
    assert!(
        !matches!(blocked, Ok(Ok(_))),
        "an unbound queue store would read at once"
    );
    drop(held);
    let found = tokio::time::timeout(
        Duration::from_secs(1),
        queue.get_queue_by_name("absent-queue"),
    )
    .await
    .expect("lookup completes after release")
    .expect("lookup succeeds");
    assert!(found.is_none());
}

#[tokio::test]
async fn given_full_foreground_when_customer_pubsub_reads_then_it_waits_for_admission() {
    let m = two_connections().await;
    let controller = m
        .admission_controller("primary")
        .expect("primary controller");
    let held = hold_foreground(&controller);
    let pubsub = m
        .messaging_store(crate::messaging::MessagingLane::Customer)
        .expect("native customer store")
        .pubsub;
    let list = || pubsub.list_topics(pubsub_provider::ListTopicsRequest { next_token: None });
    let blocked = tokio::time::timeout(Duration::from_millis(50), list()).await;
    assert!(
        !matches!(blocked, Ok(Ok(_))),
        "an unbound pubsub store would read at once"
    );
    drop(held);
    let listed = tokio::time::timeout(Duration::from_secs(1), list())
        .await
        .expect("list completes after release")
        .expect("list succeeds");
    assert!(listed.topics.is_empty());
}

#[tokio::test]
async fn given_full_foreground_when_caller_pubsub_reads_then_it_does_not_wait() {
    let m = two_connections().await;
    let controller = m
        .admission_controller("primary")
        .expect("primary controller");
    let held = hold_foreground(&controller);
    let pubsub = m
        .messaging_store(crate::messaging::MessagingLane::Caller)
        .expect("native caller store")
        .pubsub;
    let listed = tokio::time::timeout(
        Duration::from_secs(1),
        pubsub.list_topics(pubsub_provider::ListTopicsRequest { next_token: None }),
    )
    .await
    .expect("the caller lane takes no admission permit")
    .expect("list succeeds");
    assert!(listed.topics.is_empty());
    drop(held);
}

#[tokio::test]
async fn given_secondary_admission_full_when_default_stream_runs_then_only_secondary_waits() {
    let m = two_connections().await;
    let secondary = m
        .admission_controller("secondary")
        .expect("secondary controller");
    let held = hold_foreground(&secondary);
    tokio::time::timeout(Duration::from_secs(1), stream_noop(&m, "primary"))
        .await
        .expect("primary is not blocked by secondary")
        .expect("primary stream operation");
    let blocked =
        tokio::time::timeout(Duration::from_millis(50), stream_noop(&m, "secondary")).await;
    assert!(
        !matches!(blocked, Ok(Ok(()))),
        "secondary waits behind its own full controller"
    );
    drop(held);
}

#[tokio::test]
async fn given_no_regional_file_when_jobs_run_then_no_regional_attachment_or_server() {
    let m = DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await
    .expect("manager with jobs");
    assert!(m.default_connection.regional.is_none());
    assert!(
        m.named_connections
            .values()
            .all(|connection| connection.regional.is_none())
    );
    match &*m.background_services.lock().await {
        BackgroundServiceState::Running(services) => {
            assert!(services.regional_servers.is_empty());
        }
        _ => panic!("services must be running"),
    }
    m.shutdown_background_services().await.expect("shutdown");
}

#[tokio::test]
async fn given_regional_file_when_jobs_off_or_connection_unknown_then_construction_fails() {
    let directory = tempfile::tempdir().unwrap();
    let config = regional_config(directory.path());

    let registry = single_connection_registry(immediate_sqlite_test_config());
    let mut options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(false)
        .build();
    options
        .regional_connections
        .insert("default".to_owned(), config.clone());
    let error =
        DatabaseManager::new_with_connection_registry_and_runtime_options(registry, options)
            .await
            .err()
            .expect("jobs off with a regional file fails");
    assert!(
        format!("{error:?}").contains("regional replication needs database jobs"),
        "{error:?}"
    );

    let registry = single_connection_registry(immediate_sqlite_test_config());
    let mut options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(true)
        .build();
    options
        .regional_connections
        .insert("other".to_owned(), config);
    let error =
        DatabaseManager::new_with_connection_registry_and_runtime_options(registry, options)
            .await
            .err()
            .expect("unknown connection fails");
    assert!(
        format!("{error:?}").contains("regional file for unknown connection 'other'"),
        "{error:?}"
    );
}
