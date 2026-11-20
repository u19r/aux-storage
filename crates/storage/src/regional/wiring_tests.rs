use std::{sync::Arc, time::Duration};

use regional_replication::RegionalConfig;
use scheduled_bg_jobs::LifecycleObservation;
use storage_common::DatabaseJobIntervals;
use storage_types::{PairId, TableName};

use super::{PairJobs, RegionalPair, RegionalPairHandler, recorder_test_support::KeyRecorder};
use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, constants::REGIONAL_LAG_METRIC,
    maintenance_runtime::MaintenanceRuntime,
};

fn regional_config() -> Arc<RegionalConfig> {
    Arc::new(
        RegionalConfig::from_json(
            r#"{"local_region":"a","listen":"127.0.0.1:0","regions":[
                {"name":"a","rank":1,"address":"127.0.0.1:1"},
                {"name":"b","rank":2,"address":"127.0.0.1:9"}]}"#,
        )
        .unwrap(),
    )
}

/// Pair wiring and an unstarted maintenance runtime over an in-memory SQLite
/// store, with the pair-job handler registered.
async fn pair_and_runtime(namespace: &str) -> (Arc<RegionalPair>, MaintenanceRuntime) {
    let store = Arc::new(
        sql_backend::SqlStore::initialize_schema(
            sql_backend::sqlite::SqliteBackend::initialize(
                ":memory:",
                storage_types::AttemptDeadlines::default(),
            )
            .await
            .unwrap(),
            storage_types::InitialStreamLineages::ONE,
            true,
        )
        .await
        .unwrap(),
    );
    let bundle = crate::InitializedStore::native(store.clone()).caller_maintenance();
    let pair = RegionalPair::new("default", regional_config(), store);
    let runtime = MaintenanceRuntime::try_new(
        namespace,
        bundle.scheduled_job_store,
        bundle.maintenance.unwrap(),
        DatabaseJobIntervals::default(),
        Some(RegionalPairHandler::new(&pair)),
    )
    .unwrap();
    (pair, runtime)
}

#[tokio::test]
async fn given_pair_job_exists_when_prepare_repeats_then_ensure_keeps_one_job() {
    let (_pair, runtime) = pair_and_runtime("pair-jobs-ensure").await;
    let jobs = runtime
        .pair_jobs()
        .expect("a regional handler registers the pair job");
    let _seeded = runtime.initialize_seeds().await.unwrap();
    let table = TableName::new("orders");
    let pair_id = PairId::from_bytes([7; 16]);
    jobs.ensure(&table, pair_id).await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    jobs.ensure(&table, pair_id).await.unwrap();
    let observed = jobs
        .runtime
        .observe_lifecycle(PairJobs::job_id(pair_id))
        .await
        .unwrap();
    assert!(
        matches!(observed, LifecycleObservation::Current { .. }),
        "the pair job is current"
    );
}

#[tokio::test]
async fn given_handler_and_service_of_one_connection_then_they_share_one_pair() {
    let (pair, runtime) = pair_and_runtime("pair-shared").await;
    let handler = RegionalPairHandler::new(&pair);
    let attachment = super::regional_attachment(Some(Arc::clone(&pair)), &runtime)
        .unwrap()
        .expect("a pair yields an attachment");
    assert!(Arc::ptr_eq(&handler.pair, &attachment.service.pair));
    assert!(Arc::ptr_eq(&pair, &attachment.service.pair));
}

#[tokio::test]
async fn given_no_regional_file_when_manager_starts_then_no_lag_gauge_is_registered() {
    let recorder = KeyRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let manager = DatabaseManager::new_for_test_with_runtime_options(
        DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(true)
            .build(),
    )
    .await
    .unwrap();
    assert!(
        !recorder.gauges_named("storage.admission.limit").is_empty(),
        "the recorder saw construction"
    );
    assert!(recorder.gauges_named(REGIONAL_LAG_METRIC).is_empty());
    assert!(manager.regional_service("default").is_none());
    manager.shutdown_background_services().await.unwrap();
}

#[tokio::test]
async fn given_regional_file_when_manager_starts_then_one_lag_gauge_labelled_by_connection() {
    let recorder = KeyRecorder::default();
    let _guard = metrics::set_default_local_recorder(&recorder);
    let mut options = DatabaseManagerRuntimeOptions::builder()
        .enable_database_jobs(true)
        .build();
    options
        .regional_connections
        .insert("default".to_owned(), regional_config());
    let manager = DatabaseManager::new_for_test_with_runtime_options(options)
        .await
        .unwrap();
    assert!(manager.regional_service("default").is_some());
    assert_eq!(
        recorder.gauges_named(REGIONAL_LAG_METRIC),
        vec![(vec![("connection".to_owned(), "default".to_owned())], 0.0)]
    );
    manager.shutdown_background_services().await.unwrap();
}
