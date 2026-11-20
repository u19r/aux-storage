use std::{collections::HashMap, time::Duration};

use storage_provider::StorageBackend;
use storage_types::DeadlineClass;

use super::runtime_config::*;

#[test]
fn given_default_file_when_translated_then_options_are_the_defaults() {
    let runtime = RuntimeConfig::from_file(&config::RootConfig::default()).unwrap();
    let defaults = crate::DatabaseManagerRuntimeOptions::default();

    assert_eq!(runtime.registry.default_connection_id, "default");
    assert_eq!(runtime.registry.connections.len(), 1);
    assert!(matches!(
        runtime.default_backend(),
        Some(StorageBackend::SQLite)
    ));

    assert!(runtime.options.enable_database_jobs);
    assert!(!runtime.options.enable_single_table_mode);
    assert!(runtime.options.metrics_facade.is_none());
    assert_eq!(runtime.options.admission_config, defaults.admission_config);
    for class in DeadlineClass::ALL {
        assert_eq!(
            runtime.options.attempt_deadlines.budget(class),
            defaults.attempt_deadlines.budget(class)
        );
    }
    assert_eq!(
        runtime.options.attempt_deadlines.commit_grace(),
        defaults.attempt_deadlines.commit_grace()
    );
    assert_eq!(
        runtime
            .options
            .database_job_intervals
            .gsi_backfill_interval_ms,
        defaults.database_job_intervals.gsi_backfill_interval_ms
    );

    // Multi-region off: no regional file read, nothing allocated.
    assert!(runtime.options.regional_connections.is_empty());
    assert_eq!(runtime.options.regional_connections.capacity(), 0);
}

#[test]
fn given_file_settings_when_translated_then_each_reaches_its_option() {
    let defaults = crate::DatabaseManagerRuntimeOptions::default();
    let mut root = config::RootConfig::default();
    root.features.runtime.enable_background_workers = false;
    root.jobs.gsi_backfill_interval_ms = 1_234;
    root.features.storage_admission.maximum_concurrency = 16;
    root.features.storage_deadlines.write_ms = 777;
    root.features.storage_deadlines.commit_grace_ms = 1_500;

    let options = RuntimeConfig::from_file(&root).unwrap().options;

    assert!(!options.enable_database_jobs);
    assert_eq!(
        options.database_job_intervals.gsi_backfill_interval_ms,
        storage_common::JobIntervalMillis(1_234)
    );
    assert_eq!(
        options.database_job_intervals.ttl_sweep_interval_ms,
        defaults.database_job_intervals.ttl_sweep_interval_ms
    );
    assert_eq!(options.admission_config.maximum_concurrency, 16);
    assert_eq!(
        options.admission_config.minimum_concurrency,
        defaults.admission_config.minimum_concurrency
    );
    assert_eq!(
        options.attempt_deadlines.budget(DeadlineClass::Write),
        Duration::from_millis(777)
    );
    assert_eq!(
        options.attempt_deadlines.budget(DeadlineClass::PointRead),
        defaults.attempt_deadlines.budget(DeadlineClass::PointRead)
    );
    assert_eq!(
        options.attempt_deadlines.commit_grace(),
        Duration::from_millis(1_500)
    );
}

#[test]
fn given_invalid_admission_when_translated_then_the_rule_is_named() {
    let mut root = config::RootConfig::default();
    root.features.storage_admission.minimum_concurrency = 0;

    let error = RuntimeConfig::from_file(&root)
        .err()
        .expect("invalid admission");

    assert!(
        error
            .to_string()
            .contains("minimum_concurrency must be greater than zero"),
        "{error}"
    );
}

#[test]
fn given_zero_deadline_when_translated_then_rejected() {
    let mut root = config::RootConfig::default();
    root.features.storage_deadlines.point_read_ms = 0;

    let error = RuntimeConfig::from_file(&root)
        .err()
        .expect("zero deadline");

    assert!(
        error
            .to_string()
            .contains("attempt deadline budgets and commit grace must be positive"),
        "{error}"
    );
}

#[test]
fn given_regional_file_when_translated_then_config_is_stored() {
    let directory = tempfile::tempdir().unwrap();
    let valid = directory.path().join("regional.json");
    std::fs::write(
        &valid,
        r#"{"local_region":"a","listen":"127.0.0.1:7000","regions":[
            {"name":"a","rank":1,"address":"a.example:7000"},
            {"name":"b","rank":2,"address":"b.example:7000"}]}"#,
    )
    .unwrap();
    let mut root = config::RootConfig::default();
    root.features.regional_connections = HashMap::from([("default".to_owned(), valid)]);

    let options = RuntimeConfig::from_file(&root).unwrap().options;

    assert_eq!(options.regional_connections.len(), 1);
    assert_eq!(options.regional_connections["default"].peer.name, "b");

    let invalid = directory.path().join("invalid.json");
    std::fs::write(&invalid, r#"{"local_region":"a"}"#).unwrap();
    root.features.regional_connections = HashMap::from([("default".to_owned(), invalid)]);

    let error = RuntimeConfig::from_file(&root)
        .err()
        .expect("invalid regional file");

    assert!(error.to_string().contains("regional file"), "{error}");
}

#[cfg(not(feature = "turso"))]
#[tokio::test]
async fn given_turso_without_its_feature_when_opened_then_construction_rejects() {
    let dir = tempfile::tempdir().unwrap();
    let mut root = config::RootConfig::default();
    root.features.backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: Some(config::TursoBackendConfig {
            db_path: dir.path().join("t.db").display().to_string(),
            ..Default::default()
        }),
        postgres: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    let runtime = RuntimeConfig::from_file(&root).unwrap();
    assert!(matches!(
        runtime.default_backend(),
        Some(StorageBackend::Turso)
    ));
    let error = runtime.open().await.err().expect("turso not in this build");

    assert!(
        error
            .to_string()
            .contains("Turso storage backend is not enabled in this build"),
        "{error}"
    );
}

#[cfg(not(feature = "foundationdb"))]
#[tokio::test]
async fn given_foundationdb_without_its_feature_when_opened_then_construction_rejects() {
    let mut root = config::RootConfig::default();
    root.features.backends = config::Backends {
        initial_stream_lineages: None,
        sqlite: None,
        turso: None,
        postgres: None,
        rocksdb: None,
        foundationdb: Some(config::FoundationdbBackendConfig::default()),
        remote: None,
    };

    let error = RuntimeConfig::from_file(&root)
        .unwrap()
        .open()
        .await
        .err()
        .expect("foundationdb not in this build");

    assert!(
        error
            .to_string()
            .contains("FoundationDB storage backend is not enabled in this build"),
        "{error}"
    );
}

#[cfg(all(feature = "foundationdb", feature = "test-support"))]
#[tokio::test]
#[ignore = "requires a reachable FoundationDB cluster compatible with the pinned storage client"]
async fn given_foundationdb_file_config_when_opened_then_the_manager_serves_its_namespace() {
    let cluster_file = std::env::var("TEST_FDB_CLUSTER_FILE").expect("TEST_FDB_CLUSTER_FILE");
    let scope = crate::FoundationDbTestScope::try_new(cluster_file)
        .await
        .unwrap();
    let settings = scope.settings().clone();
    scope
        .run(async move {
            let mut root = config::RootConfig::default();
            root.features.runtime.enable_background_workers = false;
            root.features.backends = config::Backends {
                initial_stream_lineages: None,
                sqlite: None,
                turso: None,
                postgres: None,
                rocksdb: None,
                foundationdb: Some(config::FoundationdbBackendConfig {
                    cluster_file: settings.cluster_file.clone(),
                    subspace_prefix: settings.subspace_prefix.clone(),
                    cache_read_version_ms: 0,
                    ..config::FoundationdbBackendConfig::default()
                }),
                remote: None,
            };
            let manager = RuntimeConfig::from_file(&root)
                .unwrap()
                .open()
                .await
                .expect("FoundationDB manager from the file configuration");
            assert!(
                manager
                    .table_exists(&crate::Tables::sys_namespaces())
                    .await
                    .unwrap()
            );
            manager.shutdown_background_services().await.unwrap();
        })
        .await
        .unwrap();
}
