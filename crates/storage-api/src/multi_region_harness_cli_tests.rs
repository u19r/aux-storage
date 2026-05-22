use std::{path::PathBuf, time::Duration};

use crate::{
    multi_region_harness::{HarnessScenario, SimulationStorageBackend},
    multi_region_harness_cli::{
        HarnessCliRunArgs, HarnessScenarioArg, HarnessStorageBackendArg, default_report_path,
        harness_run_config_from_args, harness_scenario, scenario_name, storage_backend,
    },
};

fn args_for(scenario: HarnessScenarioArg) -> HarnessCliRunArgs {
    HarnessCliRunArgs {
        scenario,
        regions: 3,
        storage_backend: HarnessStorageBackendArg::Postgres,
        region_storage_backends: vec![
            HarnessStorageBackendArg::Postgres,
            HarnessStorageBackendArg::Rocksdb,
            HarnessStorageBackendArg::Turso,
        ],
        sqlite_database_dir: Some(PathBuf::from("dbs")),
        postgres_dsn_template: Some(
            "host=/tmp dbname=postgres options='-csearch_path=mr_{node_id}'".to_string(),
        ),
        postgres_max_pool_size: 4,
        postgres_tls: false,
        foundationdb_cluster_file: Some("/tmp/fdb.cluster".to_string()),
        foundationdb_subspace_prefix: Some("mr-prefix".to_string()),
        duration_secs: 11,
        warmup_secs: 2,
        cooldown_secs: 4,
        ops_per_sec: 250,
        item_size_bytes: 2048,
        hot_key_percent: 10,
        hot_key_count: 5,
        delete_percent: 7,
        read_percent: 40,
        sample_every: 9,
        load_workers: 6,
        max_in_flight_convergence_checks: 12,
        batch_mutation_limit: 100,
        batch_byte_limit: 4096,
        bootstrap_item_count: 17,
        seed: 99,
        emulated_clock_skew_ms: 30,
        apply_latency_ms: 1,
        apply_latency_us: 500,
        apply_latency_jitter_ms: 2,
        apply_latency_jitter_us: 250,
        heartbeat_latency_ms: 3,
        heartbeat_latency_us: 750,
        heartbeat_latency_jitter_ms: 4,
        heartbeat_latency_jitter_us: 125,
        drop_probability_per_10k: 5,
        duplicate_probability_per_10k: 6,
        queue_probability_per_10k: 7,
        table_name: "harness-table".to_string(),
        report_path: Some(PathBuf::from("report.json")),
    }
}

#[test]
fn scenario_args_map_to_harness_scenarios_and_report_names() {
    assert!(matches!(
        harness_scenario(HarnessScenarioArg::Perf),
        HarnessScenario::Perf
    ));
    assert!(matches!(
        harness_scenario(HarnessScenarioArg::Soak),
        HarnessScenario::Soak
    ));
    assert!(matches!(
        harness_scenario(HarnessScenarioArg::Chaos),
        HarnessScenario::Chaos
    ));
    assert!(matches!(
        harness_scenario(HarnessScenarioArg::Bootstrap),
        HarnessScenario::Bootstrap
    ));
    assert_eq!(scenario_name(HarnessScenario::Perf), "perf");
    assert_eq!(scenario_name(HarnessScenario::Soak), "soak");
    assert_eq!(scenario_name(HarnessScenario::Chaos), "chaos");
    assert_eq!(scenario_name(HarnessScenario::Bootstrap), "bootstrap");
    assert!(matches!(
        storage_backend(HarnessStorageBackendArg::Sqlite),
        SimulationStorageBackend::Sqlite
    ));
    assert!(matches!(
        storage_backend(HarnessStorageBackendArg::Foundationdb),
        SimulationStorageBackend::Foundationdb
    ));
}

#[test]
fn explicit_args_build_complete_harness_run_config() {
    let config = harness_run_config_from_args(args_for(HarnessScenarioArg::Chaos));

    assert!(matches!(config.scenario, HarnessScenario::Chaos));
    assert_eq!(config.table_name.as_ref(), "harness-table");
    assert_eq!(config.regions, 3);
    assert_eq!(config.storage_backend, SimulationStorageBackend::Postgres);
    assert_eq!(
        config.region_storage_backends,
        vec![
            SimulationStorageBackend::Postgres,
            SimulationStorageBackend::Rocksdb,
            SimulationStorageBackend::Turso,
        ]
    );
    assert_eq!(
        config.sqlite_database_dir.as_deref(),
        Some(PathBuf::from("dbs").as_path())
    );
    assert_eq!(
        config.postgres_dsn_template.as_deref(),
        Some("host=/tmp dbname=postgres options='-csearch_path=mr_{node_id}'")
    );
    assert_eq!(config.postgres_max_pool_size, 4);
    assert!(!config.postgres_tls);
    assert_eq!(
        config.foundationdb_cluster_file.as_deref(),
        Some("/tmp/fdb.cluster")
    );
    assert_eq!(
        config.foundationdb_subspace_prefix.as_deref(),
        Some("mr-prefix")
    );
    assert_eq!(config.duration, Duration::from_secs(11));
    assert_eq!(config.warmup, Duration::from_secs(2));
    assert_eq!(config.cooldown, Duration::from_secs(4));
    assert_eq!(config.ops_per_sec, 250);
    assert_eq!(config.item_size_bytes, 2048);
    assert_eq!(config.hot_key_percent, 10);
    assert_eq!(config.delete_percent, 7);
    assert_eq!(config.read_percent, 40);
    assert_eq!(config.max_in_flight_convergence_checks, 12);
    assert_eq!(config.batch_mutation_limit, 100);
    assert_eq!(config.batch_byte_limit, 4096);
    assert_eq!(config.bootstrap_item_count, 17);
    assert_eq!(config.seed, 99);
    assert_eq!(config.emulated_clock_skew_ms, 30);
    assert_eq!(config.fault_profile.apply_latency_ms, 1);
    assert_eq!(config.fault_profile.apply_latency_us, 500);
    assert_eq!(config.fault_profile.apply_latency_jitter_ms, 2);
    assert_eq!(config.fault_profile.apply_latency_jitter_us, 250);
    assert_eq!(config.fault_profile.heartbeat_latency_ms, 3);
    assert_eq!(config.fault_profile.heartbeat_latency_us, 750);
    assert_eq!(config.fault_profile.heartbeat_latency_jitter_ms, 4);
    assert_eq!(config.fault_profile.heartbeat_latency_jitter_us, 125);
    assert_eq!(config.fault_profile.drop_probability_per_10k, 5);
    assert_eq!(config.fault_profile.duplicate_probability_per_10k, 6);
    assert_eq!(config.fault_profile.queue_probability_per_10k, 7);
    assert_eq!(
        config.report_path.as_deref(),
        Some(PathBuf::from("report.json").as_path())
    );
}

#[test]
fn default_report_path_uses_workspace_multi_region_directory_and_scenario_name() {
    let path = default_report_path(HarnessScenario::Soak);

    assert!(
        path.parent()
            .is_some_and(|parent| parent.ends_with("run-artifacts/multi-region"))
    );
    assert!(path.file_name().is_some_and(|name| {
        name.to_string_lossy().starts_with("soak-") && name.to_string_lossy().ends_with(".json")
    }));
}
