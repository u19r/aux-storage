use std::{sync::OnceLock, time::Duration};

use storage_types::TableName;
use tokio::sync::{Mutex, MutexGuard};

#[cfg(any(feature = "rocksdb", feature = "turso"))]
use crate::multi_region_harness::SimulationStorageBackend;
use crate::multi_region_harness::{
    HarnessFaultProfile, HarnessRunConfig, HarnessScenario, MultiRegionHarnessRunner,
    SimulationHarness, SimulationHarnessConfig,
};

fn simulation_harness_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

async fn acquire_simulation_harness_lock() -> MutexGuard<'static, ()> {
    simulation_harness_lock().lock().await
}

#[tokio::test]
async fn simulation_four_region_converges_under_partition_delay_duplication_and_reordering() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
            "region-d".to_string(),
        ],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-four-region");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "base", 0)
        .await
        .expect("put base");
    harness.run_until_idle(10).await.expect("drain replication");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("base"))
            .await
            .expect("validate base convergence")
    );

    harness.block_link("region-a", "region-b", true);
    harness.queue_link("region-c", "region-d", true);
    harness.drop_next_apply("region-a", "region-d");
    harness.duplicate_next_apply("region-b", "region-a");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "from-a", 0)
        .await
        .expect("put from a");
    tokio::time::sleep(Duration::from_millis(5)).await;
    harness
        .put_item_value("region-c", &table_name, "pk1", "sk1", "from-c", 0)
        .await
        .expect("put from c");

    harness.run_until_idle(6).await.expect("partial drain");
    harness.block_link("region-a", "region-b", false);
    harness.queue_link("region-c", "region-d", false);
    harness
        .flush_queued_applies("region-c", "region-d", true)
        .await
        .expect("flush queued applies");
    harness.run_until_idle(20).await.expect("final drain");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("from-c"))
            .await
            .expect("validate final convergence")
    );
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn simulation_global_table_converges_on_rocksdb() {
    let _guard = acquire_simulation_harness_lock().await;
    simulation_global_table_converges_for_backend(
        SimulationStorageBackend::Rocksdb,
        "simulation-global-rocksdb",
    )
    .await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn simulation_global_table_converges_on_turso() {
    let _guard = acquire_simulation_harness_lock().await;
    simulation_global_table_converges_for_backend(
        SimulationStorageBackend::Turso,
        "simulation-global-turso",
    )
    .await;
}

#[cfg(any(feature = "rocksdb", feature = "turso"))]
async fn simulation_global_table_converges_for_backend(
    storage_backend: SimulationStorageBackend,
    table_name: &str,
) {
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        storage_backend,
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new(table_name);
    harness
        .create_global_table(&table_name)
        .await
        .expect("create global table");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "global-value", 0)
        .await
        .expect("put global value");
    harness
        .run_until_idle(200)
        .await
        .expect("drain replication");
    let mut values = Vec::new();
    for region in harness.region_names() {
        values.push((
            region.clone(),
            harness
                .get_item_value(region, &table_name, "pk1", "sk1")
                .await
                .expect("read region value"),
        ));
    }
    assert_eq!(
        values,
        vec![
            ("region-a".to_string(), Some("global-value".to_string())),
            ("region-b".to_string(), Some("global-value".to_string())),
            ("region-c".to_string(), Some("global-value".to_string())),
        ]
    );
}

#[tokio::test]
async fn simulation_token_rotation_with_overlap_keeps_replication_online() {
    let _guard = acquire_simulation_harness_lock().await;
    let mut harness = SimulationHarness::new(SimulationHarnessConfig::default())
        .await
        .expect("build simulation harness");
    let table_name = TableName::new("simulation-token-rotation");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "v1", 0)
        .await
        .expect("put v1");
    harness.run_until_idle(10).await.expect("drain v1");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("v1"))
            .await
            .expect("validate v1")
    );

    harness.accept_token("region-a", "region-b", "region-a-token-v2");
    harness.rotate_outbound_token("region-a", "region-b", "region-a-token-v2");
    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "v2", 0)
        .await
        .expect("put v2");
    harness.run_until_idle(10).await.expect("drain v2");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("v2"))
            .await
            .expect("validate v2")
    );

    harness.revoke_token("region-a", "region-b", "region-a-token-v1");
    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "v3", 0)
        .await
        .expect("put v3");
    harness.run_until_idle(10).await.expect("drain v3");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("v3"))
            .await
            .expect("validate v3")
    );
}

#[tokio::test]
async fn simulation_bootstrap_catches_up_empty_replacement_region() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig::default())
        .await
        .expect("build simulation harness");
    let table_name = TableName::new("simulation-bootstrap");
    harness
        .create_stream_table_in_all_regions(&table_name)
        .await
        .expect("create stream tables");
    harness
        .create_bootstrap_replica("region-a", "region-b", &table_name)
        .await
        .expect("create bootstrap replica");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "bootstrap-value", 0)
        .await
        .expect("put bootstrap value");
    harness.run_until_idle(20).await.expect("drain bootstrap");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("bootstrap-value"))
            .await
            .expect("validate bootstrap value")
    );
}

#[tokio::test]
async fn simulation_bootstrap_converges_after_probabilistic_queue_fault() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        queue_probability_per_10k: 10_000,
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-bootstrap-probabilistic-queue");
    harness
        .create_stream_table_in_all_regions(&table_name)
        .await
        .expect("create stream tables");
    harness
        .create_bootstrap_replica("region-a", "region-b", &table_name)
        .await
        .expect("create bootstrap replica");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "queued-bootstrap", 0)
        .await
        .expect("put bootstrap value");
    harness.run_until_idle(20).await.expect("drain bootstrap");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("queued-bootstrap"))
            .await
            .expect("validate queued bootstrap value")
    );
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn simulation_bootstrap_catches_up_empty_replacement_region_on_rocksdb() {
    let _guard = acquire_simulation_harness_lock().await;
    simulation_bootstrap_catches_up_empty_replacement_region_for_backend(
        SimulationStorageBackend::Rocksdb,
        "simulation-bootstrap-rocksdb",
    )
    .await;
}

#[cfg(feature = "turso")]
#[tokio::test]
async fn simulation_bootstrap_catches_up_empty_replacement_region_on_turso() {
    let _guard = acquire_simulation_harness_lock().await;
    simulation_bootstrap_catches_up_empty_replacement_region_for_backend(
        SimulationStorageBackend::Turso,
        "simulation-bootstrap-turso",
    )
    .await;
}

#[cfg(any(feature = "rocksdb", feature = "turso"))]
async fn simulation_bootstrap_catches_up_empty_replacement_region_for_backend(
    storage_backend: SimulationStorageBackend,
    table_name: &str,
) {
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        storage_backend,
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new(table_name);
    harness
        .create_stream_table_in_all_regions(&table_name)
        .await
        .expect("create stream tables");
    harness
        .create_bootstrap_replica("region-a", "region-b", &table_name)
        .await
        .expect("create bootstrap replica");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "bootstrap-value", 0)
        .await
        .expect("put bootstrap value");
    harness.run_until_idle(20).await.expect("drain bootstrap");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("bootstrap-value"))
            .await
            .expect("validate bootstrap value")
    );
}

#[tokio::test]
async fn simulation_delete_conflict_converges_on_latest_winner() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-delete-conflict");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "present", 0)
        .await
        .expect("put present");
    harness.run_until_idle(10).await.expect("drain initial");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", Some("present"))
            .await
            .expect("validate present")
    );

    harness.block_link("region-b", "region-a", true);
    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "updated", 0)
        .await
        .expect("put updated");
    tokio::time::sleep(Duration::from_millis(5)).await;
    harness
        .delete_item("region-b", &table_name, "pk1", "sk1")
        .await
        .expect("delete item");
    harness.run_until_idle(6).await.expect("partial drain");

    harness.block_link("region-b", "region-a", false);
    harness.run_until_idle(20).await.expect("final drain");
    assert!(
        harness
            .all_regions_match_value(&table_name, "pk1", "sk1", None)
            .await
            .expect("validate absence")
    );
}

#[tokio::test]
async fn simulation_lww_conflict_converges_with_sync_under_one_region() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        single_node_sync_regions: vec!["region-a".to_string()],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-sync-under-one-region");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value(
            "region-b",
            &table_name,
            "pk1",
            "sk1",
            "from-ordinary-region-b",
            0,
        )
        .await
        .expect("put from first ordinary region");
    assert!(
        harness
            .step_region("region-b", false)
            .await
            .expect("replicate first ordinary write"),
        "region-b should send its first ordinary write"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    harness
        .put_item_value(
            "region-c",
            &table_name,
            "pk1",
            "sk1",
            "from-ordinary-region-c",
            0,
        )
        .await
        .expect("put from second ordinary region");
    assert!(
        harness
            .step_region("region-c", false)
            .await
            .expect("replicate second ordinary write"),
        "region-c should send its later ordinary write"
    );

    harness.run_until_idle(20).await.expect("drain replication");

    let values = [
        harness
            .get_item_value("region-a", &table_name, "pk1", "sk1")
            .await
            .expect("region-a value"),
        harness
            .get_item_value("region-b", &table_name, "pk1", "sk1")
            .await
            .expect("region-b value"),
        harness
            .get_item_value("region-c", &table_name, "pk1", "sk1")
            .await
            .expect("region-c value"),
    ];
    assert_eq!(
        values,
        [
            Some("from-ordinary-region-c".to_string()),
            Some("from-ordinary-region-c".to_string()),
            Some("from-ordinary-region-c".to_string())
        ]
    );
}

#[tokio::test]
async fn local_perf_harness_runs_on_one_machine() {
    let _guard = acquire_simulation_harness_lock().await;
    let report = MultiRegionHarnessRunner::run(HarnessRunConfig {
        scenario: HarnessScenario::Perf,
        duration: Duration::from_secs(2),
        warmup: Duration::from_millis(200),
        cooldown: Duration::from_millis(200),
        ops_per_sec: 50,
        sample_every: 10,
        ..HarnessRunConfig::default()
    })
    .await
    .expect("run perf harness");
    assert!(report.operations.succeeded > 0);
    assert!(report.consistency.final_converged);
}

#[tokio::test]
async fn simulation_reports_replication_watermarks_for_sampled_writes() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig::default())
        .await
        .expect("build simulation harness");
    let table_name = TableName::new("simulation-watermark-progress");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value("region-a", &table_name, "pk1", "sk1", "v1", 0)
        .await
        .expect("put item");
    harness.run_until_idle(20).await.expect("drain replication");

    let commit_ts = harness
        .get_item_origin_commit_ts("region-a", &table_name, "pk1", "sk1")
        .await
        .expect("get origin commit ts")
        .expect("sampled write commit ts");
    assert!(
        harness
            .all_replica_regions_applied_commit("region-a", commit_ts)
            .await
            .expect("validate watermark convergence")
    );
}

#[tokio::test]
async fn local_chaos_harness_exercises_hot_key_lww() {
    let _guard = acquire_simulation_harness_lock().await;
    let report = MultiRegionHarnessRunner::run(HarnessRunConfig {
        scenario: HarnessScenario::Chaos,
        regions: 3,
        duration: Duration::from_secs(2),
        warmup: Duration::from_millis(200),
        cooldown: Duration::from_millis(200),
        ops_per_sec: 40,
        hot_key_percent: 80,
        hot_key_count: 4,
        fault_profile: HarnessFaultProfile {
            apply_latency_ms: 3,
            apply_latency_jitter_ms: 3,
            heartbeat_latency_ms: 1,
            heartbeat_latency_jitter_ms: 1,
            drop_probability_per_10k: 200,
            duplicate_probability_per_10k: 200,
            queue_probability_per_10k: 150,
            ..HarnessFaultProfile::default()
        },
        ..HarnessRunConfig::default()
    })
    .await
    .expect("run chaos harness");
    assert!(report.operations.succeeded > 0);
    assert!(report.consistency.final_converged);
}

#[tokio::test]
async fn simulation_large_item_put_delete_conflict_converges_under_reordering() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-large-item-put-delete");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value(
            "region-a",
            &table_name,
            "hot-pk",
            "hot-sk",
            "base",
            64 * 1024,
        )
        .await
        .expect("put base");
    harness.run_until_idle(10).await.expect("drain base");

    harness.block_link("region-a", "region-b", true);
    harness.queue_link("region-c", "region-b", true);
    harness
        .delete_item("region-a", &table_name, "hot-pk", "hot-sk")
        .await
        .expect("delete hot key");
    tokio::time::sleep(Duration::from_millis(5)).await;
    harness
        .put_item_value(
            "region-c",
            &table_name,
            "hot-pk",
            "hot-sk",
            "large-put-wins",
            64 * 1024,
        )
        .await
        .expect("put large winner");

    harness.run_until_idle(6).await.expect("partial drain");
    harness.block_link("region-a", "region-b", false);
    harness.queue_link("region-c", "region-b", false);
    harness
        .flush_queued_applies("region-c", "region-b", true)
        .await
        .expect("flush queued applies");
    harness.run_until_idle(20).await.expect("final drain");

    assert!(
        harness
            .all_regions_match_value(&table_name, "hot-pk", "hot-sk", Some("large-put-wins"))
            .await
            .expect("validate final convergence")
    );
}

#[tokio::test]
async fn simulation_delete_heavy_catchup_survives_restart_style_peer_steps() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-delete-heavy-catchup");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    for index in 0..48 {
        harness
            .put_item_value(
                "region-a",
                &table_name,
                &format!("pk-{index}"),
                &format!("sk-{index}"),
                &format!("seed-{index}"),
                0,
            )
            .await
            .expect("seed item");
    }
    harness.run_until_idle(30).await.expect("drain seed");

    harness.block_link("region-a", "region-b", true);
    for index in 0..48 {
        harness
            .delete_item(
                "region-a",
                &table_name,
                &format!("pk-{index}"),
                &format!("sk-{index}"),
            )
            .await
            .expect("delete item");
    }

    for _ in 0..6 {
        let _ = harness
            .step_region("region-a", true)
            .await
            .expect("step source region");
        let _ = harness
            .step_region("region-c", true)
            .await
            .expect("step healthy peer");
    }

    harness.block_link("region-a", "region-b", false);
    for _ in 0..24 {
        let _ = harness
            .step_region("region-a", true)
            .await
            .expect("step source catchup");
        let _ = harness
            .step_region("region-b", true)
            .await
            .expect("step recovering peer");
        let _ = harness
            .step_region("region-c", true)
            .await
            .expect("step healthy peer");
        harness
            .flush_all_queued_applies(false)
            .await
            .expect("flush queued applies");
    }
    harness.run_until_idle(30).await.expect("final drain");

    for index in 0..48 {
        assert!(
            harness
                .all_regions_match_value(
                    &table_name,
                    &format!("pk-{index}"),
                    &format!("sk-{index}"),
                    None,
                )
                .await
                .expect("validate absence")
        );
    }
}

#[tokio::test]
async fn simulation_hot_key_conflict_with_emulated_clock_skew_converges() {
    let _guard = acquire_simulation_harness_lock().await;
    let harness = SimulationHarness::new(SimulationHarnessConfig {
        region_names: vec![
            "region-a".to_string(),
            "region-b".to_string(),
            "region-c".to_string(),
        ],
        emulated_clock_skew_ms: 5,
        ..SimulationHarnessConfig::default()
    })
    .await
    .expect("build simulation harness");
    let table_name = TableName::new("simulation-hot-key-skew");
    harness
        .create_global_table(&table_name)
        .await
        .expect("create table");

    harness
        .put_item_value("region-a", &table_name, "hot-pk", "hot-sk", "from-a", 0)
        .await
        .expect("put from a");
    harness
        .put_item_value("region-b", &table_name, "hot-pk", "hot-sk", "from-b", 0)
        .await
        .expect("put from b");
    harness
        .put_item_value("region-c", &table_name, "hot-pk", "hot-sk", "from-c", 0)
        .await
        .expect("put from c");
    harness
        .run_until_idle(20)
        .await
        .expect("drain skewed writes");

    assert!(
        harness
            .all_regions_match_value(&table_name, "hot-pk", "hot-sk", Some("from-c"))
            .await
            .expect("validate skew winner")
    );
}
