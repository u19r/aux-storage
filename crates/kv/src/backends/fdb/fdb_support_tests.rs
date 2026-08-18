use std::{path::PathBuf, sync::OnceLock, time::Duration};

use metrics_exporter_prometheus::PrometheusHandle;
use uuid::Uuid;

use crate::{FoundationDbConfig, FoundationDbKvStore};

const DEFAULT_CLUSTER_FILE_PATHS: &[&str] = &[
    "/usr/local/etc/foundationdb/fdb.cluster",
    "/opt/homebrew/etc/foundationdb/fdb.cluster",
    "/etc/foundationdb/fdb.cluster",
];

fn local_cluster_file_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("FDB_CLUSTER_FILE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Some(path);
        }
    }

    DEFAULT_CLUSTER_FILE_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
}

pub(crate) async fn connect_fdb_store(test_scope: &str) -> Option<FoundationDbKvStore> {
    connect_fdb_store_with_cache(test_scope, 0).await
}

pub(crate) async fn connect_fdb_store_with_cache(
    test_scope: &str,
    cache_read_version_ms: u16,
) -> Option<FoundationDbKvStore> {
    let cluster_file_path = local_cluster_file_path()?;
    let prefix = format!("tests/{test_scope}/{}/", Uuid::now_v7()).into_bytes();
    let store = match FoundationDbKvStore::connect(FoundationDbConfig {
        cluster_file_path: Some(cluster_file_path.to_string_lossy().to_string()),
        tenant_name: None,
        subspace_prefix: Some(prefix),
        cache_read_version_ms,
        immediate_gsi_consistency: false,
        report_conflicting_keys: false,
    }) {
        Ok(store) => store,
        Err(error) => {
            eprintln!("Skipping FoundationDB test: unable to connect: {error}");
            return None;
        }
    };
    if let Err(error) = store.check_reachable(Duration::from_secs(3)).await {
        eprintln!("Skipping FoundationDB test: reachability check failed: {error}");
        return None;
    }
    Some(store)
}

pub(super) fn metrics_handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .install_recorder()
            .expect("install metrics recorder")
    })
}

pub(super) fn parse_metric_value(
    handle: &PrometheusHandle,
    metric: &str,
    label_fragments: &[&str],
) -> f64 {
    let body = handle.render();
    for line in body.lines() {
        if !line.starts_with(metric) {
            continue;
        }
        if !label_fragments
            .iter()
            .all(|fragment| line.contains(fragment))
        {
            continue;
        }
        if let Some(value) = line.split_whitespace().last()
            && let Ok(parsed) = value.parse::<f64>()
        {
            return parsed;
        }
    }
    0.0
}
