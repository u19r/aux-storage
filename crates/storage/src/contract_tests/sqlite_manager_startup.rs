#![cfg(feature = "sqlite")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::future::join_all;
use storage_types::context::WrappedError;

use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, StorageEnum, StorageError,
    provider::{StorageBackend, StorageConfig},
};

const CONCURRENCY: usize = 8;
const ROUNDS: usize = 2;
const PROBE_TIMEOUT: Duration = Duration::from_secs(45);
const STARTUP_CONTEXT_STAGES: [(&str, &str); 7] = [
    ("create storage provider", "provider creation"),
    ("initialize storage", "storage initialization"),
    ("initialize stream", "stream initialization"),
    (
        "bootstrap scheduled job store",
        "scheduled-job store bootstrap",
    ),
    ("create sys namespaces", "system namespace table bootstrap"),
    ("create sys analytics", "system analytics table bootstrap"),
    ("complete storage provider", "provider completion"),
];

static PATH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct StartupFailure {
    round: usize,
    worker: usize,
    category: &'static str,
    stage: &'static str,
    contexts: Vec<String>,
    internal_detail: Option<String>,
}

impl StartupFailure {
    fn summary(&self) -> String {
        format!(
            "round={} worker={} category={} stage={} contexts={:?} internal_detail={:?}",
            self.round, self.worker, self.category, self.stage, self.contexts, self.internal_detail
        )
    }
}

fn unique_database_path(round: usize, worker: usize) -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_nanos();
    let sequence = PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "aux-storage-sqlite-manager-startup-{}-{timestamp}-{round}-{worker}-{sequence}.db",
        std::process::id()
    ))
}

fn sqlite_config(path: &Path) -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some(path.to_string_lossy().into_owned()),
        file_path: None,
        // Keep this probe aligned with the reported consumer constructor. The
        // provider applies its default SQLite settings when this is absent.
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn runtime_options() -> DatabaseManagerRuntimeOptions {
    DatabaseManagerRuntimeOptions::builder()
        .enable_single_table_mode(true)
        .scheduled_job_store_config(bg_job_store::ScheduledJobStoreConfig::from(
            Duration::from_secs(5),
        ))
        .build()
}

fn remove_sqlite_artifacts(path: &Path) {
    for artifact in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = fs::remove_file(artifact);
    }
}

fn error_category(error: &StorageError) -> &'static str {
    match error.to_enum() {
        StorageEnum::InternalServerError { .. } => "internal_server_error",
        StorageEnum::Database(_) => "database",
        StorageEnum::Serialization(_) => "serialization",
        StorageEnum::Validation { .. } => "validation",
        StorageEnum::RawValidation { .. } => "raw_validation",
        _ => "other",
    }
}

fn startup_stage(contexts: &[String], internal_detail: Option<&str>) -> &'static str {
    if internal_detail == Some("scheduled-job seed initialization failed") {
        return "scheduled-job seed initialization";
    }
    STARTUP_CONTEXT_STAGES
        .iter()
        .find_map(|(needle, stage)| {
            contexts
                .iter()
                .any(|context| context.contains(needle))
                .then_some(*stage)
        })
        .unwrap_or("unknown startup stage")
}

fn safe_internal_detail(error: &StorageError) -> Option<String> {
    let StorageEnum::InternalServerError { message } = error.to_enum() else {
        return None;
    };
    match message.as_str() {
        "scheduled-job seed initialization failed" | "scheduled-job owner generation failed" => {
            Some(message.clone())
        }
        _ => Some("<redacted internal diagnostic>".to_owned()),
    }
}

async fn start_manager(round: usize, worker: usize) -> Result<(), StartupFailure> {
    let path = unique_database_path(round, worker);
    let result = DatabaseManager::new_with_config_and_runtime_options(
        sqlite_config(&path),
        runtime_options(),
    )
    .await;
    let outcome = match result {
        Ok(manager) => {
            drop(manager);
            Ok(())
        }
        Err(error) => {
            let internal_detail = safe_internal_detail(&error);
            let (_, contexts) = error.recursive_context(Vec::new());
            Err(StartupFailure {
                round,
                worker,
                category: error_category(&error),
                stage: startup_stage(&contexts, internal_detail.as_deref()),
                contexts,
                internal_detail,
            })
        }
    };
    remove_sqlite_artifacts(&path);
    outcome
}

async fn run_probe() -> Vec<StartupFailure> {
    let mut failures = Vec::new();
    for round in 0..ROUNDS {
        let outcomes = join_all((0..CONCURRENCY).map(|worker| start_manager(round, worker))).await;
        failures.extend(outcomes.into_iter().filter_map(Result::err));
    }
    failures
}

#[tokio::test]
async fn given_independent_sqlite_paths_when_managers_start_concurrently_then_all_start() {
    let failures = tokio::time::timeout(PROBE_TIMEOUT, run_probe())
        .await
        .expect("bounded SQLite startup probe must complete within 45 seconds");
    let attempts = ROUNDS * CONCURRENCY;
    eprintln!(
        "sqlite startup probe: rounds={ROUNDS} concurrency={CONCURRENCY} attempts={attempts} \
         succeeded={} failed={}",
        attempts - failures.len(),
        failures.len()
    );
    if !failures.is_empty() {
        let diagnostics = failures
            .iter()
            .map(StartupFailure::summary)
            .collect::<Vec<_>>()
            .join("\n");
        panic!("independent SQLite manager startup failures:\n{diagnostics}");
    }
}
