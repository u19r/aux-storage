use crate::builder::use_in_memory_job_lock_for_path;
#[cfg(feature = "foundationdb")]
use crate::constants::FOUNDATIONDB_STARTUP_REACHABILITY_TIMEOUT_SECS;

#[test]
fn file_backed_sqlite_paths_use_storage_backed_job_locks_even_in_tests() {
    assert!(!use_in_memory_job_lock_for_path("main.db"));
    assert!(!use_in_memory_job_lock_for_path(
        "run-artifacts/storage-data/main.db"
    ));
    assert!(!use_in_memory_job_lock_for_path(
        "sqlite:run-artifacts/storage-data/main.db"
    ));
}

#[test]
fn memory_sqlite_paths_can_use_in_memory_job_locks_for_new_for_test() {
    assert!(use_in_memory_job_lock_for_path(":memory:"));
}

#[test]
#[cfg(feature = "foundationdb")]
fn foundationdb_startup_reachability_timeout_allows_private_cluster_startup() {
    const {
        assert!(FOUNDATIONDB_STARTUP_REACHABILITY_TIMEOUT_SECS >= 30);
    }
}
