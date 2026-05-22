use crate::builder::use_in_memory_job_lock_for_path;

#[test]
fn file_backed_sqlite_paths_use_storage_backed_job_locks_even_in_tests() {
    assert!(!use_in_memory_job_lock_for_path("main.db"));
    assert!(!use_in_memory_job_lock_for_path(
        "/tmp/aux-storage-test/main.db"
    ));
    assert!(!use_in_memory_job_lock_for_path(
        "sqlite:/tmp/aux-storage-test/main.db"
    ));
}

#[test]
fn memory_sqlite_paths_can_use_in_memory_job_locks_for_new_for_test() {
    assert!(use_in_memory_job_lock_for_path(":memory:"));
}
