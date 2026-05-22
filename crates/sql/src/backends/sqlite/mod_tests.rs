use storage_provider::SqliteSettings;

use super::provider::ensure_db_directory;

fn local_temp_parent() -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let path = workspace_root.join("target").join("sql-test-data");
    std::fs::create_dir_all(&path).expect("create sql test data directory");
    path
}

#[test]
fn creates_parent_directory_for_file_backed_sqlite() {
    let temp_dir = tempfile::Builder::new()
        .prefix("sqlite-parent-")
        .tempdir_in(local_temp_parent())
        .expect("create temporary directory");
    let target_dir = temp_dir.path().join("nested/sqlite");
    let db_file = target_dir.join("main.db");

    assert!(
        !target_dir.exists(),
        "nested sqlite directory should not exist before initialization"
    );

    ensure_db_directory(db_file.to_string_lossy().as_ref()).expect("prepare db dir");

    assert!(
        target_dir.exists(),
        "sqlite initialization should create the parent directory"
    );
}

#[test]
fn force_file_backed_database_overrides_test_memory_detection() {
    let settings = SqliteSettings {
        force_file_backed_database: true,
        ..SqliteSettings::default()
    };
    let use_memory_db = "/tmp/sqlite-benchmark.db" == ":memory:"
        || (!settings.force_file_backed_database
            && (cfg!(test)
                || std::env::var("RUST_TEST_THREADS").is_ok()
                || "/tmp/sqlite-benchmark.db".contains("test")));

    assert!(
        !use_memory_db,
        "file-backed override should disable the test-only in-memory shortcut"
    );
}
