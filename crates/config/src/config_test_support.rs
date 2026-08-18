use std::{fs, path::PathBuf};

use tempfile::{Builder, NamedTempFile, TempDir};

fn test_data_dir() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let path = workspace_root.join("run-artifacts/config-data");
    fs::create_dir_all(&path).expect("create config test data directory");
    path
}

pub(crate) fn temp_dir(prefix: &str) -> TempDir {
    Builder::new()
        .prefix(prefix)
        .tempdir_in(test_data_dir())
        .expect("create config test directory")
}

pub(crate) fn write_config(value: serde_json::Value) -> NamedTempFile {
    let file = Builder::new()
        .prefix("config-")
        .tempfile_in(test_data_dir())
        .expect("create temporary config file");
    fs::write(
        file.path(),
        serde_json::to_vec_pretty(&value).expect("serialize config"),
    )
    .expect("write config");
    file
}
