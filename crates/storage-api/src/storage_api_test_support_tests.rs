use std::{fs, path::PathBuf};

pub(crate) fn unique_path(label: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let root = workspace_root.join("run-artifacts/storage-api-data");
    fs::create_dir_all(&root).expect("create storage-api test data directory");
    root.join(format!("{label}-{}", uuid::Uuid::new_v4()))
}
