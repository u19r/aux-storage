use std::{fs, path::PathBuf, time::SystemTime};

pub(crate) fn unique_path(label: &str) -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    let root = workspace_root.join("run-artifacts/storage-data");
    fs::create_dir_all(&root).expect("create storage test data directory");
    let unique = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos();
    root.join(format!("{label}-{}-{unique}", std::process::id()))
}
