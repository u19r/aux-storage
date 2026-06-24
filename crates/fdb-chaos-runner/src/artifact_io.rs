use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Serialize;

pub(crate) fn write_json<T>(path: &Path, value: &T) -> Result<(), String>
where T: Serialize {
    let json = serde_json::to_string_pretty(value)
        .map_err(|err| format!("failed to serialize metadata: {err}"))?;
    fs::write(path, format!("{json}\n"))
        .map_err(|err| format!("failed to write {}: {err}", path.display()))
}
pub(crate) fn client_artifact_dirs(artifact_dir: &Path) -> Result<Vec<PathBuf>, String> {
    let entries = fs::read_dir(artifact_dir)
        .map_err(|err| format!("failed to list {}: {err}", artifact_dir.display()))?;
    let mut dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| {
            format!(
                "failed to inspect artifact entry in {}: {err}",
                artifact_dir.display()
            )
        })?;
        let path = entry.path();
        if path.is_dir()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("client-"))
        {
            dirs.push(path);
        }
    }
    dirs.sort();
    Ok(dirs)
}
