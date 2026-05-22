//! Ensures storage-related follow-up markers are properly namespaced so they are greppable.
//! This is a lightweight convention test (not a unit test of logic) and can be expanded.

use std::fs;
use std::path::Path;

const FOLLOW_UP_MARKER: &str = concat!("TO", "DO:");
const NAMESPACED_FOLLOW_UP_MARKER: &str = concat!("TO", "DO(storage-");

#[test]
fn storage_follow_up_comments_are_namespaced() {
    let crates = [
        "crates/kv/src",
        "crates/sqlite/src",
    ];

    let mut failures = Vec::new();

    for root in crates { scan_dir(Path::new(root), &mut failures); }

    if !failures.is_empty() {
        panic!("Found un-namespaced storage follow-up markers:\n{}",
            failures.join("\n"));
    }
}

fn scan_dir(path: &Path, failures: &mut Vec<String>) {
    if path.ends_with("target") { return; }
    let Ok(entries) = fs::read_dir(path) else { return; };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() { scan_dir(&path, failures); continue; }
        if let Some(ext) = path.extension() { if ext != "rs" { continue; } } else { continue; }
        let Ok(content) = fs::read_to_string(&path) else { continue; };
        for (idx, line) in content.lines().enumerate() {
            if let Some(pos) = line.find(FOLLOW_UP_MARKER) {
                if line.contains(NAMESPACED_FOLLOW_UP_MARKER) { continue; }
                failures.push(format!("{}:{}: {}", path.display(), idx + 1, &line[pos..]));
            }
        }
    }
}
