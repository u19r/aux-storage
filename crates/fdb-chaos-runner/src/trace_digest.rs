//! Digest of one simulation run, for the determinism check.
//!
//! Two runs of one seed must give the same simulated event sequence. The
//! digest covers the time, type, machine and severity of each trace event,
//! and every detail of the events that the Rust workload writes, so that a
//! run that differs only in observed data also changes the digest.
//! It leaves out the attributes that `fdbserver` fills from real time or from
//! its non-deterministic generator (`DateTime`, `RealTime`, `ID`, `RandomId`),
//! and the memory sample events, which follow the real allocator.

use std::{
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest as _, Sha256};

/// Trace events that report the real allocator, not the simulated cluster.
const NON_SIMULATED_EVENT_TYPES: [&str; 4] = [
    "GetMagazineSample",
    "HugeArenaSample",
    "MemSample",
    "MemSampleSummary",
];

/// Attributes that `fdbserver` fills from the real machine.
const REAL_ATTRIBUTES: [&str; 3] = ["DateTime", "ThreadID", "Backtrace"];

pub(crate) const TRACE_DIGEST_FILE: &str = "trace-digest.txt";

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TraceDigest {
    pub(crate) sha256: String,
    pub(crate) events: u64,
    pub(crate) unseed: Option<String>,
}

impl TraceDigest {
    pub(crate) fn line(&self) -> String {
        format!(
            "fdb chaos trace digest: {} events={} unseed={}",
            self.sha256,
            self.events,
            self.unseed.as_deref().unwrap_or("none")
        )
    }
}

/// Digest the copied traces of `artifact_dir` and write `trace-digest.txt`.
pub(crate) fn write_trace_digest(artifact_dir: &Path) -> Result<TraceDigest, String> {
    let mut hasher = Sha256::new();
    let mut events = 0_u64;
    for path in ordered_trace_files(&artifact_dir.join("traces"))? {
        let raw = fs::read_to_string(&path)
            .map_err(|err| format!("failed to read {}: {err}", path.display()))?;
        for key in raw.lines().filter_map(simulated_event_key) {
            hasher.update(key.as_bytes());
            hasher.update(b"\n");
            events += 1;
        }
    }
    let output = fs::read_to_string(artifact_dir.join("fdbserver-output.log")).unwrap_or_default();
    let digest = TraceDigest {
        sha256: hex::encode(hasher.finalize()),
        events,
        unseed: output
            .lines()
            .find_map(|line| line.strip_prefix("Unseed: "))
            .map(|value| value.trim().to_string()),
    };
    fs::write(
        artifact_dir.join(TRACE_DIGEST_FILE),
        format!("{}\n", digest.line()),
    )
    .map_err(|err| format!("failed to write trace digest: {err}"))?;
    Ok(digest)
}

/// Rolled trace files carry their sequence number before `.xml`.
fn ordered_trace_files(directory: &Path) -> Result<Vec<PathBuf>, String> {
    let Ok(entries) = fs::read_dir(directory) else {
        return Ok(Vec::new());
    };
    let mut paths = Vec::new();
    for entry in entries {
        let path = entry
            .map_err(|err| format!("failed to inspect trace file: {err}"))?
            .path();
        if path.extension().is_some_and(|extension| extension == "xml") {
            paths.push(path);
        }
    }
    paths.sort_by_key(|path| {
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default()
            .to_string();
        let sequence = name
            .trim_end_matches(".xml")
            .rsplit('.')
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        (sequence, name)
    });
    Ok(paths)
}

pub(crate) fn simulated_event_key(line: &str) -> Option<String> {
    let line = line.trim_start();
    if !line.starts_with("<Event ") {
        return None;
    }
    // Native trace rollover replays cached events with a new time and order.
    // Their original records remain in the digest.
    if attribute(line, "TrackLatestType") == Some("Rolled") {
        return None;
    }
    let event_type = attribute(line, "Type")?;
    if NON_SIMULATED_EVENT_TYPES.contains(&event_type) {
        return None;
    }
    let mut key = format!(
        "{} {} {} {}",
        attribute(line, "Time").unwrap_or_default(),
        event_type,
        attribute(line, "Machine").unwrap_or_default(),
        attribute(line, "Severity").unwrap_or_default(),
    );
    if attribute(line, "RustWorkload") == Some("1") {
        for (name, value) in attributes(line) {
            if !REAL_ATTRIBUTES.contains(&name) {
                key.push_str(&format!(" {name}={value}"));
            }
        }
    }
    Some(key)
}

fn attributes(line: &str) -> impl Iterator<Item = (&str, &str)> {
    line.split("\" ").filter_map(|part| {
        let (name, value) = part.trim_start_matches("<Event ").split_once("=\"")?;
        Some((name.trim(), value))
    })
}

fn attribute<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let marker = format!(" {name}=\"");
    let rest = line.get(line.find(&marker)? + marker.len()..)?;
    rest.split_once('"').map(|(value, _)| value)
}
