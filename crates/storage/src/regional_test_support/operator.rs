//! Operator files shared by public native and HTTP process fixtures.
use std::{net::SocketAddr, path::Path};

use anyhow::Result;
use serde_json::json;

pub fn features(
    root: &Path,
    index: usize,
    endpoints: [SocketAddr; 2],
    declaration_bytes: usize,
    backends: config::Backends,
) -> Result<config::Features> {
    let peer = 3 - index;
    let startup = json!({
        "configuration": {
            "deployment": vec![8; 16], "tenant": vec![7; 16], "region": format!("region-{index}"),
            "group": vec![index as u8; 16], "replica": vec![index as u8; 16],
            "journal_partitions": 64, "journal_generation": 1, "endpoint": endpoints[index - 1],
            "peers": [{"region": format!("region-{peer}"), "group": vec![peer as u8; 16],
                "replica": vec![peer as u8; 16], "generation": 1, "endpoint": endpoints[peer - 1]}],
            "limits": {"frame_bytes": 1048576, "frame_events": 128, "import_bytes": 1048576,
                "import_events": 32, "apply_window": 2, "apply_concurrency": 2, "queue_bytes": 536870912},
            "backlog": {"hot_history_bytes": 268435456, "acceptance_bytes": 201326592,
                "recovery_reserve_bytes": 67108864, "transfer_bytes_per_second": 1048576, "concurrent_joins": 1},
            "timing": {"retry_min_ms": 5, "retry_max_ms": 100, "clock_alert_ms": 1000, "request_timeout_ms": 2000}
        },
        "budgets": {"attempt": {"data_bytes": 67108864, "call_guards": 100000}, "reserved_attempts": 2,
            "maximum_canonical_event_bytes": 65536, "maximum_original_position_bytes": 1048576,
            "maximum_acceptance_receipt_bytes": 4096, "maximum_origin_receipt_bytes": 65536,
            "maximum_history_declaration_bytes": declaration_bytes},
        "physical_limits": {"key_bytes": 1024, "record_bytes": 1048576, "bytes": 8388608, "records": 128}
    });
    let file = root.join(format!("region-{index}.json"));
    std::fs::write(&file, serde_json::to_vec(&startup)?)?;
    let mut features = config::Features {
        backends,
        ..Default::default()
    };
    features.regional_connections.insert("default".into(), file);
    Ok(features)
}
