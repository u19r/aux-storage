use crate::constants::{
    STORAGE_MULTI_REGION_APPLY_TOTAL_METRIC, STORAGE_MULTI_REGION_AUTH_FAILURE_TOTAL_METRIC,
    STORAGE_MULTI_REGION_HEARTBEAT_RTT_MS_METRIC,
    STORAGE_MULTI_REGION_HEARTBEAT_STALENESS_MS_METRIC,
    STORAGE_MULTI_REGION_REPLICATION_LAG_MS_METRIC, STORAGE_MULTI_REGION_SENDER_QUEUE_DEPTH_METRIC,
};

pub fn record_multi_region_replication_lag(peer_region: &str, lag_ms: u64) {
    metrics_facade::gauge!(
        STORAGE_MULTI_REGION_REPLICATION_LAG_MS_METRIC,
        "peer_region" => peer_region.to_string()
    )
    .set(lag_ms as f64);
}

pub fn record_multi_region_heartbeat_rtt(peer_region: &str, rtt_ms: u64) {
    metrics_facade::histogram!(
        STORAGE_MULTI_REGION_HEARTBEAT_RTT_MS_METRIC,
        "peer_region" => peer_region.to_string()
    )
    .record(rtt_ms as f64);
}

pub fn record_multi_region_heartbeat_staleness(peer_region: &str, staleness_ms: u64) {
    metrics_facade::gauge!(
        STORAGE_MULTI_REGION_HEARTBEAT_STALENESS_MS_METRIC,
        "peer_region" => peer_region.to_string()
    )
    .set(staleness_ms as f64);
}

pub fn record_multi_region_sender_queue_depth(peer_region: &str, depth: u64) {
    metrics_facade::gauge!(
        STORAGE_MULTI_REGION_SENDER_QUEUE_DEPTH_METRIC,
        "peer_region" => peer_region.to_string()
    )
    .set(depth as f64);
}

pub fn increment_multi_region_apply_total(peer_region: &str, outcome: &'static str, value: u64) {
    metrics_facade::counter!(
        STORAGE_MULTI_REGION_APPLY_TOTAL_METRIC,
        "peer_region" => peer_region.to_string(),
        "outcome" => outcome
    )
    .increment(value);
}

pub fn increment_multi_region_auth_failure_total(peer_region: &str) {
    metrics_facade::counter!(
        STORAGE_MULTI_REGION_AUTH_FAILURE_TOTAL_METRIC,
        "peer_region" => peer_region.to_string()
    )
    .increment(1);
}
