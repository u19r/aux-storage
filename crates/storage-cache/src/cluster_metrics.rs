//! Metrics emission for distributed cache cluster events.
//!
//! Emits counters for join, leave, reconfigure, election, and migration events
//! and gauges for active node and shard counts. All metrics are registered
//! through `metrics-facade` so they appear on the `/metrics` Prometheus
//! endpoint.

use metrics_facade::{CounterMetric, GaugeMetric};

/// Record a node joining the cache cluster.
pub fn record_cluster_join(node_id: u64) {
    metrics_facade::counter!(
        CounterMetric::CacheClusterJoinTotal,
        "node_id" => node_id.to_string()
    )
    .increment(1);
    tracing::info!(node_id, "cache cluster node joined");
}

/// Record a node leaving the cache cluster.
pub fn record_cluster_leave(node_id: u64) {
    metrics_facade::counter!(
        CounterMetric::CacheClusterLeaveTotal,
        "node_id" => node_id.to_string()
    )
    .increment(1);
    tracing::info!(node_id, "cache cluster node left");
}

/// Record a cluster reconfiguration (membership or ring change).
pub fn record_cluster_reconfigure(reason: &str) {
    metrics_facade::counter!(
        CounterMetric::CacheClusterReconfigureTotal,
        "reason" => reason.to_owned()
    )
    .increment(1);
    tracing::info!(reason, "cache cluster reconfigured");
}

/// Record a leader election completing for a shard.
pub fn record_cluster_election(shard: u8, new_leader: u64) {
    metrics_facade::counter!(
        CounterMetric::CacheClusterElectionTotal,
        "shard" => shard.to_string()
    )
    .increment(1);
    tracing::info!(shard, new_leader, "cache cluster shard election completed");
}

/// Record a shard migration event.
pub fn record_cluster_migration(shard: u8, from_node: u64, to_node: u64) {
    metrics_facade::counter!(
        CounterMetric::CacheClusterMigrationTotal,
        "shard" => shard.to_string()
    )
    .increment(1);
    tracing::info!(
        shard,
        from_node,
        to_node,
        "cache cluster shard migration initiated"
    );
}

/// Update the gauge for the number of active nodes in the cluster.
pub fn set_active_nodes(count: u64) {
    metrics_facade::gauge!(GaugeMetric::CacheClusterActiveNodes).set(count as f64);
}

/// Update the gauge for the number of active (serving) shards in the cluster.
pub fn set_active_shards(count: u64) {
    metrics_facade::gauge!(GaugeMetric::CacheClusterActiveShards).set(count as f64);
}
