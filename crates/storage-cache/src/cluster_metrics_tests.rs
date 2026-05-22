use crate::cluster_metrics::{
    record_cluster_election, record_cluster_join, record_cluster_leave, record_cluster_migration,
    record_cluster_reconfigure, set_active_nodes, set_active_shards,
};

#[test]
fn record_join_does_not_panic() {
    record_cluster_join(0);
}

#[test]
fn record_leave_does_not_panic() {
    record_cluster_leave(1);
}

#[test]
fn record_reconfigure_does_not_panic() {
    record_cluster_reconfigure("ring_update");
}

#[test]
fn record_election_does_not_panic() {
    record_cluster_election(0, 2);
}

#[test]
fn record_migration_does_not_panic() {
    record_cluster_migration(1, 0, 2);
}

#[test]
fn set_active_nodes_does_not_panic() {
    set_active_nodes(3);
}

#[test]
fn set_active_shards_does_not_panic() {
    set_active_shards(3);
}
