use config::{StorageSyncReplicationConfig, StorageSyncReplicationPeerConfig};
use storage_provider::{SqliteSettings, StorageBackend, StorageConfig};

use crate::sync_replication_startup::{
    SyncRaftStartupDecision, plan_sync_raft_startup, sync_raft_test_learner_join_peer_node_id,
    sync_raft_test_members, sync_raft_test_openraft_config,
    sync_raft_test_proposal_coalescing_window,
};

#[test]
fn sync_startup_members_include_self_and_configured_peers() {
    let members = sync_raft_test_members(&sync_config()).expect("members");

    assert_eq!(
        members.get(&1).map(|node| node.addr.as_str()),
        Some("http://127.0.0.1:9001/storage")
    );
    assert_eq!(
        members.get(&2).map(|node| node.addr.as_str()),
        Some("http://127.0.0.1:9002/storage")
    );
}

#[test]
fn sync_startup_openraft_config_uses_requested_timing_and_disables_snapshots() {
    let config = sync_raft_test_openraft_config(&sync_config()).expect("openraft config");

    assert_eq!(config.election_timeout_min, 300);
    assert_eq!(config.election_timeout_max, 600);
    assert_eq!(config.heartbeat_interval, 50);
    assert!(matches!(
        config.snapshot_policy,
        openraft::SnapshotPolicy::Never
    ));
}

#[test]
fn sync_startup_uses_configured_proposal_coalescing_window() {
    let sync = StorageSyncReplicationConfig {
        proposal_coalescing_window_us: 0,
        ..sync_config()
    };

    assert_eq!(
        sync_raft_test_proposal_coalescing_window(&sync),
        std::time::Duration::ZERO
    );
}

#[cfg(feature = "sqlite")]
#[test]
fn sync_startup_data_dir_uses_node_scoped_raft_database() {
    use crate::sync_replication_startup::sync_raft_test_sqlite_path;

    let sync = StorageSyncReplicationConfig {
        data_dir: Some("run-artifacts/storage-api-data/aux-sync".to_string()),
        ..sync_config()
    };
    let path = sync_raft_test_sqlite_path(&sync, &storage_config(), 7).expect("path");

    assert!(path.ends_with("run-artifacts/storage-api-data/aux-sync/sync-raft-node-7.db"));
}

#[cfg(feature = "sqlite")]
#[test]
fn sync_startup_allows_non_sql_storage_with_separate_raft_data_dir() {
    use crate::sync_replication_startup::sync_raft_test_sqlite_path;

    let sync = StorageSyncReplicationConfig {
        data_dir: Some("run-artifacts/storage-api-data/aux-sync-rocks".to_string()),
        ..sync_config()
    };
    let storage = StorageConfig {
        backend_type: StorageBackend::RocksDB,
        connection_string: Some("run-artifacts/storage-api-data/aux-rocksdb".to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    assert_eq!(
        plan_sync_raft_startup(&storage, &sync),
        SyncRaftStartupDecision::Allow
    );
    let path = sync_raft_test_sqlite_path(&sync, &storage, 7).expect("path");
    assert!(path.ends_with("run-artifacts/storage-api-data/aux-sync-rocks/sync-raft-node-7.db"));
}

#[cfg(feature = "sqlite")]
#[test]
fn sync_startup_rejects_non_sql_storage_without_separate_raft_data_dir() {
    let storage = StorageConfig {
        backend_type: StorageBackend::RocksDB,
        connection_string: Some("run-artifacts/storage-api-data/aux-rocksdb".to_string()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    };

    assert_eq!(
        plan_sync_raft_startup(&storage, &sync_config()),
        SyncRaftStartupDecision::SyncDataDirRequired
    );
}

#[test]
fn sync_startup_learner_join_uses_explicit_peer_or_first_peer() {
    let mut sync = sync_config();
    sync.join_as_learner = true;
    sync.learner_join_peer_node_id = Some(2);

    assert_eq!(
        sync_raft_test_learner_join_peer_node_id(&sync).expect("explicit join peer"),
        2
    );

    sync.learner_join_peer_node_id = None;
    assert_eq!(
        sync_raft_test_learner_join_peer_node_id(&sync).expect("default join peer"),
        2
    );
}

#[test]
fn sync_startup_replacement_node_config_joins_as_empty_learner() {
    let sync = StorageSyncReplicationConfig {
        node_id: Some(4),
        advertise_url: Some("http://127.0.0.1:9004/storage".to_string()),
        join_as_learner: true,
        learner_join_peer_node_id: Some(1),
        peers: vec![
            StorageSyncReplicationPeerConfig {
                node_id: 1,
                endpoint_url: "http://127.0.0.1:9001/storage".to_string(),
            },
            StorageSyncReplicationPeerConfig {
                node_id: 2,
                endpoint_url: "http://127.0.0.1:9002/storage".to_string(),
            },
        ],
        ..sync_config()
    };

    assert!(sync.join_as_learner);
    assert!(sync.peers.iter().all(|peer| peer.node_id != 4));
    assert_eq!(
        sync_raft_test_learner_join_peer_node_id(&sync).expect("replacement join peer"),
        1
    );
}

fn sync_config() -> StorageSyncReplicationConfig {
    StorageSyncReplicationConfig {
        enabled: true,
        node_id: Some(1),
        advertise_url: Some("http://127.0.0.1:9001/storage".to_string()),
        data_dir: None,
        sync_internal_token: Some("sync-secret".to_string()),
        preferred_leader_node_id: Some(1),
        join_as_learner: false,
        learner_join_peer_node_id: None,
        peers: vec![StorageSyncReplicationPeerConfig {
            node_id: 2,
            endpoint_url: "http://127.0.0.1:9002/storage".to_string(),
        }],
        election_timeout_ms: 300,
        heartbeat_interval_ms: 50,
        proposal_coalescing_window_us: 500,
    }
}

#[cfg(feature = "sqlite")]
fn storage_config() -> StorageConfig {
    StorageConfig {
        backend_type: StorageBackend::SQLite,
        connection_string: Some("storage.db".to_string()),
        file_path: None,
        sqlite: Some(SqliteSettings::default()),
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}
