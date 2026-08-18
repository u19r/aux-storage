use serde_json::json;

use crate::{ConfigError, StorageApiLaunchConfig, config_test_support::write_config, load};

#[test]
fn storage_sync_replication_defaults_disabled() {
    let loaded = load(write_config(json!({})).path()).expect("load defaults");

    assert!(!loaded.root.features.storage_sync_replication.enabled);
    assert_eq!(
        loaded
            .root
            .features
            .storage_sync_replication
            .election_timeout_ms,
        300
    );
    assert_eq!(
        loaded
            .root
            .features
            .storage_sync_replication
            .proposal_coalescing_window_us,
        500
    );
}

#[test]
fn storage_sync_replication_requires_internal_token_when_enabled() {
    let file = write_config(json!({
        "features": {
            "storage_sync_replication": {
                "enabled": true,
                "node_id": 1,
                "advertise_url": "http://127.0.0.1:9101/storage",
                "peers": [{
                    "node_id": 2,
                    "endpoint_url": "http://127.0.0.1:9102/storage"
                }]
            }
        }
    }));

    let error = load(file.path()).expect_err("missing sync token should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("features.storage_sync_replication.sync_internal_token")));
}

#[test]
fn storage_sync_replication_loads_enabled_peer_config() {
    let file = write_config(json!({
        "features": {
            "storage_sync_replication": {
                "enabled": true,
                "node_id": 1,
                "advertise_url": "http://127.0.0.1:9101/storage",
                "sync_internal_token": "sync-secret",
                "preferred_leader_node_id": 1,
                "election_timeout_ms": 250,
                "heartbeat_interval_ms": 25,
                "proposal_coalescing_window_us": 0,
                "peers": [{
                    "node_id": 2,
                    "endpoint_url": "http://127.0.0.1:9102/storage"
                }]
            }
        }
    }));

    let loaded = load(file.path()).expect("load sync config");
    let sync = &loaded.root.features.storage_sync_replication;

    assert!(sync.enabled);
    assert_eq!(sync.node_id, Some(1));
    assert_eq!(sync.sync_internal_token.as_deref(), Some("sync-secret"));
    assert!(!sync.join_as_learner);
    assert_eq!(sync.proposal_coalescing_window_us, 0);
    assert_eq!(sync.peers[0].node_id, 2);
}

#[test]
fn storage_sync_replication_validates_learner_join_peer() {
    let file = write_config(json!({
        "features": {
            "storage_sync_replication": {
                "enabled": true,
                "node_id": 3,
                "advertise_url": "http://127.0.0.1:9103/storage",
                "sync_internal_token": "sync-secret",
                "join_as_learner": true,
                "learner_join_peer_node_id": 1,
                "peers": [{
                    "node_id": 1,
                    "endpoint_url": "http://127.0.0.1:9101/storage"
                }]
            }
        }
    }));

    let loaded = load(file.path()).expect("load learner join config");
    let sync = &loaded.root.features.storage_sync_replication;

    assert!(sync.join_as_learner);
    assert_eq!(sync.learner_join_peer_node_id, Some(1));
}

#[test]
fn storage_sync_replication_rejects_learner_join_without_bootstrap_peer() {
    let file = write_config(json!({
        "features": {
            "storage_sync_replication": {
                "enabled": true,
                "node_id": 3,
                "advertise_url": "http://127.0.0.1:9103/storage",
                "sync_internal_token": "sync-secret",
                "join_as_learner": true
            }
        }
    }));

    let error = load(file.path()).expect_err("missing bootstrap peer should fail");

    assert!(matches!(error, ConfigError::Validation { message }
        if message.contains("peers must contain at least one bootstrap peer")));
}

#[test]
fn storage_api_launch_config_exposes_sync_replication_settings() {
    let file = write_config(json!({
        "features": {
            "storage_sync_replication": {
                "enabled": true,
                "node_id": 1,
                "advertise_url": "http://127.0.0.1:9101/storage",
                "sync_internal_token": "sync-secret",
                "peers": [{
                    "node_id": 2,
                    "endpoint_url": "http://127.0.0.1:9102/storage"
                }]
            }
        }
    }));

    let launch = StorageApiLaunchConfig::from_args([
        "storage-api".into(),
        "--config".into(),
        file.path().to_string_lossy().into_owned(),
    ])
    .expect("launch config");

    assert!(launch.effective.storage_sync_replication.enabled);
    assert_eq!(launch.effective.storage_sync_replication.node_id, Some(1));
    assert_eq!(
        launch
            .effective
            .storage_sync_replication
            .sync_internal_token
            .as_deref(),
        Some("sync-secret")
    );
}
