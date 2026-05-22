use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    constants::{
        DEFAULT_STORAGE_SYNC_REPLICATION_ELECTION_TIMEOUT_MS,
        DEFAULT_STORAGE_SYNC_REPLICATION_HEARTBEAT_INTERVAL_MS,
        DEFAULT_STORAGE_SYNC_REPLICATION_PROPOSAL_COALESCING_WINDOW_US,
    },
    error::ConfigError,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageSyncReplicationConfig {
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default)]
    #[schemars(default)]
    pub node_id: Option<u64>,
    #[serde(default)]
    #[schemars(default)]
    pub advertise_url: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub data_dir: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub sync_internal_token: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub preferred_leader_node_id: Option<u64>,
    #[serde(default)]
    #[schemars(default)]
    pub join_as_learner: bool,
    #[serde(default)]
    #[schemars(default)]
    pub learner_join_peer_node_id: Option<u64>,
    #[serde(default)]
    #[schemars(default)]
    pub peers: Vec<StorageSyncReplicationPeerConfig>,
    #[serde(default = "default_storage_sync_replication_election_timeout_ms")]
    #[schemars(default = "default_storage_sync_replication_election_timeout_ms")]
    pub election_timeout_ms: u64,
    #[serde(default = "default_storage_sync_replication_heartbeat_interval_ms")]
    #[schemars(default = "default_storage_sync_replication_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
    #[serde(default = "default_storage_sync_replication_proposal_coalescing_window_us")]
    #[schemars(default = "default_storage_sync_replication_proposal_coalescing_window_us")]
    pub proposal_coalescing_window_us: u64,
}

impl Default for StorageSyncReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: None,
            advertise_url: None,
            data_dir: None,
            sync_internal_token: None,
            preferred_leader_node_id: None,
            join_as_learner: false,
            learner_join_peer_node_id: None,
            peers: Vec::new(),
            election_timeout_ms: default_storage_sync_replication_election_timeout_ms(),
            heartbeat_interval_ms: default_storage_sync_replication_heartbeat_interval_ms(),
            proposal_coalescing_window_us:
                default_storage_sync_replication_proposal_coalescing_window_us(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct StorageSyncReplicationPeerConfig {
    pub node_id: u64,
    pub endpoint_url: String,
}

pub(crate) fn validate_storage_sync_replication(
    sync: &StorageSyncReplicationConfig,
) -> Result<(), ConfigError> {
    if !sync.enabled {
        return Ok(());
    }
    let Some(node_id) = sync.node_id else {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.node_id is required when enabled",
        ));
    };
    if node_id == 0 {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.node_id must be greater than 0",
        ));
    }
    if sync
        .advertise_url
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.advertise_url is required when enabled",
        ));
    }
    if sync
        .sync_internal_token
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.sync_internal_token is required when enabled",
        ));
    }
    if sync.election_timeout_ms == 0 {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.election_timeout_ms must be greater than 0",
        ));
    }
    if sync.heartbeat_interval_ms == 0 {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.heartbeat_interval_ms must be greater than 0",
        ));
    }
    if sync.heartbeat_interval_ms >= sync.election_timeout_ms {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.heartbeat_interval_ms must be less than \
             election_timeout_ms",
        ));
    }
    let mut peer_ids = BTreeSet::new();
    for peer in &sync.peers {
        validate_peer(peer, node_id, &mut peer_ids)?;
    }
    if sync.join_as_learner {
        if sync.peers.is_empty() {
            return Err(ConfigError::validation(
                "features.storage_sync_replication.peers must contain at least one bootstrap peer \
                 when join_as_learner is true",
            ));
        }
        if let Some(join_peer) = sync.learner_join_peer_node_id
            && !peer_ids.contains(&join_peer)
        {
            return Err(ConfigError::validation(
                "features.storage_sync_replication.learner_join_peer_node_id must reference a \
                 configured peer",
            ));
        }
    }
    Ok(())
}

fn validate_peer(
    peer: &StorageSyncReplicationPeerConfig,
    self_node_id: u64,
    peer_ids: &mut BTreeSet<u64>,
) -> Result<(), ConfigError> {
    if peer.node_id == 0 {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.peers[].node_id must be greater than 0",
        ));
    }
    if peer.node_id == self_node_id {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.peers must not include this node_id",
        ));
    }
    if peer.endpoint_url.trim().is_empty() {
        return Err(ConfigError::validation(
            "features.storage_sync_replication.peers[].endpoint_url must not be empty",
        ));
    }
    if !peer_ids.insert(peer.node_id) {
        return Err(ConfigError::validation(format!(
            "features.storage_sync_replication.peers contains duplicate node_id {}",
            peer.node_id
        )));
    }
    Ok(())
}

fn default_storage_sync_replication_election_timeout_ms() -> u64 {
    DEFAULT_STORAGE_SYNC_REPLICATION_ELECTION_TIMEOUT_MS
}

fn default_storage_sync_replication_heartbeat_interval_ms() -> u64 {
    DEFAULT_STORAGE_SYNC_REPLICATION_HEARTBEAT_INTERVAL_MS
}

fn default_storage_sync_replication_proposal_coalescing_window_us() -> u64 {
    DEFAULT_STORAGE_SYNC_REPLICATION_PROPOSAL_COALESCING_WINDOW_US
}
