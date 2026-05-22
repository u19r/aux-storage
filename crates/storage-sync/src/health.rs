use serde::{Deserialize, Serialize};

use crate::SyncNodeId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncRaftRole {
    Disabled,
    Learner,
    Follower,
    Candidate,
    Leader,
}

impl SyncRaftRole {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Learner => "learner",
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncPeerHealth {
    pub node_id: SyncNodeId,
    pub match_index: Option<u64>,
    pub lag_entries: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncHealthResponse {
    pub local_node_id: Option<SyncNodeId>,
    pub role: SyncRaftRole,
    pub known_leader: Option<SyncNodeId>,
    pub term: Option<u64>,
    pub commit_index: Option<u64>,
    pub applied_index: Option<u64>,
    pub voters: Vec<SyncNodeId>,
    pub learners: Vec<SyncNodeId>,
    pub peers: Vec<SyncPeerHealth>,
    pub preferred_leader: bool,
    pub leader_hint: Option<String>,
    pub logical_catchup_status: Option<String>,
    pub backend_compatibility: Option<String>,
}

impl SyncHealthResponse {
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            local_node_id: None,
            role: SyncRaftRole::Disabled,
            known_leader: None,
            term: None,
            commit_index: None,
            applied_index: None,
            voters: Vec::new(),
            learners: Vec::new(),
            peers: Vec::new(),
            preferred_leader: false,
            leader_hint: None,
            logical_catchup_status: None,
            backend_compatibility: None,
        }
    }
}
