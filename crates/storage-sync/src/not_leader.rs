use serde::{Deserialize, Serialize};

pub const SYNC_LEADER_HINT_HEADER: &str = "x-aux-storage-leader";
pub const SYNC_NOT_LEADER_ERROR_TYPE: &str = "NotLeaderException";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncNotLeader {
    pub leader_hint: Option<String>,
}

impl SyncNotLeader {
    #[must_use]
    pub fn new(leader_hint: Option<String>) -> Self {
        Self { leader_hint }
    }

    #[must_use]
    pub fn message(&self) -> String {
        match self.leader_hint.as_deref() {
            Some(leader) => {
                format!("storage sync node is not the current leader; retry against {leader}")
            }
            None => "storage sync node is not the current leader".to_string(),
        }
    }
}
