#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SyncRaftPeerStatusDecision {
    AuthenticationFailed,
    PeerReturnedError,
}

impl SyncRaftPeerStatusDecision {
    pub(crate) const fn message(self) -> &'static str {
        match self {
            Self::AuthenticationFailed => "sync raft peer authentication failed",
            Self::PeerReturnedError => "sync raft peer returned error",
        }
    }
}

pub(crate) const fn classify_sync_raft_peer_status(status_code: u16) -> SyncRaftPeerStatusDecision {
    match status_code {
        401 | 403 => SyncRaftPeerStatusDecision::AuthenticationFailed,
        _ => SyncRaftPeerStatusDecision::PeerReturnedError,
    }
}
