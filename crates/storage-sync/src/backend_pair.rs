#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncBackendPairDecision {
    ProductionSupported,
    ValidationOnly,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncBackendPairReason {
    HomogeneousNonRemoteBackend,
    HeterogeneousNonRemoteBackend,
    EmptyBackendName,
    RemoteBackend,
    UnknownBackend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyncBackendPairPlan {
    pub decision: SyncBackendPairDecision,
    pub reason: SyncBackendPairReason,
}

#[must_use]
pub fn plan_sync_backend_pair(
    source_backend: &str,
    destination_backend: &str,
) -> SyncBackendPairDecision {
    plan_sync_backend_pair_detailed(source_backend, destination_backend).decision
}

#[must_use]
pub fn plan_sync_backend_pair_detailed(
    source_backend: &str,
    destination_backend: &str,
) -> SyncBackendPairPlan {
    let source = source_backend.trim();
    let destination = destination_backend.trim();
    if source.is_empty() || destination.is_empty() {
        return SyncBackendPairPlan::rejected(SyncBackendPairReason::EmptyBackendName);
    }
    if source == "remote" || destination == "remote" {
        return SyncBackendPairPlan::rejected(SyncBackendPairReason::RemoteBackend);
    }
    if !is_non_remote_backend(source) || !is_non_remote_backend(destination) {
        return SyncBackendPairPlan::rejected(SyncBackendPairReason::UnknownBackend);
    }
    if source == destination {
        SyncBackendPairPlan {
            decision: SyncBackendPairDecision::ProductionSupported,
            reason: SyncBackendPairReason::HomogeneousNonRemoteBackend,
        }
    } else {
        SyncBackendPairPlan {
            decision: SyncBackendPairDecision::ValidationOnly,
            reason: SyncBackendPairReason::HeterogeneousNonRemoteBackend,
        }
    }
}

impl SyncBackendPairPlan {
    fn rejected(reason: SyncBackendPairReason) -> Self {
        Self {
            decision: SyncBackendPairDecision::Rejected,
            reason,
        }
    }
}

impl SyncBackendPairReason {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HomogeneousNonRemoteBackend => "homogeneous_non_remote_backend",
            Self::HeterogeneousNonRemoteBackend => "heterogeneous_non_remote_backend",
            Self::EmptyBackendName => "empty_backend_name",
            Self::RemoteBackend => "remote_backend",
            Self::UnknownBackend => "unknown_backend",
        }
    }
}

fn is_non_remote_backend(backend: &str) -> bool {
    matches!(
        backend,
        "sqlite" | "postgres" | "turso" | "rocksdb" | "foundationdb"
    )
}
