use crate::{
    SyncBackendPairDecision, SyncBackendPairReason, plan_sync_backend_pair,
    plan_sync_backend_pair_detailed,
};

const NON_REMOTE_BACKENDS: &[&str] = &["sqlite", "postgres", "turso", "rocksdb", "foundationdb"];

#[test]
fn backend_pair_policy_supports_exact_non_remote_matrix() {
    for source in NON_REMOTE_BACKENDS {
        for destination in NON_REMOTE_BACKENDS {
            let expected = if source == destination {
                SyncBackendPairDecision::ProductionSupported
            } else {
                SyncBackendPairDecision::ValidationOnly
            };
            assert_eq!(
                plan_sync_backend_pair(source, destination),
                expected,
                "{source}->{destination}"
            );
        }
    }
}

#[test]
fn backend_pair_policy_explains_exact_non_remote_matrix() {
    for source in NON_REMOTE_BACKENDS {
        for destination in NON_REMOTE_BACKENDS {
            let plan = plan_sync_backend_pair_detailed(source, destination);
            let expected_reason = if source == destination {
                SyncBackendPairReason::HomogeneousNonRemoteBackend
            } else {
                SyncBackendPairReason::HeterogeneousNonRemoteBackend
            };
            assert_eq!(plan.reason, expected_reason, "{source}->{destination}");
        }
    }
}

#[test]
fn backend_pair_policy_rejects_missing_remote_and_unknown_backends_with_reasons() {
    for (source, destination, reason) in [
        ("", "sqlite", SyncBackendPairReason::EmptyBackendName),
        ("sqlite", "", SyncBackendPairReason::EmptyBackendName),
        ("remote", "sqlite", SyncBackendPairReason::RemoteBackend),
        ("sqlite", "remote", SyncBackendPairReason::RemoteBackend),
        ("remote", "remote", SyncBackendPairReason::RemoteBackend),
        ("mysql", "sqlite", SyncBackendPairReason::UnknownBackend),
        ("sqlite", "mysql", SyncBackendPairReason::UnknownBackend),
    ] {
        let plan = plan_sync_backend_pair_detailed(source, destination);
        assert_eq!(
            plan.decision,
            SyncBackendPairDecision::Rejected,
            "{source}->{destination}"
        );
        assert_eq!(plan.reason, reason, "{source}->{destination}");
    }
}
