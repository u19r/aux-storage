use storage_types::{
    IndexName, SystemStreamCheckpoint, SystemStreamPosition, SystemStreamScope, TableName,
    maintenance::TargetIncarnation, storage_serde,
};

use super::{
    GsiBuild, GsiBuildPhase, GsiBuildStatus, GsiBuildTarget, GsiLifecycleError,
    GsiProjectionLifecycle,
};

fn target() -> GsiBuildTarget {
    GsiBuildTarget {
        table_name: TableName::new("orders"),
        index_name: IndexName::new("by-status"),
        incarnation: TargetIncarnation::try_from(vec![1]).unwrap(),
    }
}

#[test]
fn given_persisted_build_phases_when_reading_status_then_payloads_are_not_materialized() {
    for (tag, label, phase, writable, queryable) in [
        ("Scanning", "scanning", GsiBuildPhase::Scanning, true, false),
        (
            "CatchingUp",
            "catching-up",
            GsiBuildPhase::CatchingUp,
            true,
            false,
        ),
        ("Active", "active", GsiBuildPhase::Active, true, true),
        ("Blocked", "blocked", GsiBuildPhase::Blocked, false, false),
    ] {
        // Cursor/checkpoint semantics belong to maintenance, not foreground
        // reads.
        let record =
            serde_json::json!({"target": target(), "state": {tag: {"after": "x".repeat(4096)}}});
        let bytes = storage_serde::to_bytes(&record).unwrap();
        let status: GsiBuildStatus = storage_serde::from_bytes(&bytes).unwrap();
        assert!(status.target == target());
        assert_eq!(status.phase, phase);
        assert_eq!(GsiBuildPhase::try_from(label).unwrap(), phase);
        assert_eq!(phase.permits_projection(), writable);
        assert_eq!(phase.permits_queries(), queryable);
        assert!(storage_serde::from_bytes::<GsiBuild<Vec<u8>>>(&bytes).is_err());
    }
}

#[test]
fn given_real_serialized_build_when_reading_status_then_existing_format_is_compatible() {
    let checkpoint = SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("test", 1).unwrap(),
        vec![SystemStreamPosition::try_new("root", "root", "00000000000000000001").unwrap()],
    )
    .unwrap();
    let build = GsiBuild::<Vec<u8>>::new(target(), checkpoint);
    let bytes = storage_serde::to_bytes(&build).unwrap();
    let status: GsiBuildStatus = storage_serde::from_bytes(&bytes).unwrap();
    assert!(status.target == build.target);
    assert_eq!(status.phase, GsiBuildPhase::Scanning);
    let lifecycle = GsiProjectionLifecycle::try_from(vec![status]).unwrap();
    assert!(lifecycle.writable_targets() == [build.target]);
}

#[test]
fn given_invalid_build_status_when_decoding_then_unknown_phase_and_identity_are_rejected() {
    assert!(GsiBuildPhase::try_from("unknown").is_err());
    for record in [
        serde_json::json!({"target": target(), "state": {"Unknown": {}}}),
        serde_json::json!({"target": target(), "state": {"Active": {}, "Scanning": {}}}),
        serde_json::json!({"target": target()}),
        serde_json::json!({"target": {"table_name": "orders", "index_name": "by-status", "incarnation": []}, "state": {"Active": {}}}),
    ] {
        let bytes = storage_serde::to_bytes(&record).unwrap();
        assert!(storage_serde::from_bytes::<GsiBuildStatus>(&bytes).is_err());
    }
    assert!(
        serde_json::from_slice::<GsiBuildStatus>(br#"{"target":{},"state":{"Active":broken}}"#)
            .is_err()
    );
}

#[test]
fn given_duplicate_build_statuses_when_selecting_lifecycle_then_identity_ambiguity_is_rejected() {
    let status = || GsiBuildStatus {
        target: target(),
        phase: GsiBuildPhase::Active,
    };
    assert!(matches!(
        GsiProjectionLifecycle::try_from(vec![status(), status()]),
        Err(GsiLifecycleError::DuplicateIndex)
    ));
    let mut second = status();
    second.target.index_name = IndexName::new("by-customer");
    assert!(matches!(
        GsiProjectionLifecycle::try_from(vec![status(), second]),
        Err(GsiLifecycleError::DuplicateIncarnation)
    ));
}
