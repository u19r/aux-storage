use crate::{
    manager::sync_raft_runtime_adapter::validate_backend_pair, types::SyncLearnerJoinRequest,
};

#[test]
fn learner_join_backend_pair_validation_rejects_unsupported_backend_names() {
    let error = validate_backend_pair(
        Some("sqlite"),
        &SyncLearnerJoinRequest {
            node_id: 2,
            advertise_url: "http://127.0.0.1:9002/storage".to_string(),
            backend_compatibility: Some(String::new()),
        },
    )
    .expect_err("empty backend should fail");

    assert!(error.message.contains("unsupported sync backend pair"));
    assert!(error.message.contains("reason=empty_backend_name"));
}

#[test]
fn learner_join_backend_pair_validation_allows_all_non_remote_mixed_pairs() {
    for (local_backend, remote_backend) in [
        ("postgres", "sqlite"),
        ("sqlite", "postgres"),
        ("turso", "foundationdb"),
        ("foundationdb", "rocksdb"),
    ] {
        validate_backend_pair(
            Some(local_backend),
            &SyncLearnerJoinRequest {
                node_id: 2,
                advertise_url: "http://127.0.0.1:9002/storage".to_string(),
                backend_compatibility: Some(remote_backend.to_string()),
            },
        )
        .expect("non-remote mixed backend pair should be validation-only");
    }
}

#[test]
fn learner_join_backend_pair_validation_rejects_remote_pairs_with_reason() {
    let error = validate_backend_pair(
        Some("postgres"),
        &SyncLearnerJoinRequest {
            node_id: 2,
            advertise_url: "http://127.0.0.1:9002/storage".to_string(),
            backend_compatibility: Some("remote".to_string()),
        },
    )
    .expect_err("remote backend pair should fail");

    assert!(error.message.contains("source=postgres"));
    assert!(error.message.contains("destination=remote"));
    assert!(error.message.contains("reason=remote_backend"));
}

#[test]
fn learner_join_backend_pair_validation_allows_validation_only_mixed_pair() {
    validate_backend_pair(
        Some("sqlite"),
        &SyncLearnerJoinRequest {
            node_id: 2,
            advertise_url: "http://127.0.0.1:9002/storage".to_string(),
            backend_compatibility: Some("rocksdb".to_string()),
        },
    )
    .expect("mixed validation pair");
}
