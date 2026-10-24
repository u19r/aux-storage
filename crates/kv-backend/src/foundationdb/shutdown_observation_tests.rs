use std::{fs, sync::Arc, time::Instant};

use super::shutdown_observation::{
    ObservationControl, ObservationPhase, ReleaseObservation, RetainedContexts,
};

fn control(directory: &std::path::Path) -> ObservationControl {
    ObservationControl {
        directory: directory.to_path_buf(),
        nonce: uuid::Uuid::new_v4(),
    }
}

#[test]
fn given_retained_contexts_when_owners_leak_or_registry_is_sealed_then_readiness_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let mut registry = RetainedContexts {
        control: control(directory.path()),
        databases: Vec::new(),
        phase: ObservationPhase::Open,
    };
    let database = Arc::new(());
    registry.register_database(&database).unwrap();
    registry.register_database(&database).unwrap();
    assert_eq!(registry.databases.len(), 1);
    registry.phase = ObservationPhase::Sampling;
    assert!(registry.validate_owners().is_err());
    assert!(registry.register_database(&Arc::new(())).is_err());
    drop(database);
    registry.validate_owners().unwrap();
    registry.databases.clear();
    assert!(registry.validate_owners().is_err());
}

#[test]
fn given_full_registry_when_another_context_is_registered_then_capacity_and_original_owners_are_preserved()
 {
    let directory = tempfile::tempdir().unwrap();
    let mut registry = RetainedContexts {
        control: control(directory.path()),
        databases: Vec::new(),
        phase: ObservationPhase::Open,
    };
    for _ in 0..128 {
        registry.register_database(&Arc::new(())).unwrap();
    }
    assert!(registry.register_database(&Arc::new(())).is_err());
    assert_eq!(registry.databases.len(), 128);
    registry.phase = ObservationPhase::Sampling;
    registry.validate_owners().unwrap();
}

#[test]
fn given_release_when_identity_is_wrong_or_deadline_expires_then_release_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let control = control(directory.path());
    control.validate_directory().unwrap();
    let mut release = ReleaseObservation {
        nonce: control.nonce,
        pid: std::process::id(),
    };
    control
        .validate_release(&serde_json::to_vec(&release).unwrap())
        .unwrap();
    release.pid += 1;
    assert!(
        control
            .validate_release(&serde_json::to_vec(&release).unwrap())
            .is_err()
    );
    release.pid = std::process::id();
    release.nonce = uuid::Uuid::new_v4();
    assert!(
        control
            .validate_release(&serde_json::to_vec(&release).unwrap())
            .is_err()
    );
    assert!(control.validate_release(b"{").is_err());
    assert!(control.wait_release(Instant::now()).is_err());
    fs::write(directory.path().join("release.json"), b"{}").unwrap();
    assert!(control.validate_directory().is_err());
}

#[test]
#[ignore = "subprocess fixture; requires owned FDB and a shutdown-observation controller"]
fn given_native_runtime_when_shutdown_is_requested_then_tail_work_is_observed_before_network_stop()
{
    use super::{FoundationDbBackend, FoundationDbOptions, shutdown_foundationdb_network};
    use crate::{AttemptMode, KvAttempt, KvBackend, KvMutation};

    let cluster = std::env::var("FDB_CLUSTER_FILE").expect("owned cluster");
    let started = std::env::var_os("AUXFN_FDB_SHUTDOWN_CHILD_STARTED").expect("started marker");
    let result = std::panic::catch_unwind(|| {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mut signal =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            let first = Arc::new(
                FoundationDbBackend::initialize(FoundationDbOptions {
                    cluster_file: Some(&cluster),
                    ..Default::default()
                })
                .await
                .unwrap(),
            );
            let second = FoundationDbBackend::initialize(FoundationDbOptions {
                cluster_file: Some(&cluster),
                ..Default::default()
            })
            .await
            .unwrap();
            fs::write(started, b"ready\n").unwrap();
            signal.recv().await;
            for index in 0..3 {
                first
                    .with_attempt(AttemptMode::ReadWrite, move |attempt| {
                        Box::pin(async move {
                            attempt.native_stage(vec![KvMutation::PutUnchunked {
                                key: format!("shutdown-tail-{index}").into_bytes(),
                                value: b"owned-test".to_vec(),
                            }])?;
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
            }
            // Leave a real provider transaction suspended; runtime destruction
            // must cancel it before observation readiness can claim exclusive
            // database ownership.
            let background = Arc::clone(&first);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                background
                    .with_attempt(AttemptMode::ReadOnly, move |attempt| {
                        Box::pin(async move {
                            let _ = attempt
                                .native_get(
                                    b"shutdown-tail-0",
                                    crate::ReadProtection::Snapshot,
                                    crate::KvValueAllowance::for_unqualified_values(1),
                                )
                                .await?;
                            let _ = sender.send(());
                            std::future::pending::<()>().await;
                            Ok(())
                        })
                    })
                    .await
                    .unwrap();
            });
            receiver.await.unwrap();
            if std::env::var_os("AUXFN_FDB_SHUTDOWN_CHILD_PANIC").is_some() {
                panic!("intentional shutdown-order regression");
            }
            drop(second);
        });
        drop(runtime);
    });
    shutdown_foundationdb_network().expect("observation and native network drain");
    shutdown_foundationdb_network().expect("idempotent native shutdown");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}
