//! Native operation-deadline enforcement on an owned FoundationDB fixture.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use foundationdb::{Database, options::TransactionOption};
use storage_types::{
    AttemptDeadline, AttemptMode, AttemptPhase, AttemptPlan, DeadlineClass, StorageError,
};

use super::driver::{FoundationDbBackend, FoundationDbOptions};
use crate::{KvAttempt as _, KvBackend as _, KvMutation};

async fn open_backend() -> FoundationDbBackend {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        ..Default::default()
    })
    .await
    .unwrap()
}

fn unique_key(label: &str) -> Vec<u8> {
    format!(
        "fdb-deadline/{}/{}/{label}",
        std::process::id(),
        uuid::Uuid::new_v4()
    )
    .into_bytes()
}

fn physical_key(backend: &FoundationDbBackend, logical: &[u8]) -> Vec<u8> {
    let mut key = backend.prefix.to_vec();
    key.extend_from_slice(logical);
    key
}

async fn read_key(database: &Database, key: &[u8]) -> Option<Vec<u8>> {
    let transaction = database.create_trx().unwrap();
    transaction
        .set_option(TransactionOption::Timeout(10_000))
        .unwrap();
    transaction
        .get(key, false)
        .await
        .unwrap()
        .map(|value| value.to_vec())
}

fn deadline_after(budget: Duration, commit_grace: Duration) -> AttemptDeadline {
    AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() + budget,
        commit_grace,
    }
}

fn is_deadline(error: &StorageError, phase: AttemptPhase) -> bool {
    error.deadline_phase() == Some(phase)
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_expired_request_when_attempt_starts_then_no_callback_and_no_durable_write() {
    let backend = open_backend().await;
    let logical_key = unique_key("expired-request");
    let physical_key = physical_key(&backend, &logical_key);
    let callback_ran = Arc::new(AtomicBool::new(false));
    let observed_callback = Arc::clone(&callback_ran);
    let deadline = AttemptDeadline {
        class: DeadlineClass::Write,
        expires_at: tokio::time::Instant::now() - Duration::from_secs(1),
        commit_grace: Duration::from_millis(10),
    };
    let error = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            move |attempt| {
                Box::pin(async move {
                    observed_callback.store(true, Ordering::SeqCst);
                    attempt.stage(vec![KvMutation::PutUnchunked {
                        key: logical_key,
                        value: b"committed".to_vec(),
                    }])
                })
            },
        )
        .await
        .unwrap_err();
    assert!(is_deadline(&error, AttemptPhase::Acquire), "{error:?}");
    assert!(!callback_ran.load(Ordering::SeqCst));
    assert!(read_key(&backend.database, &physical_key).await.is_none());
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_staged_write_and_pending_native_read_when_deadline_expires_then_known_no_effects() {
    let backend = open_backend().await;
    let logical_key = unique_key("expires-during-callback");
    let physical_key = physical_key(&backend, &logical_key);
    // A far-future read version holds the callback's native read open.
    let version = backend
        .database
        .create_trx()
        .unwrap()
        .get_read_version()
        .await
        .unwrap();
    backend.pin_next_read_version(version + 1_000_000_000_000);
    let reads_started = Arc::new(AtomicUsize::new(0));
    let observed_reads = Arc::clone(&reads_started);
    let deadline = deadline_after(Duration::from_millis(300), Duration::from_secs(2));
    let error = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            move |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::PutUnchunked {
                        key: logical_key.clone(),
                        value: b"staged".to_vec(),
                    }])?;
                    observed_reads.fetch_add(1, Ordering::SeqCst);
                    // A staged key reads locally; an unstaged key waits for the
                    // server.
                    attempt
                        .get(
                            &unique_key("unstaged-read"),
                            crate::ReadProtection::Conflict,
                        )
                        .await?;
                    Ok(())
                })
            },
        )
        .await
        .unwrap_err();
    assert!(is_deadline(&error, AttemptPhase::Statement), "{error:?}");
    assert_eq!(reads_started.load(Ordering::SeqCst), 1);
    assert!(read_key(&backend.database, &physical_key).await.is_none());
}

#[cfg(feature = "fault-injection")]
struct HoldBeforeCommit(Duration);

#[cfg(feature = "fault-injection")]
impl crate::KvFaultProbe for HoldBeforeCommit {
    fn fail(&self, _call: crate::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        let hold = self.0;
        Box::pin(async move {
            if boundary == crate::KvCommitBoundary::BeforeCommit {
                tokio::time::sleep(hold).await;
            }
            Ok(())
        })
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_deadline_passes_before_commit_gate_when_ordinary_or_versionstamped_then_no_submission()
 {
    let backend = open_backend()
        .await
        .with_fault_probe(Arc::new(HoldBeforeCommit(Duration::from_millis(400))));
    for versionstamped in [false, true] {
        let logical_key = unique_key(if versionstamped {
            "gate-versionstamped"
        } else {
            "gate-ordinary"
        });
        let physical_key = physical_key(&backend, &logical_key);
        let callbacks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&callbacks);
        let deadline = deadline_after(Duration::from_millis(150), Duration::from_secs(2));
        let request = AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline);
        let error = if versionstamped {
            backend
                .with_versionstamped_attempt(request, move |attempt| {
                    Box::pin(async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        attempt.stage(vec![KvMutation::PutUnchunked {
                            key: logical_key,
                            value: b"staged".to_vec(),
                        }])
                    })
                })
                .await
                .map(|_| ())
                .unwrap_err()
        } else {
            backend
                .with_attempt(request, move |attempt| {
                    Box::pin(async move {
                        observed.fetch_add(1, Ordering::SeqCst);
                        attempt.stage(vec![KvMutation::PutUnchunked {
                            key: logical_key,
                            value: b"staged".to_vec(),
                        }])
                    })
                })
                .await
                .unwrap_err()
        };
        assert!(
            is_deadline(&error, AttemptPhase::Settle),
            "versionstamped={versionstamped} {error:?}"
        );
        assert_eq!(callbacks.load(Ordering::SeqCst), 1);
        assert!(read_key(&backend.database, &physical_key).await.is_none());
    }
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_native_timeout_at_submitted_commit_when_settled_then_outcome_is_unknown() {
    let backend = open_backend().await;
    let logical_key = unique_key("submitted-timeout");
    let physical_key = physical_key(&backend, &logical_key);
    let deadline = deadline_after(Duration::from_secs(10), Duration::from_secs(2));
    let error = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, deadline.class).started(deadline),
            move |attempt| {
                Box::pin(async move {
                    attempt.stage(vec![KvMutation::PutUnchunked {
                        key: logical_key,
                        value: b"staged".to_vec(),
                    }])?;
                    // The native timer elapses before the operation deadline,
                    // so the commit gate opens and the submitted commit
                    // observes 1031.
                    attempt
                        .native_transaction()
                        .set_option(TransactionOption::Timeout(1))
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Ok(())
                })
            },
        )
        .await
        .unwrap_err();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert!(
        !error
            .attempt_kind()
            .is_some_and(|kind| kind != storage_types::AttemptKind::CommitUnknown)
    );
    println!(
        "submitted commit code 1031 unknown=true durable_key={}",
        read_key(&backend.database, &physical_key).await.is_some(),
    );
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_staged_native_write_when_destroyed_before_commit_then_fresh_read_is_absent() {
    let backend = open_backend().await;
    let key = unique_key("destroy-before-commit");
    assert!(read_key(&backend.database, &key).await.is_none());
    let transaction = backend.database.create_trx().unwrap();
    transaction.set(&key, b"staged");
    assert_eq!(
        transaction.get(&key, false).await.unwrap().as_deref(),
        Some(&b"staged"[..])
    );
    drop(transaction);
    assert!(read_key(&backend.database, &key).await.is_none());
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_native_timeout_or_cancel_when_precommit_read_settles_then_deadline_rejection() {
    let backend = open_backend().await;
    let key = unique_key("native-errors");
    let transaction = backend.database.create_trx().unwrap();
    transaction
        .set_option(TransactionOption::Timeout(1))
        .unwrap();
    transaction.set(&key, b"staged");
    tokio::time::sleep(Duration::from_millis(20)).await;
    let timed_out = transaction
        .get(&key, false)
        .await
        .err()
        .expect("native timeout");
    assert_eq!(timed_out.code(), 1031);
    assert!(is_deadline(
        &super::error::map_operation_error(timed_out),
        AttemptPhase::Statement
    ));
    drop(transaction);
    assert!(read_key(&backend.database, &key).await.is_none());

    let transaction = backend.database.create_trx().unwrap();
    // A far-future read version cannot complete before cancellation.
    let version = backend
        .database
        .create_trx()
        .unwrap()
        .get_read_version()
        .await
        .unwrap();
    transaction.set_read_version(version + 1_000_000_000_000);
    transaction
        .set_option(TransactionOption::Timeout(10_000))
        .unwrap();
    let pending_read = transaction.get(&key, false);
    let cancelled = transaction.cancel();
    let error = pending_read.await.err().expect("native cancellation");
    assert_eq!(error.code(), 1025);
    assert!(is_deadline(
        &super::error::map_operation_error(error),
        AttemptPhase::Statement
    ));
    drop(cancelled);
    assert!(read_key(&backend.database, &key).await.is_none());
}

/// Bound of every wait of a stopped-attempt test.
#[cfg(feature = "fault-injection")]
const STOP_WAIT: Duration = Duration::from_secs(10);

/// Holds an attempt at one commit boundary until released, and tells the test
/// that the boundary was reached.
#[cfg(feature = "fault-injection")]
struct HoldAtBoundary {
    boundary: crate::KvCommitBoundary,
    reached: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
    release: tokio::sync::Notify,
}

#[cfg(feature = "fault-injection")]
impl crate::KvFaultProbe for HoldAtBoundary {
    fn fail(&self, _call: crate::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if boundary == self.boundary {
                let _ = self.reached.lock().unwrap().send(());
                self.release.notified().await;
            }
            Ok(())
        })
    }
}

/// Runs one write attempt on a runtime that is shut down when the attempt
/// reaches `boundary`, then reads the key through a fresh backend of the same
/// cluster and prefix on another runtime.
#[cfg(feature = "fault-injection")]
fn stopped_foundationdb_write(boundary: crate::KvCommitBoundary) -> (StorageError, bool) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let handle = runtime.handle().clone();
    let (reached, wait) = std::sync::mpsc::channel();
    let hold = Arc::new(HoldAtBoundary {
        boundary,
        reached: std::sync::Mutex::new(reached),
        release: tokio::sync::Notify::new(),
    });
    let backend = handle.block_on(async { open_backend().await.with_fault_probe(hold.clone()) });
    let logical_key = unique_key("stopped-write");
    let physical_key = physical_key(&backend, &logical_key);
    let stopper = std::thread::spawn(move || {
        let reached = wait.recv_timeout(STOP_WAIT).is_ok();
        runtime.shutdown_background();
        reached
    });
    let result = {
        let _context = handle.enter();
        futures::executor::block_on(
            backend.with_attempt(
                AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                    .start(backend.attempt_deadlines()),
                move |attempt| {
                    Box::pin(async move {
                        attempt.stage(vec![KvMutation::PutUnchunked {
                            key: logical_key,
                            value: b"stopped".to_vec(),
                        }])
                    })
                },
            ),
        )
    };
    assert!(
        stopper.join().unwrap(),
        "the attempt did not reach {boundary:?} within {STOP_WAIT:?}"
    );
    hold.release.notify_one();
    drop(backend);
    let error = result.expect_err("an attempt on a stopped runtime cannot return its value");
    let verifier = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let present = verifier.block_on(async {
        let backend = open_backend().await;
        read_key(&backend.database, &physical_key).await.is_some()
    });
    (error, present)
}

#[cfg(feature = "fault-injection")]
#[test]
#[ignore = "requires just test-fdb-owned"]
fn given_write_attempt_stopped_before_commit_submission_when_supervised_then_known_and_not_committed()
 {
    let (error, present) = stopped_foundationdb_write(crate::KvCommitBoundary::BeforeCommit);
    assert!(
        !error.is_commit_unknown(),
        "a stop before commit submission is a known failure: {error:?}"
    );
    assert!(!present, "no commit was submitted");
}

#[cfg(feature = "fault-injection")]
#[test]
#[ignore = "requires just test-fdb-owned"]
fn given_write_attempt_stopped_after_commit_submission_when_supervised_then_unknown_and_committed()
{
    let (error, present) =
        stopped_foundationdb_write(crate::KvCommitBoundary::AfterDurableAcknowledgement);
    assert!(
        error.is_commit_unknown(),
        "a stop after commit submission is unknown: {error:?}"
    );
    assert!(present, "the commit reached the cluster");
}

#[cfg(feature = "fault-injection")]
struct CommitAnswer1025 {
    fired: AtomicBool,
}

#[cfg(feature = "fault-injection")]
impl crate::KvFaultProbe for CommitAnswer1025 {
    fn fail(&self, _call: crate::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn replace_commit_answer(&self) -> Option<i32> {
        (!self.fired.swap(true, Ordering::SeqCst)).then_some(1025)
    }
}

#[cfg(feature = "fault-injection")]
#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_cancelled_answer_1025_at_submitted_commit_when_settled_then_unknown_and_never_replayed()
 {
    let backend = open_backend()
        .await
        .with_fault_probe(Arc::new(CommitAnswer1025 {
            fired: AtomicBool::new(false),
        }));
    let logical_key = unique_key("commit-1025");
    let physical_key = physical_key(&backend, &logical_key);
    let calls = Arc::new(AtomicUsize::new(0));
    let result = storage_common::retry::retry_native_conflicts(
        &storage_types::SystemClock,
        storage_provider::DYNAMO_WRITE_RETRY,
        backend.attempt_deadlines(),
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write),
        |request| {
            let calls = Arc::clone(&calls);
            let logical_key = logical_key.clone();
            backend.with_attempt(request, move |attempt| {
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    attempt.stage(vec![KvMutation::PutUnchunked {
                        key: logical_key.clone(),
                        value: b"staged".to_vec(),
                    }])
                })
            })
        },
    )
    .await;
    let error = result.unwrap_err();
    assert!(error.is_commit_unknown(), "{error:?}");
    assert_eq!(
        error.attempt_kind(),
        Some(storage_types::AttemptKind::CommitUnknown)
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1, "never replayed");
    let durable = read_key(&backend.database, &physical_key)
        .await
        .expect("the submitted commit is durable");
    assert_eq!(
        crate::value_layout::decode_unchunked_value(&durable, usize::MAX).unwrap(),
        b"staged"
    );
    println!("submitted commit code 1025 unknown=true calls=1 durable_key=true");
}
