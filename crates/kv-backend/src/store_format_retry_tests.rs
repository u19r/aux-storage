use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures::future::BoxFuture;
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass, StorageError, StorageResult};

use super::{
    KvAttempt as _, KvBackend as _, KvStore, ReadProtection,
    constants::{
        INFRASTRUCTURE_FORMAT_KEY, MAINTENANCE_IDENTITY_KEY, SYSTEM_STREAM_SCOPE_KEY,
        TTL_INDEX_FORMAT_KEY,
    },
    rocksdb::RocksDbBackend,
};

#[derive(Default)]
struct FormatProbe {
    /// Withhold the durable answer of the first write commit (committed, then
    /// unknown).
    withhold_first_answer: bool,
    /// Fail this many first `Stage` calls with an unknown outcome (nothing
    /// committed).
    unknown_stages: usize,
    fired: AtomicBool,
    unknown_answers: AtomicUsize,
    stages_before: AtomicUsize,
    stages_after: AtomicUsize,
}

impl crate::KvFaultProbe for FormatProbe {
    fn fail(&self, call: crate::KvFaultCall) -> Option<StorageError> {
        if call.operation != crate::KvFaultOperation::Stage || call.mutations == 0 {
            return None;
        }
        if self.unknown_answers.load(Ordering::SeqCst) < self.unknown_stages {
            self.unknown_answers.fetch_add(1, Ordering::SeqCst);
            self.fired.store(true, Ordering::SeqCst);
            return Some(StorageError::commit_unknown("test: unknown before commit"));
        }
        if self.fired.load(Ordering::SeqCst) {
            self.stages_after.fetch_add(1, Ordering::SeqCst);
        } else {
            self.stages_before.fetch_add(1, Ordering::SeqCst);
        }
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: crate::KvCommitBoundary,
        mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        let withhold = self.withhold_first_answer
            && boundary == crate::KvCommitBoundary::AfterDurableAcknowledgement
            && mode == AttemptMode::ReadWrite
            && !self.fired.swap(true, Ordering::SeqCst);
        if withhold {
            self.unknown_answers.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(async move {
            if withhold {
                Err(StorageError::internal("test: withheld durable answer"))
            } else {
                Ok(())
            }
        })
    }
}

fn open(path: std::path::PathBuf, probe: Arc<FormatProbe>) -> RocksDbBackend {
    RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256)
        .unwrap()
        .with_fault_probe(probe)
}

async fn format_rows(path: std::path::PathBuf) -> Vec<Option<Vec<u8>>> {
    let reopened =
        RocksDbBackend::initialize(path, storage_types::AttemptDeadlines::default(), 256).unwrap();
    reopened
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(reopened.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    attempt
                        .get_batch(
                            vec![
                                INFRASTRUCTURE_FORMAT_KEY.to_vec(),
                                TTL_INDEX_FORMAT_KEY.to_vec(),
                                SYSTEM_STREAM_SCOPE_KEY.to_vec(),
                                MAINTENANCE_IDENTITY_KEY.to_vec(),
                            ],
                            ReadProtection::Snapshot,
                        )
                        .await
                })
            },
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn given_unknown_answer_after_format_commit_when_initialized_then_next_round_accepts_committed_rows()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let probe = Arc::new(FormatProbe {
        withhold_first_answer: true,
        ..Default::default()
    });
    let backend = open(path.clone(), probe.clone());
    let result =
        KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE).await;
    assert!(result.is_ok());
    drop(result);

    assert_eq!(probe.unknown_answers.load(Ordering::SeqCst), 1);
    assert!(probe.stages_before.load(Ordering::SeqCst) >= 1);
    assert_eq!(probe.stages_after.load(Ordering::SeqCst), 0);
    let rows = format_rows(path).await;
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(Option::is_some));
}

#[tokio::test]
async fn given_unknown_outcome_before_format_commit_when_initialized_then_next_round_writes_rows() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let probe = Arc::new(FormatProbe {
        unknown_stages: 1,
        ..Default::default()
    });
    let backend = open(path.clone(), probe.clone());
    let result =
        KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE).await;
    assert!(result.is_ok());
    drop(result);

    assert_eq!(probe.unknown_answers.load(Ordering::SeqCst), 1);
    assert!(probe.stages_after.load(Ordering::SeqCst) >= 1);
    let rows = format_rows(path).await;
    assert_eq!(rows.len(), 4);
    assert!(rows.iter().all(Option::is_some));
}

#[tokio::test]
async fn given_unknown_outcome_on_every_round_when_initialized_then_fails_unknown_after_four_rounds()
 {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("store");
    let probe = Arc::new(FormatProbe {
        unknown_stages: usize::MAX,
        ..Default::default()
    });
    let backend = open(path.clone(), probe.clone());
    let error = match KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
    {
        Ok(_) => panic!("format initialization succeeded despite unknown outcomes"),
        Err(error) => error,
    };
    assert!(error.is_commit_unknown());

    assert_eq!(
        probe.unknown_answers.load(Ordering::SeqCst),
        1 + crate::store::FORMAT_UNKNOWN_OUTCOME_RETRIES as usize
    );
    assert_eq!(probe.stages_after.load(Ordering::SeqCst), 0);
    let rows = format_rows(path).await;
    assert_eq!(rows, vec![None, None, None, None]);
}
