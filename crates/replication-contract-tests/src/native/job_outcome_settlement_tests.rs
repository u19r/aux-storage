//! Real SQLite settlement contention at the scheduled-job store boundary.

use std::{
    num::NonZeroU16,
    sync::{Arc, Mutex},
    time::Duration,
};

use bg_job_store::{ScheduledJobStore, ScheduledJobStoreError, ScheduledJobStoreSource};
use bytes::Bytes;
use scheduled_bg_jobs::{
    EffectMode, EnqueueOptions, ExecutionContext, FairnessClass, FairnessKey, FairnessPath,
    FairnessPolicy, HandlerFuture, JobCatalog, JobHandler, JobId, JobKind, JobOutcome, JobSpec,
    JobState, LifecycleObservation, Namespace, OutcomeCode, OwnerId, QueueName, RetryClass,
    RetryPreset, RetrySelection, Timestamp,
    runtime::{JobSystem, JobSystemBuilder},
    store::{
        Check, CommitOutcome, JobStore, RangePage, RangeRequest, StoreCapabilities, StoreKey,
        StoreTransaction,
    },
    test_support::{
        TestProfile,
        codec::{KeyCodec, KeyTag},
        raw::OutcomeResult,
    },
};
use tokio::sync::oneshot;

use super::fixture;
use crate::HarnessResult;

const NOW: Timestamp = Timestamp::from_millis(1_800_000_000_000);

struct SettlementHandler;

impl JobHandler for SettlementHandler {
    fn execute<'a>(
        &'a self,
        _context: ExecutionContext<'a>,
        _payload: &'a [u8],
    ) -> HandlerFuture<'a> {
        Box::pin(async {
            Ok(JobOutcome::Complete(
                OutcomeCode::try_new("completed").expect("outcome is valid"),
            ))
        })
    }
}

#[derive(Clone, Debug, Default)]
struct SettlementReport {
    commit_count: usize,
    first_native_result: Option<CommitOutcome>,
    second_native_result: Option<CommitOutcome>,
    mismatched_tags: Vec<KeyTag>,
    unchanged_authority_tags: Vec<KeyTag>,
    non_wake_mutations_unchanged: bool,
}

struct FirstObservation {
    transaction: StoreTransaction,
    mutation_values: Vec<Option<Bytes>>,
}

struct ArmedInterposition {
    next_commit: usize,
    ready_sender: Option<oneshot::Sender<()>>,
    ready_receiver: Option<oneshot::Receiver<()>>,
    release_sender: Option<oneshot::Sender<()>>,
    release_receiver: Option<oneshot::Receiver<()>>,
    first: Option<FirstObservation>,
    report: SettlementReport,
}

impl ArmedInterposition {
    fn new() -> Self {
        let (ready_sender, ready_receiver) = oneshot::channel();
        let (release_sender, release_receiver) = oneshot::channel();
        Self {
            next_commit: 0,
            ready_sender: Some(ready_sender),
            ready_receiver: Some(ready_receiver),
            release_sender: Some(release_sender),
            release_receiver: Some(release_receiver),
            first: None,
            report: SettlementReport::default(),
        }
    }
}

#[derive(Clone)]
struct NativeSettlementStore {
    inner: ScheduledJobStore,
    namespace: Namespace,
    interposition: Arc<Mutex<Option<ArmedInterposition>>>,
}

enum CommitRole {
    First {
        ready: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    },
    Second {
        ready: oneshot::Receiver<()>,
        release: oneshot::Sender<()>,
    },
    Later,
    Unarmed,
}

impl NativeSettlementStore {
    fn new(inner: ScheduledJobStore, namespace: Namespace) -> Self {
        Self {
            inner,
            namespace,
            interposition: Arc::new(Mutex::new(None)),
        }
    }

    fn arm(&self) {
        *self.interposition.lock().expect("interposition lock") = Some(ArmedInterposition::new());
    }

    fn report(&self) -> SettlementReport {
        self.interposition
            .lock()
            .expect("interposition lock")
            .as_ref()
            .expect("interposition is armed")
            .report
            .clone()
    }

    fn next_role(&self) -> CommitRole {
        let mut guard = self.interposition.lock().expect("interposition lock");
        let Some(state) = guard.as_mut() else {
            return CommitRole::Unarmed;
        };
        state.next_commit += 1;
        state.report.commit_count = state.next_commit;
        match state.next_commit {
            1 => CommitRole::First {
                ready: state.ready_sender.take().expect("first ready sender"),
                release: state
                    .release_receiver
                    .take()
                    .expect("first release receiver"),
            },
            2 => CommitRole::Second {
                ready: state.ready_receiver.take().expect("second ready receiver"),
                release: state.release_sender.take().expect("second release sender"),
            },
            _ => CommitRole::Later,
        }
    }

    async fn hold_first(
        &self,
        transaction: &StoreTransaction,
        ready: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
    ) -> Result<CommitOutcome, ScheduledJobStoreError> {
        let mutation_keys = transaction
            .mutations()
            .iter()
            .map(|mutation| mutation.key().clone())
            .collect::<Vec<_>>();
        let mutation_values = self.inner.get_many(&mutation_keys).await?;
        self.interposition
            .lock()
            .expect("interposition lock")
            .as_mut()
            .expect("interposition is armed")
            .first = Some(FirstObservation {
            transaction: transaction.clone(),
            mutation_values,
        });
        ready.send(()).expect("second settlement remains active");
        release.await.expect("second settlement releases first");

        let outcome = self.inner.commit(transaction).await?;
        self.record_first_outcome(outcome).await?;
        Ok(outcome)
    }

    async fn commit_second(
        &self,
        transaction: &StoreTransaction,
        ready: oneshot::Receiver<()>,
        release: oneshot::Sender<()>,
    ) -> Result<CommitOutcome, ScheduledJobStoreError> {
        ready
            .await
            .expect("first settlement records its transaction");
        let outcome = self.inner.commit(transaction).await?;
        if outcome == CommitOutcome::Committed {
            self.record_winner_conflicts().await?;
        }
        self.interposition
            .lock()
            .expect("interposition lock")
            .as_mut()
            .expect("interposition is armed")
            .report
            .second_native_result = Some(outcome);
        release.send(()).expect("first settlement remains active");
        Ok(outcome)
    }

    async fn record_winner_conflicts(&self) -> Result<(), ScheduledJobStoreError> {
        let transaction = self.first_transaction();
        let keys = transaction
            .checks()
            .iter()
            .map(|check| check.key().clone())
            .collect::<Vec<_>>();
        let values = self.inner.get_many(&keys).await?;
        let mismatched_tags = transaction
            .checks()
            .iter()
            .zip(values.iter())
            .filter(|(check, value)| !check_matches(check, value.as_ref()))
            .map(|(check, _)| self.key_tag(check.key()))
            .collect();
        self.interposition
            .lock()
            .expect("interposition lock")
            .as_mut()
            .expect("interposition is armed")
            .report
            .mismatched_tags = mismatched_tags;
        Ok(())
    }

    async fn record_first_outcome(
        &self,
        outcome: CommitOutcome,
    ) -> Result<(), ScheduledJobStoreError> {
        let (transaction, before) = {
            let guard = self.interposition.lock().expect("interposition lock");
            let first = guard
                .as_ref()
                .and_then(|state| state.first.as_ref())
                .expect("first transaction is recorded");
            (first.transaction.clone(), first.mutation_values.clone())
        };
        let mutation_keys = transaction
            .mutations()
            .iter()
            .map(|mutation| mutation.key().clone())
            .collect::<Vec<_>>();
        let after = self.inner.get_many(&mutation_keys).await?;
        let unchanged = transaction
            .mutations()
            .iter()
            .zip(before.iter().zip(after.iter()))
            .filter(|(mutation, _)| self.key_tag(mutation.key()) != KeyTag::Wake)
            .all(|(_, (before, after))| before == after);
        let authority = self.unchanged_authority_tags(&transaction).await?;

        let mut guard = self.interposition.lock().expect("interposition lock");
        let report = &mut guard.as_mut().expect("interposition is armed").report;
        report.first_native_result = Some(outcome);
        report.unchanged_authority_tags = authority;
        report.non_wake_mutations_unchanged = unchanged;
        Ok(())
    }

    async fn unchanged_authority_tags(
        &self,
        transaction: &StoreTransaction,
    ) -> Result<Vec<KeyTag>, ScheduledJobStoreError> {
        let keys = transaction
            .checks()
            .iter()
            .map(|check| check.key().clone())
            .collect::<Vec<_>>();
        let values = self.inner.get_many(&keys).await?;
        Ok(transaction
            .checks()
            .iter()
            .zip(values.iter())
            .filter(|(check, value)| check_matches(check, value.as_ref()))
            .map(|(check, _)| self.key_tag(check.key()))
            .filter(|tag| matches!(tag, KeyTag::Job | KeyTag::Lease | KeyTag::Operation))
            .collect())
    }

    fn first_transaction(&self) -> StoreTransaction {
        self.interposition
            .lock()
            .expect("interposition lock")
            .as_ref()
            .and_then(|state| state.first.as_ref())
            .expect("first transaction is recorded")
            .transaction
            .clone()
    }

    fn key_tag(&self, key: &StoreKey) -> KeyTag {
        KeyCodec::decode(&self.namespace, key)
            .expect("scheduled-job key decodes")
            .tag
    }
}

impl JobStore for NativeSettlementStore {
    type Error = ScheduledJobStoreError;

    fn capabilities(&self) -> StoreCapabilities {
        self.inner.capabilities()
    }

    async fn get(&self, key: StoreKey) -> Result<Option<Bytes>, Self::Error> {
        self.inner.get(key).await
    }

    async fn get_many(&self, keys: &[StoreKey]) -> Result<Vec<Option<Bytes>>, Self::Error> {
        self.inner.get_many(keys).await
    }

    async fn read_range(&self, request: RangeRequest) -> Result<RangePage, Self::Error> {
        self.inner.read_range(request).await
    }

    async fn commit(&self, transaction: &StoreTransaction) -> Result<CommitOutcome, Self::Error> {
        match self.next_role() {
            CommitRole::First { ready, release } => {
                self.hold_first(transaction, ready, release).await
            }
            CommitRole::Second { ready, release } => {
                self.commit_second(transaction, ready, release).await
            }
            CommitRole::Later | CommitRole::Unarmed => self.inner.commit(transaction).await,
        }
    }
}

fn check_matches(check: &Check, value: Option<&Bytes>) -> bool {
    match check {
        Check::Absent(_) => value.is_none(),
        Check::Value(_, expected) => value == Some(expected),
    }
}

struct SettlementFixture {
    _database: fixture::FileSqlite,
    store: NativeSettlementStore,
    system: JobSystem<NativeSettlementStore>,
    handle: scheduled_bg_jobs::JobHandle<Bytes>,
}

impl SettlementFixture {
    async fn try_new() -> HarnessResult<Self> {
        let database = fixture::FileSqlite::try_new().await?;
        let namespace = Namespace::try_new("native-outcome-settlement")?;
        let inner = database
            .store
            .scheduled_job_store(fixture::scheduled_job_config())?;
        let store = NativeSettlementStore::new(inner, namespace.clone());
        let mut catalog = JobCatalog::new();
        let handle = catalog.register(
            JobSpec::bytes(
                JobKind::try_new("native-outcome-settlement")?,
                NonZeroU16::MIN,
                QueueName::try_new("native-outcome-settlement")?,
                SettlementHandler,
            )
            .policy(
                EffectMode::Fenced,
                FairnessPolicy::from(NonZeroU16::MIN),
                RetrySelection::Preset(RetryPreset::Network),
                Duration::from_secs(30),
            ),
        )?;
        let system = JobSystemBuilder::new(store.clone())
            .catalog(catalog)
            .namespace(namespace)
            .owner(OwnerId::from([1; 16]))
            .scheduler(Arc::new(TestProfile::new()))
            .build()?;
        Ok(Self {
            _database: database,
            store,
            system,
            handle,
        })
    }

    async fn claim(
        &self,
        id: u8,
    ) -> HarnessResult<scheduled_bg_jobs::test_support::raw::ClaimedJob> {
        let job_id = JobId::from([id; 16]);
        let fairness = FairnessClass::from(FairnessPath::new(FairnessKey::try_new([id])?, None));
        self.system
            .enqueue_typed(
                &self.handle,
                Bytes::from_static(b"payload"),
                EnqueueOptions::new(fairness)
                    .job_id(job_id)
                    .scheduled_at(NOW),
                NOW,
            )
            .await?;
        Ok(self.system.claim(job_id, NOW).await?)
    }
}

#[derive(Clone, Copy)]
enum ExpectedSecondOutcome {
    Complete,
    Retry,
}

async fn verify_overlapping_settlements(
    second_outcome: JobOutcome,
    expected: ExpectedSecondOutcome,
) -> HarnessResult<()> {
    let fixture = SettlementFixture::try_new().await?;
    let first = fixture.claim(1).await?;
    let second = fixture.claim(2).await?;
    fixture.store.arm();
    let settlements = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            fixture.system.apply_outcome(first, completion(), NOW),
            fixture.system.apply_outcome(second, second_outcome, NOW),
        )
    })
    .await?;
    assert!(matches!(settlements.0?, OutcomeResult::Completed(_)));
    match (expected, settlements.1?) {
        (ExpectedSecondOutcome::Complete, OutcomeResult::Completed(_))
        | (ExpectedSecondOutcome::Retry, OutcomeResult::Retried { .. }) => {}
        (_, outcome) => panic!("unexpected competing settlement result: {outcome:?}"),
    }
    assert_native_conflict_report(&fixture.store.report());
    assert_final_lifecycle(&fixture, expected).await?;
    Ok(())
}

fn assert_native_conflict_report(report: &SettlementReport) {
    assert!(report.commit_count >= 3, "wake conflict must be retried");
    assert_eq!(report.second_native_result, Some(CommitOutcome::Committed));
    assert_eq!(report.first_native_result, Some(CommitOutcome::Conflict));
    assert_eq!(report.mismatched_tags, [KeyTag::Wake]);
    assert_eq!(
        report.unchanged_authority_tags,
        [KeyTag::Job, KeyTag::Lease, KeyTag::Operation]
    );
    assert!(
        report.non_wake_mutations_unchanged,
        "conflicting settlement applied a partial non-wake mutation"
    );
}

async fn assert_final_lifecycle(
    fixture: &SettlementFixture,
    expected: ExpectedSecondOutcome,
) -> HarnessResult<()> {
    assert!(matches!(
        fixture
            .system
            .observe_lifecycle(JobId::from([1; 16]))
            .await?,
        LifecycleObservation::Terminal(_)
    ));
    let second = fixture
        .system
        .observe_lifecycle(JobId::from([2; 16]))
        .await?;
    match expected {
        ExpectedSecondOutcome::Complete => {
            assert!(matches!(second, LifecycleObservation::Terminal(_)));
        }
        ExpectedSecondOutcome::Retry => assert!(matches!(
            second,
            LifecycleObservation::Current {
                record,
                ..
            } if record.state == JobState::Scheduled
        )),
    }
    Ok(())
}

fn completion() -> JobOutcome {
    JobOutcome::Complete(OutcomeCode::try_new("completed").expect("outcome is valid"))
}

fn retry() -> JobOutcome {
    JobOutcome::Retry {
        class: RetryClass::Transient,
        not_before: Some(Timestamp::from_millis(NOW.as_millis() + 1_000)),
        code: OutcomeCode::try_new("retry").expect("outcome is valid"),
    }
}

#[tokio::test]
async fn given_distinct_sqlite_claims_when_completions_overlap_then_both_settle_after_only_wake_conflicts()
-> HarnessResult<()> {
    verify_overlapping_settlements(completion(), ExpectedSecondOutcome::Complete).await
}

#[tokio::test]
async fn given_distinct_sqlite_claims_when_completion_and_retry_overlap_then_both_settle_after_only_wake_conflicts()
-> HarnessResult<()> {
    verify_overlapping_settlements(retry(), ExpectedSecondOutcome::Retry).await
}

#[tokio::test]
async fn given_reclaimed_sqlite_job_when_stale_claim_completes_then_successor_authority_is_unchanged()
-> HarnessResult<()> {
    let fixture = SettlementFixture::try_new().await?;
    let stale = fixture.claim(3).await?;
    let stale_authority = stale.authority().ok_or("stale claim authority missing")?;
    let reclaimed_at = NOW.checked_add(Duration::from_secs(31))?;
    let successor = fixture
        .system
        .claim(JobId::from([3; 16]), reclaimed_at)
        .await?;
    let successor_authority = successor
        .authority()
        .ok_or("successor claim authority missing")?;
    assert!(successor_authority.fence > stale_authority.fence);

    assert!(matches!(
        fixture.system.apply_outcome(stale, completion(), NOW).await,
        Err(scheduled_bg_jobs::JobSystemError::LeaseLost)
    ));
    let observed = fixture
        .system
        .observe_lifecycle(JobId::from([3; 16]))
        .await?;
    assert!(matches!(
        observed,
        LifecycleObservation::Current {
            record,
            ..
        } if record.generation == successor_authority.generation
            && matches!(record.state, JobState::Leased(ref lease)
                if lease.owner == successor_authority.owner
                    && lease.token == successor_authority.token
                    && lease.fence == successor_authority.fence)
    ));
    Ok(())
}
