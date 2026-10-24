use std::num::{NonZeroU64, NonZeroUsize};

use scheduled_bg_jobs::{JobId, Namespace};
use storage_types::{
    Revision, StorageError, StorageResult, TableName, TimestampMillis,
    maintenance::{
        MaintenanceKind, StepResult, TargetIncarnation, TaskAuthority, TaskProgress, TaskScope,
    },
};

use crate::maintenance::{
    ExpirationCurrentObservation, ExpirationCurrentRead, ExpirationPageWrite, ExpirationPosition,
    ExpirationProgress, ExpirationProgressWrite, ExpirationRequest, ExpirationScanPage,
    ExpirationScanRead, ExpirationTarget, ExpirationTargetRead, ExpirationTransaction,
};

struct ExpirationFixture {
    destination: Namespace,
    job: JobId,
    transaction: RecordingExpirationTransaction,
}

impl ExpirationFixture {
    fn with_progress(fence: u64, revision: u64, position: ExpirationPosition) -> Self {
        let destination = Namespace::try_new("expiration-contract").expect("valid namespace");
        let job = JobId::from([7; JobId::BYTE_LENGTH]);
        let identity = TargetIncarnation::try_from(b"database-a".to_vec())
            .expect("valid destination incarnation");
        let authority = TaskAuthority {
            scope: TaskScope {
                destination: destination.clone(),
                target: identity.clone(),
                work: MaintenanceKind::TtlSweep,
            },
            job,
            fence: NonZeroU64::new(fence).expect("non-zero fence"),
        };
        Self {
            destination,
            job,
            transaction: RecordingExpirationTransaction {
                identity,
                progress: Some(TaskProgress {
                    authority,
                    revision: Revision::from(revision),
                    state: ExpirationProgress {
                        cutoff: TimestampMillis::from_timestamp(100_000),
                        position,
                    },
                }),
                progress_writes: 0,
            },
        }
    }

    fn request(&self, fence: u64, cutoff: i64) -> ExpirationRequest {
        ExpirationRequest {
            destination: self.destination.clone(),
            job: self.job,
            fence: NonZeroU64::new(fence).expect("non-zero fence"),
            requested_cutoff: TimestampMillis::from_timestamp(cutoff),
            candidate_limit: NonZeroUsize::new(25).expect("non-zero page limit"),
        }
    }
}

struct RecordingExpirationTransaction {
    identity: TargetIncarnation,
    progress: Option<TaskProgress<ExpirationProgress>>,
    progress_writes: usize,
}

impl RecordingExpirationTransaction {
    fn persist_progress(
        &mut self,
        write: ExpirationProgressWrite,
    ) -> StorageResult<StepResult<()>> {
        if write.current_target != self.identity
            || write.expected_revision != self.progress.as_ref().map(|progress| progress.revision)
        {
            return Err(StorageError::validation(
                "expiration write lost its authority",
            ));
        }
        self.progress = Some(write.next_progress);
        self.progress_writes += 1;
        Ok(write.result)
    }

    fn progress_observation(&self) -> Option<TaskProgress<ExpirationProgress>> {
        self.progress.as_ref().map(|progress| TaskProgress {
            authority: progress.authority.clone(),
            revision: progress.revision,
            state: ExpirationProgress {
                cutoff: progress.state.cutoff,
                position: match &progress.state.position {
                    ExpirationPosition::Start => ExpirationPosition::Start,
                    ExpirationPosition::Active {
                        table_name,
                        table_incarnation,
                        policy_generation,
                        cursor,
                    } => ExpirationPosition::Active {
                        table_name: table_name.clone(),
                        table_incarnation: table_incarnation.clone(),
                        policy_generation: *policy_generation,
                        cursor: cursor.clone(),
                    },
                    ExpirationPosition::After { table_name } => ExpirationPosition::After {
                        table_name: table_name.clone(),
                    },
                },
            },
        })
    }

    fn unexpected_read<T>() -> StorageResult<T> {
        Err(StorageError::internal(
            "idle expiration transaction attempted a page read",
        ))
    }
}

impl ExpirationTransaction for RecordingExpirationTransaction {
    async fn read_maintenance_identity(&mut self) -> StorageResult<TargetIncarnation> {
        Ok(self.identity.clone())
    }

    async fn read_expiration_progress(
        &mut self,
    ) -> StorageResult<Option<TaskProgress<ExpirationProgress>>> {
        Ok(self.progress_observation())
    }

    async fn read_expiration_target(
        &mut self,
        _read: &ExpirationTargetRead,
    ) -> StorageResult<Option<ExpirationTarget>> {
        Ok(None)
    }

    async fn read_expiration_scan(
        &mut self,
        _read: &ExpirationScanRead,
    ) -> StorageResult<ExpirationScanPage> {
        Self::unexpected_read()
    }

    async fn read_expiration_candidates(
        &mut self,
        _read: &ExpirationCurrentRead,
    ) -> StorageResult<ExpirationCurrentObservation> {
        Self::unexpected_read()
    }

    async fn write_expiration_progress(
        &mut self,
        write: ExpirationProgressWrite,
    ) -> StorageResult<StepResult<()>> {
        self.persist_progress(write)
    }

    async fn write_expiration_page(
        &mut self,
        _write: ExpirationPageWrite,
    ) -> StorageResult<StepResult<()>> {
        Self::unexpected_read()
    }
}

#[tokio::test]
async fn higher_fence_on_idle_task_is_persisted_without_a_domain_effect() {
    let mut fixture = ExpirationFixture::with_progress(2, 5, ExpirationPosition::Start);
    let request = fixture.request(3, 200_000);

    let result = fixture
        .transaction
        .advance_expiration(&request)
        .await
        .expect("idle fence advances");

    assert!(matches!(result, StepResult::Complete));
    assert_eq!(fixture.transaction.progress_writes, 1);
    let progress = fixture.transaction.progress.expect("durable progress");
    assert_eq!(progress.authority.fence, NonZeroU64::new(3).unwrap());
    assert_eq!(progress.revision, Revision::from(6));
    assert!(matches!(
        &progress.state.position,
        ExpirationPosition::Start
    ));
    assert_eq!(
        progress.state.cutoff,
        TimestampMillis::from_timestamp(200_000)
    );
}

#[tokio::test]
async fn periodic_end_resets_only_the_cursor_and_retains_authority() {
    let mut fixture = ExpirationFixture::with_progress(
        3,
        8,
        ExpirationPosition::After {
            table_name: TableName::new("last-table"),
        },
    );
    let request = fixture.request(3, 200_000);

    let result = fixture
        .transaction
        .advance_expiration(&request)
        .await
        .expect("period ends");

    assert!(matches!(result, StepResult::Complete));
    let progress = fixture.transaction.progress.expect("durable progress");
    assert_eq!(progress.authority.fence, NonZeroU64::new(3).unwrap());
    assert_eq!(progress.revision, Revision::from(9));
    assert!(matches!(
        &progress.state.position,
        ExpirationPosition::Start
    ));
    assert_eq!(
        progress.state.cutoff,
        TimestampMillis::from_timestamp(100_000)
    );
}

#[tokio::test]
async fn lower_fence_is_idle_and_does_not_rewrite_progress() {
    let mut fixture = ExpirationFixture::with_progress(4, 9, ExpirationPosition::Start);
    let request = fixture.request(3, 200_000);

    let result = fixture
        .transaction
        .advance_expiration(&request)
        .await
        .expect("stale fence is an idle result");

    assert!(matches!(result, StepResult::Superseded));
    assert_eq!(fixture.transaction.progress_writes, 0);
    let progress = fixture.transaction.progress.expect("unchanged progress");
    assert_eq!(progress.authority.fence, NonZeroU64::new(4).unwrap());
    assert_eq!(progress.revision, Revision::from(9));
}
