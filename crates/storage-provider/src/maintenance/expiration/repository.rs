use std::{future::Future, num::NonZeroUsize};

use async_trait::async_trait;
use storage_common::{MAX_TTL_SWEEP_PAGE_LIMIT, retry::retry_native_conflicts};
use storage_types::{
    Revision, StorageError, StorageResult,
    maintenance::{StepResult, TargetIncarnation, TaskProgress},
};

use super::{
    ExpirationAttempt, ExpirationCandidateBatch, ExpirationChange, ExpirationCurrentObservation,
    ExpirationCurrentRead, ExpirationDeletionBatch, ExpirationEvaluation, ExpirationPagePlan,
    ExpirationPageWrite, ExpirationPort, ExpirationPosition, ExpirationProgress,
    ExpirationProgressWrite, ExpirationRequest, ExpirationScanDecision, ExpirationScanPage,
    ExpirationScanRead, ExpirationStart, ExpirationTarget, ExpirationTargetDecision,
    ExpirationTargetRead, ExpirationWrite,
};
use crate::{DYNAMO_WRITE_RETRY, DynamoPort};

pub trait ExpirationTransaction {
    fn advance_expiration(
        &mut self,
        request: &ExpirationRequest,
    ) -> impl Future<Output = StorageResult<StepResult<()>>>
    where
        Self: Sized,
    {
        async move {
            let (current_target, durable) = self.read_expiration_admission().await?;
            let evaluation = match ExpirationStart::try_new(&current_target, durable, request)? {
                ExpirationStart::Evaluate(evaluation) => evaluation,
                ExpirationStart::Superseded => return Ok(StepResult::Superseded),
                ExpirationStart::BindingMismatch => {
                    return Err(StorageError::validation(
                        "expiration task is bound to another catalog job",
                    ));
                }
            };
            evaluation
                .prepare_target(self, &current_target, request.candidate_limit)
                .await?
                .apply(self)
                .await
        }
    }

    fn read_maintenance_identity(
        &mut self,
    ) -> impl Future<Output = StorageResult<TargetIncarnation>>;

    fn read_expiration_progress(
        &mut self,
    ) -> impl Future<Output = StorageResult<Option<TaskProgress<ExpirationProgress>>>>;

    /// Observe independent authority and progress in one native read wave.
    /// The default keeps existing in-memory transaction fixtures usable.
    fn read_expiration_admission(
        &mut self,
    ) -> impl Future<Output = StorageResult<(TargetIncarnation, Option<TaskProgress<ExpirationProgress>>)>>
    {
        async move {
            let identity = self.read_maintenance_identity().await?;
            let progress = self.read_expiration_progress().await?;
            Ok((identity, progress))
        }
    }

    fn read_expiration_target(
        &mut self,
        read: &ExpirationTargetRead,
    ) -> impl Future<Output = StorageResult<Option<ExpirationTarget>>>;

    fn read_expiration_scan(
        &mut self,
        read: &ExpirationScanRead,
    ) -> impl Future<Output = StorageResult<ExpirationScanPage>>;

    fn read_expiration_candidates(
        &mut self,
        read: &ExpirationCurrentRead,
    ) -> impl Future<Output = StorageResult<ExpirationCurrentObservation>>;

    fn write_expiration_progress(
        &mut self,
        write: ExpirationProgressWrite,
    ) -> impl Future<Output = StorageResult<StepResult<()>>>;

    fn write_expiration_page(
        &mut self,
        write: ExpirationPageWrite,
    ) -> impl Future<Output = StorageResult<StepResult<()>>>;
}

#[async_trait]
pub trait ExpirationRepository: Send + Sync {
    async fn advance_expiration(&self, request: ExpirationRequest)
    -> StorageResult<StepResult<()>>;
}

#[async_trait]
impl<P: DynamoPort> ExpirationRepository for P
where for<'native> P::Attempt<'native>: ExpirationPort
{
    async fn advance_expiration(
        &self,
        request: ExpirationRequest,
    ) -> StorageResult<StepResult<()>> {
        retry_native_conflicts(self, DYNAMO_WRITE_RETRY, || {
            let request = request.clone();
            self.with_dynamo_attempt(storage_types::AttemptMode::ReadWrite, move |attempt| {
                Box::pin(async move {
                    ExpirationAttempt::new(attempt)
                        .advance_expiration(&request)
                        .await
                })
            })
        })
        .await
    }
}

impl ExpirationEvaluation {
    pub(super) async fn prepare_target(
        self,
        transaction: &mut impl ExpirationTransaction,
        current_target: &TargetIncarnation,
        candidate_limit: NonZeroUsize,
    ) -> StorageResult<ExpirationWrite> {
        let target_read = self.expiration_target_read();
        let target = transaction.read_expiration_target(&target_read).await?;
        match self.decide_expiration_target(&target_read, target, candidate_limit)? {
            ExpirationTargetDecision::Persist(change) => Ok(ExpirationWrite::Progress(
                change.into_progress_write(current_target)?,
            )),
            ExpirationTargetDecision::Read(read) => {
                self.prepare_scan(transaction, current_target, read).await
            }
        }
    }

    fn expiration_target_read(&self) -> ExpirationTargetRead {
        match &self.progress.position {
            ExpirationPosition::Start => ExpirationTargetRead::First,
            ExpirationPosition::Active {
                table_name,
                table_incarnation,
                ..
            } => ExpirationTargetRead::Exact {
                table_name: table_name.clone(),
                table_incarnation: table_incarnation.clone(),
            },
            ExpirationPosition::After { table_name } => ExpirationTargetRead::After {
                table_name: table_name.clone(),
            },
        }
    }

    fn decide_expiration_target(
        &self,
        read: &ExpirationTargetRead,
        target: Option<ExpirationTarget>,
        limit: NonZeroUsize,
    ) -> StorageResult<ExpirationTargetDecision> {
        if limit.get() > MAX_TTL_SWEEP_PAGE_LIMIT {
            return Err(StorageError::validation(
                "expiration candidate limit exceeds the supported page bound",
            ));
        }
        match target {
            Some(target) => self.decide_present_target(target, limit),
            None => self.decide_missing_target(read),
        }
    }

    fn decide_missing_target(
        &self,
        read: &ExpirationTargetRead,
    ) -> StorageResult<ExpirationTargetDecision> {
        if let ExpirationTargetRead::Exact { table_name, .. } = read {
            return self.persist_after_target(table_name.clone());
        }
        let progress = ExpirationProgress {
            cutoff: self.progress.cutoff,
            position: ExpirationPosition::Start,
        };
        Ok(ExpirationTargetDecision::Persist(self.change(
            progress,
            ExpirationDeletionBatch::default(),
            StepResult::Complete,
        )?))
    }

    fn decide_present_target(
        &self,
        target: ExpirationTarget,
        limit: NonZeroUsize,
    ) -> StorageResult<ExpirationTargetDecision> {
        if self.target_changed(&target) || !target.is_enabled() {
            return self.persist_after_target(target.table_name);
        }
        if target.ttl_attribute.trim().is_empty() {
            return Err(StorageError::internal(
                "persisted expiration policy has an empty attribute",
            ));
        }
        let cutoff_seconds = self.progress.cutoff.timestamp().max(0);
        let cursor = match &self.progress.position {
            ExpirationPosition::Active { cursor, .. } => Some(cursor.clone()),
            ExpirationPosition::Start | ExpirationPosition::After { .. } => None,
        };
        Ok(ExpirationTargetDecision::Read(ExpirationScanRead {
            table_name: target.table_name,
            table_incarnation: target.table_incarnation,
            ttl_attribute: target.ttl_attribute,
            policy_generation: target.policy_generation,
            cutoff_seconds,
            cursor,
            limit,
        }))
    }

    fn target_changed(&self, target: &ExpirationTarget) -> bool {
        matches!(
            &self.progress.position,
            ExpirationPosition::Active { table_name, table_incarnation, policy_generation, .. }
                if table_name != &target.table_name
                    || table_incarnation != &target.table_incarnation
                    || policy_generation != &target.policy_generation
        )
    }

    fn persist_after_target(
        &self,
        table_name: storage_types::TableName,
    ) -> StorageResult<ExpirationTargetDecision> {
        let progress = self.progress_after_target(table_name);
        Ok(ExpirationTargetDecision::Persist(self.change(
            progress,
            ExpirationDeletionBatch::default(),
            StepResult::Advanced(()),
        )?))
    }

    async fn prepare_scan(
        self,
        transaction: &mut impl ExpirationTransaction,
        current_target: &TargetIncarnation,
        read: ExpirationScanRead,
    ) -> StorageResult<ExpirationWrite> {
        let page = transaction.read_expiration_scan(&read).await?;
        match self.decide_expiration_scan(&read, page)? {
            ExpirationScanDecision::Persist(change) => Ok(ExpirationWrite::Progress(
                change.into_progress_write(current_target)?,
            )),
            ExpirationScanDecision::Read(read) => {
                let table_incarnation = read.table_incarnation.clone();
                let table_name = read.table_name.clone();
                let observation = transaction.read_expiration_candidates(&read).await?;
                let change = self.decide_expiration_candidates(read, observation)?;
                Ok(ExpirationWrite::Page(change.into_page_write(
                    current_target,
                    self.progress.cutoff,
                    table_name,
                    table_incarnation,
                )?))
            }
        }
    }

    fn decide_expiration_scan(
        &self,
        read: &ExpirationScanRead,
        page: ExpirationScanPage,
    ) -> StorageResult<ExpirationScanDecision> {
        page.validate_for(read)?;
        let scanned = ExpirationPagePlan::try_new(
            page.rows,
            page.more,
            read.limit,
            read.cutoff_seconds,
            &page.table_info,
        )?;
        if scanned.candidates.is_empty() {
            let progress = self.progress_after_page(
                &read.table_name,
                &read.table_incarnation,
                read.policy_generation,
                scanned.next_cursor,
            );
            return Ok(ExpirationScanDecision::Persist(self.change(
                progress,
                ExpirationDeletionBatch::default(),
                StepResult::Advanced(()),
            )?));
        }
        Ok(ExpirationScanDecision::Read(ExpirationCurrentRead {
            table_name: read.table_name.clone(),
            table_incarnation: read.table_incarnation.clone(),
            table_info: page.table_info,
            policy_generation: read.policy_generation,
            batch: ExpirationCandidateBatch {
                ttl_attribute: read.ttl_attribute.clone(),
                cutoff_seconds: read.cutoff_seconds,
                candidates: scanned.candidates,
            },
            next_cursor: scanned.next_cursor,
        }))
    }

    fn decide_expiration_candidates(
        &self,
        read: ExpirationCurrentRead,
        observation: ExpirationCurrentObservation,
    ) -> StorageResult<ExpirationChange> {
        observation.validate_for(&read)?;
        let deletions = read.batch.plan_deletions(observation.batch)?;
        let progress = self.progress_after_page(
            &read.table_name,
            &read.table_incarnation,
            read.policy_generation,
            read.next_cursor,
        );
        self.change(progress, deletions, StepResult::Advanced(()))
    }

    fn progress_after_page(
        &self,
        table_name: &storage_types::TableName,
        table_incarnation: &TargetIncarnation,
        policy_generation: storage_common::TtlPolicyGeneration,
        next_cursor: Option<storage_common::TtlMaintenanceCursor>,
    ) -> ExpirationProgress {
        match next_cursor {
            Some(cursor) => ExpirationProgress {
                cutoff: self.progress.cutoff,
                position: ExpirationPosition::Active {
                    table_name: table_name.clone(),
                    table_incarnation: table_incarnation.clone(),
                    policy_generation,
                    cursor,
                },
            },
            None => self.progress_after_target(table_name.clone()),
        }
    }

    fn progress_after_target(&self, table_name: storage_types::TableName) -> ExpirationProgress {
        ExpirationProgress {
            cutoff: self.progress.cutoff,
            position: ExpirationPosition::After { table_name },
        }
    }

    fn change(
        &self,
        progress: ExpirationProgress,
        deletions: ExpirationDeletionBatch,
        result: StepResult<()>,
    ) -> StorageResult<ExpirationChange> {
        let revision = self
            .expected_revision
            .unwrap_or(Revision::INITIAL)
            .checked_next()
            .map_err(|_| StorageError::internal("expiration task revision overflow"))?;
        Ok(ExpirationChange {
            expected_revision: self.expected_revision,
            next_progress: TaskProgress {
                authority: self.authority.clone(),
                revision,
                state: progress,
            },
            deletions,
            result,
        })
    }
}

impl ExpirationTarget {
    fn is_enabled(&self) -> bool {
        matches!(
            self.status,
            storage_types::TimeToLiveStatus::Enabled | storage_types::TimeToLiveStatus::Enabling
        )
    }
}

impl ExpirationScanPage {
    fn validate_for(&self, read: &ExpirationScanRead) -> StorageResult<()> {
        if self.table_name != read.table_name
            || self.table_info.info.table_name != read.table_name
            || read.table_incarnation.as_ref() != self.table_info.history_id.as_ref()
            || self.table_incarnation != read.table_incarnation
            || self.policy_generation != read.policy_generation
        {
            return Err(StorageError::internal(
                "expiration scan does not match its requested target",
            ));
        }
        if self.rows.len() > read.limit.get().saturating_add(1) {
            return Err(StorageError::internal(
                "expiration scan exceeded its requested physical page bound",
            ));
        }
        Ok(())
    }
}

impl ExpirationCurrentObservation {
    fn validate_for(&self, read: &ExpirationCurrentRead) -> StorageResult<()> {
        if self.table_name != read.table_name
            || self.table_incarnation != read.table_incarnation
            || self.policy_generation != read.policy_generation
        {
            return Err(StorageError::internal(
                "expiration observation does not match its requested target",
            ));
        }
        Ok(())
    }
}

impl ExpirationChange {
    fn into_progress_write(
        self,
        current_target: &TargetIncarnation,
    ) -> StorageResult<ExpirationProgressWrite> {
        let (write, deletions) = self.into_parts(current_target)?;
        if !deletions.index_entries.is_empty() || !deletions.items.is_empty() {
            return Err(StorageError::internal(
                "expiration progress write unexpectedly contains deletions",
            ));
        }
        Ok(write)
    }

    fn into_page_write(
        self,
        current_target: &TargetIncarnation,
        captured_at: storage_types::TimestampMillis,
        table_name: storage_types::TableName,
        table_incarnation: TargetIncarnation,
    ) -> StorageResult<ExpirationPageWrite> {
        let (progress, deletions) = self.into_parts(current_target)?;
        let progress_matches = matches!(
            &progress.next_progress.state.position,
            ExpirationPosition::Active { table_name: active, .. }
                | ExpirationPosition::After { table_name: active }
                if active == &table_name
        );
        if !progress_matches {
            return Err(StorageError::internal(
                "expiration page write does not match its scanned table",
            ));
        }
        Ok(ExpirationPageWrite {
            progress,
            table_name,
            table_incarnation,
            deletions,
            captured_at,
        })
    }

    fn into_parts(
        self,
        current_target: &TargetIncarnation,
    ) -> StorageResult<(ExpirationProgressWrite, ExpirationDeletionBatch)> {
        if self.next_progress.authority.scope.target != *current_target {
            return Err(StorageError::internal(
                "expiration change targets another storage incarnation",
            ));
        }
        Ok((
            ExpirationProgressWrite {
                current_target: current_target.clone(),
                expected_revision: self.expected_revision,
                next_progress: self.next_progress,
                result: self.result,
            },
            self.deletions,
        ))
    }
}
