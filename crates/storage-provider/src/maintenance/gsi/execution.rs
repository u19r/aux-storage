use storage_backfill::{GsiBuild, GsiChange};
use storage_common::TtlConfigActivation;
use storage_types::{
    Revision, StorageError, StorageResult,
    maintenance::{StepResult, TaskProgress},
};

use super::{GsiBuildAdvance, GsiBuildTransaction, GsiTransition, models::GsiBuildEvaluation};

impl GsiBuildEvaluation {
    pub(super) async fn advance(
        mut self,
        transaction: &mut impl GsiBuildTransaction,
    ) -> StorageResult<GsiBuildAdvance> {
        let mut candidates = transaction
            .read_gsi_build_candidates(self.after.as_ref())
            .await?;
        self.after = candidates.scanned_through.take();
        let Some(build) = candidates.schedule() else {
            transaction
                .write_gsi_progress(self.expected_revision, self.next_progress()?)
                .await?;
            return Ok(GsiBuildAdvance {
                // A skipped target advances the durable cursor but has no
                // runnable work. Use the normal cadence instead of busy
                // looping.
                result: StepResult::Complete,
                ttl_activation: None,
            });
        };
        let change = Box::pin(transaction.evaluate_gsi_build(&build, self.page_limit)).await?;
        // Keep each large generic future behind its own allocation. Turso's
        // concrete transaction future otherwise exceeds the test-thread stack.
        Box::pin(self.persist(transaction, build, change)).await
    }

    async fn persist<Transaction>(
        self,
        transaction: &mut Transaction,
        expected: GsiBuild<Transaction::Cursor>,
        change: GsiChange<Transaction::Cursor>,
    ) -> StorageResult<GsiBuildAdvance>
    where
        Transaction: GsiBuildTransaction,
    {
        match change {
            GsiChange::Persist {
                projections,
                next_state,
            } => {
                let ttl_activation = self
                    .ttl_activation(transaction, &expected, &next_state)
                    .await?;
                Box::pin(transaction.write_gsi_transition(GsiTransition {
                    expected_revision: self.expected_revision,
                    next_progress: self.next_progress()?,
                    expected,
                    projections,
                    next_state,
                    ttl_activation: ttl_activation.as_ref(),
                }))
                .await?;
                Ok(GsiBuildAdvance {
                    result: StepResult::Advanced(()),
                    ttl_activation,
                })
            }
            GsiChange::Superseded => {
                transaction
                    .write_gsi_progress(self.expected_revision, self.next_progress()?)
                    .await?;
                Ok(GsiBuildAdvance {
                    result: StepResult::Superseded,
                    ttl_activation: None,
                })
            }
            GsiChange::Idle => {
                transaction
                    .write_gsi_progress(self.expected_revision, self.next_progress()?)
                    .await?;
                Ok(GsiBuildAdvance {
                    result: StepResult::Complete,
                    ttl_activation: None,
                })
            }
        }
    }

    async fn ttl_activation<Transaction>(
        &self,
        transaction: &mut Transaction,
        expected: &GsiBuild<Transaction::Cursor>,
        next_state: &storage_backfill::GsiState<Transaction::Cursor>,
    ) -> StorageResult<Option<TtlConfigActivation>>
    where
        Transaction: GsiBuildTransaction,
    {
        if !expected.target.index_name.is_ttl_index()
            || !matches!(expected.state, storage_backfill::GsiState::CatchingUp(_))
            || !matches!(next_state, storage_backfill::GsiState::Active(_))
        {
            return Ok(None);
        }
        let config = transaction
            .read_gsi_ttl_config(&expected.target.table_name)
            .await?
            .ok_or_else(|| {
                StorageError::internal("TTL index activation is missing its policy record")
            })?;
        config
            .activate_for_index(
                expected.target.table_name.clone(),
                &expected.target.index_name,
                self.captured_at,
            )
            .map(Some)
    }

    fn next_progress(
        &self,
    ) -> StorageResult<TaskProgress<Option<storage_backfill::GsiBuildTarget>>> {
        let revision = self
            .expected_revision
            .unwrap_or(Revision::INITIAL)
            .checked_next()
            .map_err(|_| StorageError::internal("GSI build task revision overflow"))?;
        Ok(TaskProgress {
            authority: self.authority.clone(),
            revision,
            state: self.after.clone(),
        })
    }
}
