use std::{num::NonZeroUsize, sync::Arc};

use storage_common::{
    native_attempt_lifecycle::AttemptLifecycle,
    native_observer::{CommitBoundary, observe_commit_boundary},
    native_outcome::{NativeCommitError, classify_commit},
};
use storage_types::{AttemptDeadline, AttemptPhase, StorageError, StorageResult};
use tokio::sync::OwnedMutexGuard;

use super::{
    error::{commit_error, map_driver_error},
    statements,
};
use crate::{AttemptMode, ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement};

/// One exclusive connection lease and its native transaction.
pub struct TursoAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    connection: OwnedMutexGuard<turso::Connection>,
    mode: AttemptMode,
    /// `None` in production.
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl TursoAttempt {
    pub(super) fn new(
        connection: OwnedMutexGuard<turso::Connection>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            connection,
            mode,
            fault_probe,
        }
    }

    async fn restore_clean(&self) -> StorageResult<()> {
        if !self.connection.is_autocommit().map_err(map_driver_error)? {
            let native = async {
                self.connection
                    .execute("ROLLBACK", ())
                    .await
                    .map_err(map_driver_error)
            };
            let native = crate::fault_injection::settlement_wave(
                self.fault_probe.as_deref(),
                "ROLLBACK",
                crate::SqlNativeEvent::Rollback,
                native,
            );
            native.await?;
        }
        Ok(())
    }

    /// FIN-b: a busy BEGIN on this connection waits at most until `deadline`.
    pub(super) fn bound_busy_wait(&self, deadline: &AttemptDeadline) -> StorageResult<()> {
        let busy = crate::backend::local_busy_wait(deadline)?;
        self.connection.busy_timeout(busy).map_err(map_driver_error)
    }

    pub(super) async fn begin(&self) -> StorageResult<()> {
        self.restore_clean().await?;
        // The shared sequence authority serializes write commits. Acquire the
        // native writer lease before reading its snapshot rather than retrying
        // optimistic attempts which all update the same sequence row.
        let sql = match self.mode {
            AttemptMode::ReadOnly => "BEGIN",
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => "BEGIN IMMEDIATE",
        };
        let native = async {
            self.connection
                .execute(sql, ())
                .await
                .map_err(map_driver_error)
        };
        let native = crate::fault_injection::settlement_wave(
            self.fault_probe.as_deref(),
            sql,
            crate::SqlNativeEvent::Begin,
            native,
        );
        native.await?;
        Ok(())
    }

    pub(super) async fn commit(
        &self,
        deadline: AttemptDeadline,
        lifecycle: &AttemptLifecycle,
    ) -> StorageResult<()> {
        if let Err(error) = self.require_settlement() {
            self.rollback().await?;
            return Err(error);
        }
        if let Some(observer) = lifecycle.observer()
            && let Err(error) =
                observe_commit_boundary(observer, CommitBoundary::BeforeCommit, self.mode).await
        {
            self.rollback().await?;
            return Err(error);
        }
        // The one commit-submission point. A read commits nothing and keeps
        // only the Settle deadline check.
        let ticket = if self.mode == AttemptMode::ReadOnly {
            deadline
                .require_remaining(AttemptPhase::Settle)
                .map(|()| None)
        } else {
            lifecycle.submit_commit(&deadline).map(Some)
        };
        let ticket = match ticket {
            Ok(ticket) => ticket,
            Err(error) => {
                self.rollback().await?;
                return Err(error);
            }
        };
        let native = async {
            self.connection
                .execute("COMMIT", ())
                .await
                .map_err(commit_error)
        };
        let native = crate::fault_injection::settlement_wave(
            self.fault_probe.as_deref(),
            "COMMIT",
            crate::SqlNativeEvent::Commit,
            native,
        );
        let answer = match native.await {
            Ok(_) => Ok(()),
            Err(classified) => match self.restore_clean().await {
                Ok(()) => Err(classified),
                Err(_) => Err(NativeCommitError::Unproven {
                    reason: "Turso commit and rollback were not acknowledged",
                    cause: None,
                }),
            },
        };
        match ticket {
            Some(ticket) => ticket.settle(answer),
            None => answer.map_err(|error| classify_commit(false, error)),
        }?;
        if let Some(observer) = lifecycle.observer() {
            observe_commit_boundary(
                observer,
                CommitBoundary::AfterDurableAcknowledgement,
                self.mode,
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn rollback(&self) -> StorageResult<()> {
        self.restore_clean()
            .await
            .map_err(|_| StorageError::commit_unknown("Turso rollback was not acknowledged"))
    }
}

impl SqlAttempt for TursoAttempt {
    fn fault_probe(&self) -> Option<&dyn crate::SqlFaultProbe> {
        self.fault_probe.as_deref()
    }

    fn reject_settlement(&mut self) {
        self.settlement.reject();
    }
    fn reject_settlement_error(&mut self, error: &storage_types::StorageError) {
        self.settlement.reject_error(error);
    }
    fn require_settlement(&self) -> StorageResult<()> {
        self.settlement.require_open()
    }

    async fn query(
        &mut self,
        statement: SqlStatement,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        let call = self.settlement.begin_call()?;
        let result = async {
            statement.charge_work(&self.settlement)?;
            protection.validate(self.mode)?;
            statements::query(
                &self.connection,
                statement,
                max_bytes,
                self.fault_probe.as_deref(),
            )
            .await
        }
        .await;
        let result = result.and_then(|page| {
            page.charge_work(&self.settlement)?;
            Ok(page)
        });
        call.complete(result)
    }

    async fn query_batch_protected(
        &mut self,
        queries: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_calls(queries.len())?;
            for (statement, _) in &queries {
                statement.charge_work(&self.settlement)?;
            }
            for (_, protection) in &queries {
                protection.validate(self.mode)?;
            }
            statements::query_batch(
                &self.connection,
                queries,
                max_bytes,
                self.fault_probe.as_deref(),
            )
            .await
        }
        .await;
        let result = result.and_then(|pages| {
            for page in &pages {
                page.charge_work(&self.settlement)?;
            }
            Ok(pages)
        });
        call.complete(result)
    }

    async fn execute(&mut self, statement: SqlStatement) -> StorageResult<u64> {
        let call = self.settlement.begin_call()?;
        let result = async {
            statement.charge_work(&self.settlement)?;
            self.mode.require_write()?;
            statements::execute(&self.connection, statement, self.fault_probe.as_deref()).await
        }
        .await;
        let result = result.and_then(|count| {
            self.settlement.charge_data(std::mem::size_of::<u64>())?;
            Ok(count)
        });
        call.complete(result)
    }

    async fn execute_batch(&mut self, writes: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_calls(writes.len())?;
            for statement in &writes {
                statement.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            statements::execute_batch(&self.connection, writes, self.fault_probe.as_deref()).await
        }
        .await;
        let result = result.and_then(|counts| {
            let bytes = counts
                .len()
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or(storage_types::AttemptFailure::CAPACITY)?;
            self.settlement.charge_data(bytes)?;
            Ok(counts)
        });
        call.complete(result)
    }
}
