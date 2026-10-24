use std::num::NonZeroUsize;
#[cfg(feature = "fault-injection")]
use std::sync::Arc;

use storage_types::StorageResult;
use tokio::sync::OwnedMutexGuard;

use super::{
    error::{map_commit_error, map_driver_error, unknown_settlement},
    statements,
};
use crate::{AttemptMode, ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement};

/// One exclusive connection lease and its native transaction.
pub struct TursoAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
    connection: OwnedMutexGuard<turso::Connection>,
    mode: AttemptMode,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl TursoAttempt {
    pub(super) fn new(
        connection: OwnedMutexGuard<turso::Connection>,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            ownership: Default::default(),
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            connection,
            mode,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }

    async fn restore_clean(&self) -> StorageResult<()> {
        if !self.connection.is_autocommit().map_err(map_driver_error)? {
            self.connection
                .execute("ROLLBACK", ())
                .await
                .map_err(map_driver_error)?;
        }
        Ok(())
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
        self.connection
            .execute(sql, ())
            .await
            .map_err(map_driver_error)?;
        Ok(())
    }

    pub(super) async fn commit(&self) -> StorageResult<()> {
        if let Err(error) = self.require_settlement() {
            self.rollback().await?;
            return Err(error);
        }
        #[cfg(feature = "fault-injection")]
        if let Err(error) = crate::SqlCommitBoundary::BeforeCommit
            .observe(self.fault_probe.as_deref())
            .await
        {
            self.rollback().await?;
            return Err(error);
        }
        match self.connection.execute("COMMIT", ()).await {
            Ok(_) => {
                #[cfg(feature = "fault-injection")]
                crate::SqlCommitBoundary::AfterDurableAcknowledgement
                    .observe(self.fault_probe.as_deref())
                    .await?;
                Ok(())
            }
            Err(error) => {
                let classified = map_commit_error(error);
                self.restore_clean().await.map_err(|_| {
                    unknown_settlement("Turso commit and rollback were not acknowledged")
                })?;
                Err(classified)
            }
        }
    }

    pub(super) async fn rollback(&self) -> StorageResult<()> {
        self.restore_clean()
            .await
            .map_err(|_| unknown_settlement("Turso rollback was not acknowledged"))
    }
}

impl SqlAttempt for TursoAttempt {
    #[cfg(feature = "fault-injection")]
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
    fn ownership_changes(
        &mut self,
    ) -> &mut stream_provider::retained_image::RetainedOwnershipChanges {
        &mut self.ownership
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
                #[cfg(feature = "fault-injection")]
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
                #[cfg(feature = "fault-injection")]
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
            statements::execute(
                &self.connection,
                statement,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            )
            .await
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
            statements::execute_batch(
                &self.connection,
                writes,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            )
            .await
        }
        .await;
        let result = result.and_then(|counts| {
            let bytes = counts
                .len()
                .checked_mul(std::mem::size_of::<u64>())
                .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            self.settlement.charge_data(bytes)?;
            Ok(counts)
        });
        call.complete(result)
    }
}
