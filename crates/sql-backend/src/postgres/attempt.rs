use std::num::NonZeroUsize;
#[cfg(feature = "fault-injection")]
use std::sync::Arc;

use deadpool_postgres::Client;
use futures::future::join_all;
use storage_common::diagnostics::NativeOperation;
use storage_types::{NativeAttemptSettlement, StorageError, StorageResult};

use super::{
    driver::PostgresTransaction,
    error::{collect_pipeline, map_commit_error, map_driver_error, unknown_settlement},
    statements::NativeStatements,
};
use crate::{
    ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement, constants::POSTGRES_PIPELINE_STATEMENTS,
};

/// One checked-out Postgres client; construction is limited to its backend.
pub struct PostgresAttempt {
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
    client: Option<Client>,
    mode: PostgresTransaction,
    settlement: NativeAttemptSettlement,
    disposition: ConnectionDisposition,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

enum ConnectionDisposition {
    Unsettled,
    Reusable,
}

impl PostgresAttempt {
    pub(super) fn new(
        client: Client,
        execution: (PostgresTransaction, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            ownership: Default::default(),
            client: Some(client),
            mode,
            settlement: NativeAttemptSettlement::new(limits),
            disposition: ConnectionDisposition::Unsettled,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }

    fn client(&self) -> StorageResult<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| StorageError::internal("Postgres attempt client is missing"))
    }

    pub(super) async fn begin(&mut self) -> StorageResult<()> {
        self.client()?
            .batch_execute(self.mode.begin_sql())
            .await
            .map_err(map_driver_error)?;
        Ok(())
    }

    pub(super) async fn commit(&mut self) -> StorageResult<()> {
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
        self.client()?
            .batch_execute("COMMIT")
            .await
            .map_err(map_commit_error)?;
        self.disposition = ConnectionDisposition::Reusable;
        #[cfg(feature = "fault-injection")]
        crate::SqlCommitBoundary::AfterDurableAcknowledgement
            .observe(self.fault_probe.as_deref())
            .await?;
        Ok(())
    }

    pub(super) async fn rollback(&mut self) -> StorageResult<()> {
        self.client()?
            .batch_execute("ROLLBACK")
            .await
            .map_err(|_| unknown_settlement("Postgres rollback was not acknowledged"))?;
        self.disposition = ConnectionDisposition::Reusable;
        Ok(())
    }
}

impl Drop for PostgresAttempt {
    fn drop(&mut self) {
        if let Some(client) = self.client.take() {
            if matches!(self.disposition, ConnectionDisposition::Reusable) {
                drop(client);
            } else {
                drop(deadpool_postgres::Object::take(client));
            }
        }
    }
}

impl SqlAttempt for PostgresAttempt {
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
            protection.validate(self.mode.capability())?;
            super::input_limits::require_statement_input(&statement, protection)?;
            NativeStatements::new(
                self.client()?,
                &self.settlement,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            )
            .query(statement, protection, max_bytes)
            .await
            .map_err(|error| error.report(NativeOperation::SqlQuery))
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
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_calls(statements.len())?;
            for (statement, _) in &statements {
                statement.charge_work(&self.settlement)?;
            }
            for (statement, protection) in &statements {
                protection.validate(self.mode.capability())?;
                super::input_limits::require_statement_input(statement, *protection)?;
            }
            let native = NativeStatements::new(
                self.client()?,
                &self.settlement,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            );
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut statements = statements.into_iter();
            loop {
                let mut batch = Vec::new();
                batch
                    .try_reserve_exact(statements.len().min(POSTGRES_PIPELINE_STATEMENTS))
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                batch.extend(statements.by_ref().take(POSTGRES_PIPELINE_STATEMENTS));
                if batch.is_empty() {
                    break;
                }
                pages.extend(native.query_window(batch, max_bytes).await?);
            }
            Ok(pages)
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
            self.mode.capability().require_write()?;
            super::input_limits::require_statement_input(&statement, ReadProtection::Snapshot)?;
            NativeStatements::new(
                self.client()?,
                &self.settlement,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            )
            .execute(statement)
            .await
            .map_err(|error| error.report(NativeOperation::SqlExecute))
        }
        .await;
        let result = result.and_then(|count| {
            self.settlement.charge_data(std::mem::size_of::<u64>())?;
            Ok(count)
        });
        call.complete(result)
    }

    async fn execute_batch(&mut self, statements: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_calls(statements.len())?;
            for statement in &statements {
                statement.charge_work(&self.settlement)?;
            }
            self.mode.capability().require_write()?;
            for statement in &statements {
                super::input_limits::require_statement_input(statement, ReadProtection::Snapshot)?;
            }
            let native = NativeStatements::new(
                self.client()?,
                &self.settlement,
                #[cfg(feature = "fault-injection")]
                self.fault_probe.as_deref(),
            );
            let mut changed = Vec::new();
            changed
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
            let mut statements = statements.into_iter();
            loop {
                let mut batch = Vec::new();
                batch
                    .try_reserve_exact(statements.len().min(POSTGRES_PIPELINE_STATEMENTS))
                    .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
                batch.extend(statements.by_ref().take(POSTGRES_PIPELINE_STATEMENTS));
                if batch.is_empty() {
                    break;
                }
                changed.extend(collect_pipeline(
                    join_all(batch.into_iter().map(|statement| native.execute(statement))).await,
                    NativeOperation::SqlExecute,
                )?);
            }
            Ok(changed)
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
