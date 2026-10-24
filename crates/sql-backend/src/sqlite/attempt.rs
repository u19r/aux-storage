use std::num::NonZeroUsize;
#[cfg(feature = "fault-injection")]
use std::sync::Arc;

use storage_types::StorageResult;

use super::{statements::NativeStatements, transaction::Session};
use crate::{AttemptMode, ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement};

/// A transaction-scoped SQLite statement executor; construction is private.
pub struct SqliteAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    ownership: stream_provider::retained_image::RetainedOwnershipChanges,
    session: Session,
    mode: AttemptMode,
    #[cfg(feature = "fault-injection")]
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl SqliteAttempt {
    pub(super) fn new(
        session: Session,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        #[cfg(feature = "fault-injection")] fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            ownership: Default::default(),
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            session,
            mode,
            #[cfg(feature = "fault-injection")]
            fault_probe,
        }
    }
}

impl SqlAttempt for SqliteAttempt {
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
            for (_, protection) in &statements {
                protection.validate(self.mode)?;
            }
            #[cfg(feature = "fault-injection")]
            let fault_probe = self.fault_probe.clone();
            self.session
                .call(move |connection| {
                    let native = NativeStatements::new(
                        connection,
                        #[cfg(feature = "fault-injection")]
                        fault_probe.as_deref(),
                    );
                    native.query_batch(statements, max_bytes)
                })
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

    async fn execute_batch(&mut self, statements: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        let call = self.settlement.begin_call()?;
        let result = async {
            self.settlement.charge_calls(statements.len())?;
            for statement in &statements {
                statement.charge_work(&self.settlement)?;
            }
            self.mode.require_write()?;
            #[cfg(feature = "fault-injection")]
            let fault_probe = self.fault_probe.clone();
            self.session
                .call(move |connection| {
                    let native = NativeStatements::new(
                        connection,
                        #[cfg(feature = "fault-injection")]
                        fault_probe.as_deref(),
                    );
                    native.execute_batch(statements)
                })
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
            #[cfg(feature = "fault-injection")]
            let fault_probe = self.fault_probe.clone();
            self.session
                .call(move |connection| {
                    NativeStatements::new(
                        connection,
                        #[cfg(feature = "fault-injection")]
                        fault_probe.as_deref(),
                    )
                    .query(statement, max_bytes)
                })
                .await
        }
        .await;
        let result = result.and_then(|page| {
            page.charge_work(&self.settlement)?;
            Ok(page)
        });
        call.complete(result)
    }

    async fn execute(&mut self, statement: SqlStatement) -> StorageResult<u64> {
        let call = self.settlement.begin_call()?;
        let result = async {
            statement.charge_work(&self.settlement)?;
            self.mode.require_write()?;
            #[cfg(feature = "fault-injection")]
            let fault_probe = self.fault_probe.clone();
            self.session
                .call(move |connection| {
                    NativeStatements::new(
                        connection,
                        #[cfg(feature = "fault-injection")]
                        fault_probe.as_deref(),
                    )
                    .execute(statement)
                })
                .await
        }
        .await;
        let result = result.and_then(|count| {
            self.settlement.charge_data(std::mem::size_of::<u64>())?;
            Ok(count)
        });
        call.complete(result)
    }
}
