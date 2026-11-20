use std::{num::NonZeroUsize, sync::Arc};

use storage_types::StorageResult;

use super::{statements::NativeStatements, transaction::Session};
use crate::{AttemptMode, ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement};

/// A transaction-scoped SQLite statement executor; construction is private.
pub struct SqliteAttempt {
    settlement: storage_types::NativeAttemptSettlement,
    session: Session,
    mode: AttemptMode,
    // Any execute call or query that is not read-only registers this attempt
    // as a pending durable writer before its commit, even if it fails.
    durable_write: DurableWriteSlot,
    /// `None` in production.
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

impl SqliteAttempt {
    pub(super) fn new(
        session: Session,
        execution: (AttemptMode, Option<storage_types::NativeWorkLimits>),
        durable_write: DurableWriteSlot,
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            settlement: storage_types::NativeAttemptSettlement::new(limits),
            session,
            mode,
            durable_write,
            fault_probe,
        }
    }

    fn record_write(&self, wrote: bool) {
        if wrote {
            self.durable_write.register();
        }
    }
}

/// The attempt's registration as a pending durable writer. The driver takes
/// it after the native commit or failure and settles durability with it.
#[derive(Clone)]
pub(super) struct DurableWriteSlot {
    batch: Option<std::sync::Arc<storage_common::durability::DurabilityBatch>>,
    write:
        std::sync::Arc<std::sync::Mutex<Option<storage_common::durability::PendingDurableWrite>>>,
}

impl DurableWriteSlot {
    pub(super) fn new(
        batch: Option<std::sync::Arc<storage_common::durability::DurabilityBatch>>,
    ) -> Self {
        Self {
            batch,
            write: std::sync::Arc::default(),
        }
    }

    fn register(&self) {
        let Some(batch) = &self.batch else {
            return;
        };
        let mut write = self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if write.is_none() {
            *write = Some(batch.begin_write());
        }
    }

    pub(super) fn is_registered(&self) -> bool {
        self.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some()
    }

    /// On the writer thread, right after the native commit returned success.
    pub(super) fn record_commit(&self) {
        if let Some(write) = self
            .write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
        {
            write.record_commit();
        }
    }

    pub(super) fn take(&self) -> Option<storage_common::durability::PendingDurableWrite> {
        self.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

impl SqlAttempt for SqliteAttempt {
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
            let writes = self.mode != AttemptMode::ReadOnly;
            let fault_probe = self.fault_probe.clone();
            let wave = match self.fault_probe.as_deref() {
                Some(probe) => {
                    let sql = statements
                        .iter()
                        .map(|(statement, _)| statement.sql())
                        .collect::<Vec<_>>();
                    crate::fault_injection::SqlWave::start_plain(Some(probe), &sql).await?
                }
                None => None,
            };
            let (pages, wrote) = self
                .session
                .call(move |connection| {
                    let native = NativeStatements::new(connection, writes, fault_probe.as_deref());
                    Ok((native.query_batch(statements, max_bytes), native.wrote()))
                })
                .await?;
            crate::fault_injection::end_wave(wave);
            self.record_write(wrote);
            pages
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
            self.record_write(!statements.is_empty());
            let fault_probe = self.fault_probe.clone();
            let wave = match self.fault_probe.as_deref() {
                Some(probe) => {
                    let sql = statements.iter().map(SqlStatement::sql).collect::<Vec<_>>();
                    crate::fault_injection::SqlWave::start_plain(Some(probe), &sql).await?
                }
                None => None,
            };
            let counts = self
                .session
                .call(move |connection| {
                    let native = NativeStatements::new(connection, true, fault_probe.as_deref());
                    native.execute_batch(statements)
                })
                .await;
            crate::fault_injection::end_wave(wave);
            counts
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
            let writes = self.mode != AttemptMode::ReadOnly;
            let fault_probe = self.fault_probe.clone();
            let wave = crate::fault_injection::SqlWave::start_plain(
                self.fault_probe.as_deref(),
                &[statement.sql()],
            )
            .await?;
            let (page, wrote) = self
                .session
                .call(move |connection| {
                    let native = NativeStatements::new(connection, writes, fault_probe.as_deref());
                    Ok((native.query(statement, max_bytes), native.wrote()))
                })
                .await?;
            crate::fault_injection::end_wave(wave);
            self.record_write(wrote);
            page
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
            self.record_write(true);
            let fault_probe = self.fault_probe.clone();
            let wave = crate::fault_injection::SqlWave::start_plain(
                self.fault_probe.as_deref(),
                &[statement.sql()],
            )
            .await?;
            let count = self
                .session
                .call(move |connection| {
                    NativeStatements::new(connection, true, fault_probe.as_deref())
                        .execute(statement)
                })
                .await;
            crate::fault_injection::end_wave(wave);
            count
        }
        .await;
        let result = result.and_then(|count| {
            self.settlement.charge_data(std::mem::size_of::<u64>())?;
            Ok(count)
        });
        call.complete(result)
    }
}
