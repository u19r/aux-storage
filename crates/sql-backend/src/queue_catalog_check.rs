//! A SQL attempt that reads a hinted queue catalog row in the same batch as
//! the first message read (W7 P7a).

use std::{future::Future, num::NonZeroUsize};

use queue_provider::QueueCatalogRecord;
use storage_types::{ReadProtection, StorageError, StorageResult};

use crate::{
    SqlAttempt, SqlCountedWrite, SqlLayout, SqlOpenReadBatchResult, SqlQueryPage,
    SqlReadBatchResult, SqlStatement, SqlWriteBatchFailure, constants::QUEUE_CATALOG_READ_BYTES,
    queue_catalog,
};

/// What the adapter knows about the hinted row after a call.
pub(crate) enum CatalogCheck {
    /// No hinted row in this attempt.
    None,
    /// Not read yet.
    Pending(QueueCatalogRecord),
    /// Read in a batch; `current` is the row at the attempt's read version.
    Read {
        expected: QueueCatalogRecord,
        current: Option<QueueCatalogRecord>,
    },
}

pub(crate) struct CheckedSqlAttempt<'a, A, L> {
    inner: &'a mut A,
    engine: L,
    check: CatalogCheck,
}

impl<'a, A: SqlAttempt, L: SqlLayout> CheckedSqlAttempt<'a, A, L> {
    pub(crate) fn new(inner: &'a mut A, engine: L, pending: Option<QueueCatalogRecord>) -> Self {
        Self {
            inner,
            engine,
            check: pending.map_or(CatalogCheck::None, CatalogCheck::Pending),
        }
    }

    pub(crate) fn finish(self) -> CatalogCheck {
        self.check
    }

    async fn read_with_check(
        &mut self,
        mut statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<storage_types::Exact<SqlQueryPage>> {
        let CatalogCheck::Pending(expected) =
            std::mem::replace(&mut self.check, CatalogCheck::None)
        else {
            return self
                .inner
                .query_batch_protected_exact(statements, max_bytes)
                .await;
        };
        let requested = statements.len();
        statements.insert(
            0,
            (
                queue_catalog::queue_by_name_statement(self.engine, &expected.queue.queue_name),
                ReadProtection::Snapshot,
            ),
        );
        let mut pages = match self
            .inner
            .query_batch_protected_exact(statements, max_bytes.max(QUEUE_CATALOG_READ_BYTES))
            .await
        {
            Ok(pages) => pages.into_iter(),
            Err(error) => {
                self.check = CatalogCheck::Pending(expected);
                return Err(error);
            }
        };
        // The check statement answers first; the rest answer the caller's.
        let check_page = pages.next();
        let rest = storage_types::Exact::try_new(requested, pages.collect())?;
        let current = check_page
            .map(|page| queue_catalog::decode_named_page(page, &expected.queue.queue_name))
            .transpose()?
            .flatten();
        self.check = CatalogCheck::Read { expected, current };
        Ok(rest)
    }
}

impl<A: SqlAttempt, L: SqlLayout> SqlAttempt for CheckedSqlAttempt<'_, A, L> {
    fn fault_probe(&self) -> Option<&dyn crate::SqlFaultProbe> {
        self.inner.fault_probe()
    }

    fn reject_settlement(&mut self) {
        self.inner.reject_settlement();
    }

    fn reject_settlement_error(&mut self, error: &StorageError) {
        self.inner.reject_settlement_error(error);
    }

    fn require_settlement(&self) -> StorageResult<()> {
        self.inner.require_settlement()
    }

    fn finish_read_batch(
        &mut self,
        observation: SqlStatement,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<SqlReadBatchResult>> + Send {
        self.inner
            .finish_read_batch(observation, statements, max_bytes)
    }

    fn query_batch_open(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<SqlOpenReadBatchResult>> + Send {
        self.inner.query_batch_open(statements, max_bytes)
    }

    fn finish_write_batch(
        &mut self,
        statements: Vec<SqlCountedWrite>,
        max_bytes: NonZeroUsize,
    ) -> impl Future<Output = StorageResult<Result<Vec<SqlQueryPage>, SqlWriteBatchFailure>>> + Send
    {
        self.inner.finish_write_batch(statements, max_bytes)
    }

    async fn query(
        &mut self,
        statement: SqlStatement,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        if !matches!(self.check, CatalogCheck::Pending(_)) {
            return self.inner.query(statement, protection, max_bytes).await;
        }
        let pages = self
            .read_with_check(vec![(statement, protection)], max_bytes)
            .await?;
        let [page] = pages.into_array::<1>()?;
        Ok(page)
    }

    fn execute(
        &mut self,
        statement: SqlStatement,
    ) -> impl Future<Output = StorageResult<u64>> + Send {
        self.inner.execute(statement)
    }

    fn execute_batch(
        &mut self,
        statements: Vec<SqlStatement>,
    ) -> impl Future<Output = StorageResult<Vec<u64>>> + Send {
        self.inner.execute_batch(statements)
    }

    async fn query_batch_protected(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        Ok(self
            .read_with_check(statements, max_bytes)
            .await?
            .into_vec())
    }
}
