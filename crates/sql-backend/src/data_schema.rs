//! Initialize one physical SQL data address space with the canonical family
//! schemas.
//!
//! Each DDL statement runs in its own short transaction (`SqlStore::
//! initialize_schema`): the Postgres stream frontier (`pg_snapshot_xmin`)
//! waits for the oldest running transaction ID of the whole server, not only
//! of this database, so one long schema transaction stops every stream reader.
//! All statements are idempotent.

use std::num::NonZeroUsize;

use storage_types::{ReadProtection, StorageError, StorageResult};

use crate::{
    SqlAttempt, SqlLayout, SqlQueryPage, SqlStatement, catalog::initialize_catalog,
    gsi::initialize_gsi, maintenance::initialize_maintenance,
    pubsub_publication::initialize_pubsub_publications,
    pubsub_subscription::initialize_pubsub_subscriptions, pubsub_topic::initialize_pubsub_topics,
    queue_catalog::initialize_queue_catalog, queue_message::initialize_queue_messages,
    retention::initialize_retention, stream_catalog::initialize_stream_catalog,
    stream_cursor::initialize_stream_cursors, stream_history::initialize_system_stream,
    stream_trim::initialize_trim_receipts, stream_write::initialize_stream_rows,
    table_reclaim::initialize_table_reclaim, transact_write::initialize_transact_receipts,
};

/// The DDL of every family, in dependency order. The family initializers
/// render into a collector; none of them reads.
pub(crate) async fn schema_statements(
    engine: crate::SqlEngine,
    regional: bool,
) -> StorageResult<Vec<String>> {
    let mut ddl = DdlCollector::default();
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    crate::job_store::initialize(&mut ddl, engine).await?;
    initialize_catalog(&mut ddl, engine).await?;
    initialize_table_reclaim(&mut ddl, engine).await?;
    initialize_queue_catalog(&mut ddl, engine).await?;
    initialize_queue_messages(&mut ddl, engine).await?;
    initialize_pubsub_topics(&mut ddl, engine).await?;
    initialize_pubsub_subscriptions(&mut ddl, engine).await?;
    initialize_pubsub_publications(&mut ddl, engine).await?;
    crate::pubsub_schedule::initialize_pubsub_schedule(&mut ddl, engine).await?;
    initialize_maintenance(&mut ddl, engine).await?;
    initialize_stream_rows(&mut ddl, engine).await?;
    initialize_system_stream(&mut ddl, engine).await?;
    initialize_stream_catalog(&mut ddl, engine).await?;
    initialize_stream_cursors(&mut ddl, engine).await?;
    initialize_trim_receipts(&mut ddl, engine).await?;
    initialize_retention(&mut ddl, engine).await?;
    initialize_gsi(&mut ddl, engine).await?;
    if regional {
        crate::regional::initialize_regional_markers(&mut ddl, engine).await?;
        crate::regional::initialize_regional_progress(&mut ddl, engine).await?;
    }
    initialize_transact_receipts(&mut ddl, engine).await?;
    Ok(ddl.0)
}

/// Seed rows that the schema needs after its DDL: the maintenance identity
/// and the system stream authority. One short transaction.
pub(crate) async fn seed_data_schema<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    initial_stream_lineages: storage_types::InitialStreamLineages,
) -> StorageResult<()> {
    crate::maintenance::seed_maintenance(attempt, engine).await?;
    crate::stream_history::seed_system_stream(attempt, engine, initial_stream_lineages).await
}

/// Records DDL statements in order. A read is a defect of the initializer.
#[derive(Default)]
struct DdlCollector(Vec<String>);

impl DdlCollector {
    fn push(&mut self, statement: SqlStatement) -> StorageResult<()> {
        let (sql, binds) = statement.into_parts();
        if !binds.is_empty() {
            return Err(StorageError::internal("SQL schema DDL has bind values"));
        }
        self.0.push(sql.into_owned());
        Ok(())
    }
}

fn read_error() -> StorageError {
    StorageError::internal("SQL schema DDL step reads")
}

impl SqlAttempt for DdlCollector {
    fn reject_settlement(&mut self) {}

    fn reject_settlement_error(&mut self, _error: &StorageError) {}

    fn require_settlement(&self) -> StorageResult<()> {
        Ok(())
    }

    async fn query(
        &mut self,
        _statement: SqlStatement,
        _protection: ReadProtection,
        _max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        Err(read_error())
    }

    async fn execute(&mut self, statement: SqlStatement) -> StorageResult<u64> {
        self.push(statement)?;
        Ok(0)
    }

    async fn execute_batch(&mut self, statements: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        let count = statements.len();
        for statement in statements {
            self.push(statement)?;
        }
        Ok(vec![0; count])
    }

    async fn query_batch_protected(
        &mut self,
        _statements: Vec<(SqlStatement, ReadProtection)>,
        _max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        Err(read_error())
    }
}
