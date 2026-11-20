//! Native-call hooks for fault and census tests. Always compiled; a probe
//! exists only when the `fault-injection` feature installs one.

use futures::future::BoxFuture;
/// The commit boundary that `SqlFaultProbe::at_commit_boundary` receives.
pub use storage_common::native_observer::CommitBoundary as SqlCommitBoundary;
use storage_types::{AttemptMode, StorageError, StorageResult};

use crate::SqlQueryPage;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlFaultOperation {
    Query,
    Execute,
}

pub struct SqlFaultCall<'a> {
    pub operation: SqlFaultOperation,
    pub sql: &'a str,
}

/// A SQL probe seen as the commit observer of every attempt of its store.
#[cfg(feature = "fault-injection")]
pub(crate) struct SqlProbeObserver(pub(crate) std::sync::Arc<dyn SqlFaultProbe>);

#[cfg(feature = "fault-injection")]
impl storage_common::native_observer::NativeObserver for SqlProbeObserver {
    fn at_commit_boundary(
        &self,
        boundary: storage_common::native_observer::CommitBoundary,
        mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        self.0.at_commit_boundary(boundary, mode)
    }
}

/// The observer that `with_fault_probe` stores beside the probe.
#[cfg(feature = "fault-injection")]
pub(crate) fn commit_observer(
    probe: &std::sync::Arc<dyn SqlFaultProbe>,
) -> std::sync::Arc<dyn storage_common::native_observer::NativeObserver> {
    std::sync::Arc::new(SqlProbeObserver(std::sync::Arc::clone(probe)))
}

/// One native event of a SQL attempt, reported to `SqlFaultProbe::observe`.
/// Tests count these events for one customer operation (the wave census).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SqlNativeEvent {
    Begin,
    Commit,
    Rollback,
    /// One round trip (Postgres, Turso) or worker hop (SQLite). `statements`
    /// is the count sent in it; `cold_prepares` counts statements whose plan
    /// was not yet cached on the connection (each adds a Parse round trip).
    Wave {
        statements: usize,
        cold_prepares: usize,
    },
    /// One statement result; `rows` is the rows returned by a query or the
    /// rows changed by an execute.
    Statement {
        operation: SqlFaultOperation,
        rows: u64,
    },
    /// Actual rows changed by counted DML after its validated reply was
    /// drained. Includes attempted changes that a later statement may roll
    /// back.
    RowsWritten {
        rows: u64,
    },
    DurabilityBarrier,
    /// SQLite WAL frames (pages) that one commit appended.
    WalFrames {
        frames: u64,
    },
}

/// One statement of a wave as the driver sends it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqlWaveStatement<'a> {
    pub sql: &'a str,
    /// The row-lock clause that the driver appends to `sql` (Postgres read
    /// protection), or empty.
    pub lock_suffix: &'static str,
}

impl<'a> SqlWaveStatement<'a> {
    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    pub(crate) fn plain(sql: &'a str) -> Self {
        Self {
            sql,
            lock_suffix: "",
        }
    }
}

/// One wave that the driver reports to the probe: the probe can hold it
/// before it is sent, and observes it when it completes.
#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) struct SqlWave<'probe> {
    probe: &'probe dyn SqlFaultProbe,
    statements: usize,
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
impl<'probe> SqlWave<'probe> {
    /// Waits for `before_wave`, then starts the wave.
    pub(crate) async fn start(
        probe: Option<&'probe dyn SqlFaultProbe>,
        statements: &[SqlWaveStatement<'_>],
    ) -> StorageResult<Option<Self>> {
        let Some(probe) = probe else {
            return Ok(None);
        };
        storage_common::writer_occupancy::exempt_wait(probe.before_wave(statements)).await?;
        Ok(Some(Self {
            probe,
            statements: statements.len(),
        }))
    }

    /// A wave of statements that the driver sends without a lock suffix.
    pub(crate) async fn start_plain(
        probe: Option<&'probe dyn SqlFaultProbe>,
        sql: &[&str],
    ) -> StorageResult<Option<Self>> {
        if probe.is_none() {
            return Ok(None);
        }
        let statements = sql
            .iter()
            .map(|sql| SqlWaveStatement::plain(sql))
            .collect::<Vec<_>>();
        Self::start(probe, &statements).await
    }

    pub(crate) fn finish(self, cold_prepares: usize) {
        self.probe.observe(SqlNativeEvent::Wave {
            statements: self.statements,
            cold_prepares,
        });
    }
}

/// Reports one settlement event (`Begin`, `Commit`, `Rollback`) that is
/// sent as its own wave.
#[cfg(any(feature = "postgres", feature = "turso"))]
pub(crate) async fn settlement_wave<T, E: From<StorageError>>(
    probe: Option<&dyn SqlFaultProbe>,
    sql: &str,
    event: SqlNativeEvent,
    native: impl Future<Output = Result<T, E>>,
) -> Result<T, E> {
    let Some(probe) = probe else {
        return native.await;
    };
    let wave = SqlWave::start(Some(probe), &[SqlWaveStatement::plain(sql)]).await?;
    let result = native.await;
    end_wave(wave);
    if result.is_ok() {
        probe.observe(event);
    }
    result
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) fn end_wave(wave: Option<SqlWave<'_>>) {
    if let Some(wave) = wave {
        wave.finish(0);
    }
}

#[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
pub(crate) fn observe_statement(
    probe: Option<&dyn SqlFaultProbe>,
    operation: SqlFaultOperation,
    rows: u64,
) {
    if let Some(probe) = probe {
        probe.observe(SqlNativeEvent::Statement { operation, rows });
    }
}

pub trait SqlFaultProbe: Send + Sync {
    /// Runs synchronously before the actual PostgreSQL connection future and
    /// socket are dropped.
    fn before_postgres_connection_cleanup(&self) {}

    /// Runs on SQLite's blocking sync thread immediately before syncing the
    /// WAL.
    fn before_wal_sync(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn fail(&self, call: SqlFaultCall<'_>) -> Option<StorageError>;

    /// Postgres counted writes only: called after the server answered the
    /// statement with a valid count row. `Some` turns the result into a
    /// client-side (`Local`) failure, as an undecodable reply would be (FIN-c).
    #[cfg(feature = "fault-injection")]
    fn fail_counted_result(&self, _sql: &str) -> Option<StorageError> {
        None
    }

    /// Intercept a hydrated primary image without coupling fault fixtures to
    /// the native descriptor or immutable-body encoding.
    fn alter_item_image(
        &self,
        _protection: crate::ReadProtection,
        item: storage_types::WireItem,
    ) -> storage_types::WireItem {
        item
    }

    /// Holds the commit of a read or write attempt (`mode`) at `boundary`.
    fn at_commit_boundary(
        &self,
        _boundary: SqlCommitBoundary,
        _mode: AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    fn alter_query_page(&self, _call: SqlFaultCall<'_>, page: SqlQueryPage) -> SqlQueryPage {
        page
    }

    /// Receives every native event of every attempt of the store.
    fn observe(&self, _event: SqlNativeEvent) {}

    /// Runs once before a wave is sent. The whole wave waits, so no statement
    /// of the wave can overtake another one.
    fn before_wave<'a>(
        &'a self,
        _statements: &'a [SqlWaveStatement<'a>],
    ) -> BoxFuture<'a, StorageResult<()>> {
        Box::pin(async { Ok(()) })
    }

    /// `true` makes each Postgres attempt flush its backend statistics when
    /// it ends (`pg_stat_force_next_flush` in the `BEGIN` round trip), so
    /// that another session reads the WAL of the attempt at once.
    fn flush_native_statistics(&self) -> bool {
        false
    }
}
