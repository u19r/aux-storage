use std::{fmt::Write as _, num::NonZeroUsize, sync::Arc};

use futures::{
    TryStreamExt as _,
    future::{BoxFuture, join_all},
};
use storage_common::{
    diagnostics::NativeOperation,
    native_attempt_lifecycle::AttemptLifecycle,
    native_observer::{CommitBoundary, observe_commit_boundary},
    native_outcome::{NativeCommitError, classify_commit},
};
use storage_types::{
    AttemptPhase, NativeAttemptSettlement, NativeCallPhase, NativeCause, NativeCode, NativeEngine,
    StorageError, StorageResult, expired_in,
};

use super::{
    driver::PostgresTransaction,
    error::{collect_pipeline, commit_error, map_driver_error, pipeline_failure_index},
    pool::Client,
    statements::{NativeStatements, PreparedQuery},
};
use crate::{
    ReadProtection, SqlAttempt, SqlQueryPage, SqlStatement, constants::POSTGRES_PIPELINE_STATEMENTS,
};

/// One checked-out Postgres client; construction is limited to its backend.
pub struct PostgresAttempt {
    client: Option<Client>,
    mode: PostgresTransaction,
    settlement: NativeAttemptSettlement,
    state: NativeState,
    lifecycle: Arc<AttemptLifecycle>,
    durability: storage_types::AttemptDurability,
    deadline: storage_types::AttemptDeadline,
    /// `None` in production.
    fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
}

/// Where the one native transaction of an attempt stands. With the commit
/// gate and the shared work settlement, the only settlement record.
enum NativeState {
    /// No transaction is open: nothing was sent yet (lazy BEGIN) or it ended
    /// cleanly.
    Idle,
    /// The wave that carries BEGIN was written; COMMIT or ROLLBACK will end it.
    Open,
    /// A terminal batch is in flight; a dropped one leaves this state.
    InFlight,
    /// The server answered COMMIT with success.
    Committed,
    /// Ended without commit, or with the outcome that `cause` reports.
    /// `clean`: the server acknowledged the end; the connection is reusable.
    Ended {
        cause: Option<StorageError>,
        clean: bool,
    },
}

impl PostgresAttempt {
    pub(super) fn new(
        client: Client,
        execution: (PostgresTransaction, Option<storage_types::NativeWorkLimits>),
        durability: storage_types::AttemptDurability,
        deadline: storage_types::AttemptDeadline,
        lifecycle: Arc<AttemptLifecycle>,
        fault_probe: Option<Arc<dyn crate::SqlFaultProbe>>,
    ) -> Self {
        let (mode, limits) = execution;
        Self {
            client: Some(client),
            mode,
            settlement: NativeAttemptSettlement::new(limits),
            state: NativeState::Idle,
            lifecycle,
            durability,
            deadline,
            fault_probe,
        }
    }

    fn client(&self) -> StorageResult<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| StorageError::internal("Postgres attempt client is missing"))
    }

    fn is_write(&self) -> bool {
        self.attempt_mode() != storage_types::AttemptMode::ReadOnly
    }

    fn attempt_mode(&self) -> storage_types::AttemptMode {
        self.mode.capability()
    }

    fn statements(&self) -> StorageResult<NativeStatements<'_>> {
        Ok(NativeStatements::new(
            self.client()?,
            &self.settlement,
            self.fault_probe.as_deref(),
        ))
    }

    /// The BEGIN batch, written first in the attempt's first wave (lazy BEGIN,
    /// see `wave_begin`). The server bounds every
    /// statement and the whole transaction by the remaining budget, so it ends
    /// work that the client stopped waiting for; the commit grace keeps it
    /// from ending a COMMIT that the client still waits for.
    ///
    /// A Relaxed attempt turns off the commit flush for this transaction
    /// only. Its commit is visible before it is on disk; only queue and
    /// delivery rows may be relaxed (decision D7), and DynamoDB reads do not
    /// read them, so a Durable DynamoDB read still observes only durable data.
    /// Durable attempts keep the server default (`synchronous_commit = on`).
    ///
    /// Rule for every caller that selects Relaxed: a Durable read must not
    /// depend on rows that Relaxed attempts write (receive, delete, and the
    /// visibility state of standard queues), unless it first forces a WAL
    /// flush; otherwise a crash can remove data that the read returned.
    fn begin_statement(&self) -> StorageResult<String> {
        let statement_ms = self.deadline.remaining().as_millis();
        if statement_ms == 0 {
            return Err(expired_in(AttemptPhase::Begin));
        }
        let transaction_ms = statement_ms.saturating_add(self.deadline.commit_grace.as_millis());
        let mut begin = String::with_capacity(192);
        let _ = write!(
            begin,
            "{}; SET LOCAL statement_timeout = {statement_ms}; SET LOCAL transaction_timeout = \
             {transaction_ms}",
            self.mode.begin_sql()
        );
        if self.durability == storage_types::AttemptDurability::Relaxed {
            begin.push_str("; SET LOCAL synchronous_commit TO OFF");
        }
        if self
            .fault_probe
            .as_deref()
            .is_some_and(crate::SqlFaultProbe::flush_native_statistics)
        {
            begin.push_str("; SELECT pg_stat_force_next_flush()");
        }
        Ok(begin)
    }

    /// The BEGIN batch that the next wave writes first: `Some` only while no
    /// transaction is open. Built directly before the wave is written, so the
    /// server timers are the budget left then and end the transaction at the
    /// attempt deadline plus the commit grace. Past the deadline nothing is
    /// written: a known `Deadline(Begin)`, and the state stays `Idle`.
    fn wave_begin(&self) -> StorageResult<Option<String>> {
        if !matches!(self.state, NativeState::Idle) {
            return Ok(None);
        }
        // BEGIN is one admitted native call, as when it was sent alone.
        self.settlement.charge_calls(1)?;
        self.begin_statement().map(Some)
    }

    /// The first branch of every wave's `futures::join!`. `join!` polls it
    /// first and tokio-postgres writes a request on its first poll, so the
    /// BEGIN batch precedes the wave's statements on the wire and on the
    /// server. When it fails, the wave's statements fail with 25P02 or the same
    /// lost connection; callers report this error, not theirs.
    async fn send_wave_begin(&self, begin: Option<&str>) -> StorageResult<()> {
        let Some(begin) = begin else { return Ok(()) };
        attached(&self.client)?
            .batch_execute(begin)
            .await
            .map_err(|error| map_driver_error(&error))?;
        if let Some(probe) = self.fault_probe.as_deref() {
            probe.observe(crate::SqlNativeEvent::Begin);
        }
        Ok(())
    }

    /// The census statements of a wave: the BEGIN first while none was sent.
    fn wave_statements<'a>(
        &self,
        statements: impl IntoIterator<Item = crate::SqlWaveStatement<'a>>,
    ) -> Vec<crate::SqlWaveStatement<'a>> {
        let begin = matches!(self.state, NativeState::Idle)
            .then(|| crate::SqlWaveStatement::plain(self.mode.begin_sql()));
        begin.into_iter().chain(statements).collect()
    }

    /// One wave of prepared reads behind the lazy BEGIN.
    async fn read_wave(
        &self,
        begin: Option<&str>,
        prepared: Vec<PreparedQuery>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Vec<SqlQueryPage>> {
        let native = self.statements()?;
        let (begun, results) = futures::join!(
            self.send_wave_begin(begin),
            join_all(
                prepared
                    .into_iter()
                    .map(|statement| native.query_prepared(statement, max_bytes))
            ),
        );
        begun?;
        collect_pipeline(results, NativeOperation::SqlQuery)
    }

    /// One wave of prepared writes behind the lazy BEGIN.
    async fn execute_wave(
        &self,
        begin: Option<&str>,
        prepared: Vec<PreparedQuery>,
    ) -> StorageResult<Vec<u64>> {
        let native = self.statements()?;
        let (begun, results) = futures::join!(
            self.send_wave_begin(begin),
            join_all(
                prepared
                    .iter()
                    .map(|statement| native.execute_prepared(statement))
            ),
        );
        begun?;
        collect_pipeline(results, NativeOperation::SqlExecute)
    }

    /// Calls are refused after a terminal batch: unknown when it was dropped
    /// after COMMIT was sent, a known failure otherwise.
    fn require_usable(&self) -> StorageResult<()> {
        let error = match self.state {
            NativeState::Idle | NativeState::Open => return Ok(()),
            NativeState::InFlight => missing_answer(
                &self.lifecycle,
                StorageError::internal("Postgres terminal batch was abandoned before its answer"),
            ),
            NativeState::Committed | NativeState::Ended { .. } => {
                StorageError::internal("Postgres attempt already finished")
            }
        };
        self.settlement.reject_error(&error);
        Err(error)
    }

    /// Ends the transaction without COMMIT. No COMMIT was sent, so this is a
    /// known abort whatever the reply; an unanswered ROLLBACK, or one past
    /// the deadline, leaves the connection to be dropped.
    async fn abort(&mut self, cause: Option<StorageError>) {
        let clean = match self.state {
            NativeState::Committed | NativeState::Ended { .. } => return,
            NativeState::Idle => true,
            NativeState::InFlight => false,
            NativeState::Open => {
                !self.deadline.remaining().is_zero() && self.send_rollback().await.is_ok()
            }
        };
        self.state = NativeState::Ended { cause, clean };
    }

    async fn abort_with(&mut self, error: &StorageError) {
        self.settlement.reject_error(error);
        self.abort(Some(error.clone())).await;
    }

    async fn send_rollback(&self) -> StorageResult<()> {
        let client = self.client()?;
        let native = async {
            client
                .batch_execute("ROLLBACK")
                .await
                .map_err(|error| map_driver_error(&error))
        };
        let native = crate::fault_injection::settlement_wave(
            self.fault_probe.as_deref(),
            "ROLLBACK",
            crate::SqlNativeEvent::Rollback,
            native,
        );
        native.await
    }

    /// A terminal batch failed without an answer that ends the transaction.
    fn fail_terminal(&mut self, error: StorageError) -> StorageError {
        self.settlement.reject_error(&error);
        self.state = NativeState::Ended {
            cause: None,
            clean: false,
        };
        error
    }

    /// A COMMIT that the server answered with a conflict ended the transaction:
    /// the connection stays usable.
    fn fail_answered_commit(&mut self, error: StorageError) -> StorageError {
        let clean = error.attempt_kind() == Some(storage_types::AttemptKind::Conflict);
        self.settlement.reject_error(&error);
        self.state = NativeState::Ended { cause: None, clean };
        error
    }

    async fn abort_open_batch(
        &mut self,
        pages: Vec<SqlQueryPage>,
        statement_index: usize,
        error: super::error::PostgresCallError,
    ) -> StorageResult<crate::SqlOpenReadBatchResult> {
        let is_native = error.is_native();
        let error = error.into_read_error();
        self.abort_with(&error).await;
        if !is_native {
            return Err(error);
        }
        Ok(crate::SqlOpenReadBatchResult {
            pages,
            failure: Some(crate::SqlReadBatchFailure {
                statement_index,
                error,
            }),
        })
    }

    /// A failed final read window. `open`: the transaction is still open and
    /// needs a ROLLBACK; otherwise it never began or COMMIT ended it.
    async fn end_read_batch(
        &mut self,
        observation: Option<SqlQueryPage>,
        statement_index: usize,
        error: super::error::PostgresCallError,
        open: bool,
    ) -> StorageResult<crate::SqlReadBatchResult> {
        let is_native = error.is_native();
        let error = error.into_read_error();
        self.state = if open {
            NativeState::Open
        } else {
            NativeState::Idle
        };
        if !is_native {
            self.abort(None).await;
            return Err(error);
        }
        self.abort(Some(error.clone())).await;
        Ok(crate::SqlReadBatchResult {
            observation,
            reads: Err(crate::SqlReadBatchFailure {
                statement_index,
                error,
            }),
        })
    }

    pub(super) fn has_committed_write(&self) -> bool {
        self.is_write() && matches!(self.state, NativeState::Committed)
    }

    fn take_finished(&mut self) -> Option<Option<StorageError>> {
        match &mut self.state {
            NativeState::Committed => Some(None),
            NativeState::Ended { cause, .. } => Some(cause.take()),
            _ => None,
        }
    }

    fn is_reusable(&self) -> bool {
        matches!(
            self.state,
            NativeState::Idle | NativeState::Committed | NativeState::Ended { clean: true, .. }
        ) && self
            .client
            .as_ref()
            .is_some_and(|client| !client.has_warning())
            && !self
                .settlement
                .require_open()
                .err()
                .is_some_and(|error| error.is_commit_unknown())
    }

    /// The healthy end: a settled connection returns to the pool.
    pub(super) async fn release_client(&mut self) {
        if self.is_reusable() {
            drop(self.client.take());
        } else {
            self.discard_client().await;
        }
    }

    pub(super) async fn discard_client(&mut self) {
        if let Some(client) = self.client.take() {
            Client::take(client).discard().await;
        }
    }

    /// The result of an attempt whose callback the deadline timer stopped.
    pub(super) fn expired_error(&self) -> StorageError {
        let error = if self.has_committed_write() {
            StorageError::internal("Postgres callback did not finish after confirmed commit")
        } else if self.is_write() {
            missing_answer(&self.lifecycle, expired_in(AttemptPhase::Statement))
        } else {
            expired_in(AttemptPhase::Statement)
        };
        error.preserve_native_settlement(self.require_settlement())
    }

    /// The callback, then COMMIT or ROLLBACK unless a terminal batch ended
    /// the transaction. The deadline timer of `run_attempt` races this.
    pub(super) async fn run<R, F>(&mut self, operation: F) -> StorageResult<R>
    where F: for<'a> FnOnce(&'a mut PostgresAttempt) -> BoxFuture<'a, StorageResult<R>> {
        let result = operation(self).await;
        if matches!(self.state, NativeState::InFlight) {
            let error = self.require_usable().expect_err("abandoned terminal batch");
            self.state = NativeState::Ended {
                cause: None,
                clean: false,
            };
            return Err(error.preserve_native_settlement(self.require_settlement()));
        }
        match result {
            Ok(value) => match self.take_finished() {
                // The transaction ended with a cause: the value is discarded.
                Some(Some(error)) => {
                    Err(error.preserve_native_settlement(self.require_settlement()))
                }
                Some(None) => {
                    self.require_settlement()?;
                    Ok(value)
                }
                None => {
                    self.commit().await?;
                    Ok(value)
                }
            },
            Err(error) => {
                self.abort(None).await;
                Err(error.preserve_native_settlement(self.require_settlement()))
            }
        }
    }

    async fn commit(&mut self) -> StorageResult<()> {
        if let Err(error) = self.require_settlement() {
            self.abort(None).await;
            return Err(error);
        }
        if matches!(self.state, NativeState::Idle) {
            // Lazy BEGIN: nothing was sent, so no transaction exists to commit.
            self.state = NativeState::Committed;
            return Ok(());
        }
        if let Some(observer) = self.lifecycle.observer()
            && let Err(error) =
                observe_commit_boundary(observer, CommitBoundary::BeforeCommit, self.attempt_mode())
                    .await
        {
            self.abort(None).await;
            return Err(error);
        }
        let wave =
            crate::fault_injection::SqlWave::start_plain(self.fault_probe.as_deref(), &["COMMIT"])
                .await?;
        // The one commit-submission point. A read commits nothing: no ticket.
        let lifecycle = Arc::clone(&self.lifecycle);
        let ticket = if self.is_write() {
            match lifecycle.submit_commit(&self.deadline) {
                Ok(ticket) => Some(ticket),
                Err(error) => return Err(self.fail_terminal(error)),
            }
        } else {
            None
        };
        let answer = self.client()?.batch_execute("COMMIT").await;
        crate::fault_injection::end_wave(wave);
        if let Some(ticket) = ticket {
            let answer = ticket.settle(answer.map_err(|error| commit_error(&error)));
            if let Err(error) = answer.and_then(|()| qualify_commit(self.client()?)) {
                return Err(self.fail_answered_commit(error));
            }
        } else if let Err(error) = answer {
            return Err(self.fail_terminal(map_driver_error(&error)));
        }
        self.state = NativeState::Committed;
        if let Some(probe) = self.fault_probe.as_deref() {
            probe.observe(crate::SqlNativeEvent::Commit);
        }
        self.after_commit_boundary().await?;
        Ok(())
    }

    /// The observer boundary after a COMMIT answer. A withheld write answer is
    /// unknown; a read commits nothing, so its withheld answer is a known
    /// failure (`observe_commit_boundary` classifies by mode).
    async fn after_commit_boundary(&self) -> StorageResult<()> {
        let Some(observer) = self.lifecycle.observer() else {
            return Ok(());
        };
        observe_commit_boundary(
            observer,
            CommitBoundary::AfterDurableAcknowledgement,
            self.attempt_mode(),
        )
        .await
    }
}

/// A COMMIT answer that carries a warning (SQLSTATE 01000, a cancelled
/// synchronous replication wait) commits locally only: unknown outcome.
fn qualify_commit(client: &Client) -> StorageResult<()> {
    if client.has_warning() {
        return Err(StorageError::commit_unknown(
            "Postgres commit was answered with a warning",
        ));
    }
    Ok(())
}

/// A failure that the caller observed: unknown once COMMIT was sent and not
/// answered, the known `before` otherwise.
fn missing_answer(lifecycle: &AttemptLifecycle, before: StorageError) -> StorageError {
    if lifecycle.awaits_answer() {
        StorageError::commit_unknown("Postgres commit was not answered")
    } else {
        before
    }
}

/// The attempt's client through a field borrow, so that `self.state` can be
/// assigned while a wave or a settlement call borrows other fields.
fn attached(client: &Option<Client>) -> StorageResult<&Client> {
    client
        .as_ref()
        .ok_or_else(|| StorageError::internal("Postgres attempt client is missing"))
}

/// Starts the census of one wave: `None` without a probe. `statements` lists
/// the wave for the probe and runs only with one, so an attempt without a
/// probe builds no list. Borrows fields, not the attempt, so the caller can
/// still assign `self.state` while the wave is open.
async fn census_wave<'a, 's>(
    probe: Option<&'a dyn crate::SqlFaultProbe>,
    client: &'a Option<Client>,
    statements: impl FnOnce() -> Vec<crate::SqlWaveStatement<'s>>,
) -> StorageResult<Option<PostgresWave<'a>>> {
    let Some(probe) = probe else {
        return Ok(None);
    };
    PostgresWave::start(Some(probe), attached(client)?, &statements())
        .await
        .map(Some)
}

/// One reported Postgres wave. A statement whose plan was not cached on the
/// client adds a Parse round trip; it is reported as a cold prepare.
struct PostgresWave<'a> {
    wave: Option<crate::fault_injection::SqlWave<'a>>,
    client: &'a Client,
    cached: usize,
}

impl<'a> PostgresWave<'a> {
    async fn start(
        probe: Option<&'a dyn crate::SqlFaultProbe>,
        client: &'a Client,
        statements: &[crate::SqlWaveStatement<'_>],
    ) -> StorageResult<Self> {
        let wave = crate::fault_injection::SqlWave::start(probe, statements).await?;
        Ok(Self {
            wave,
            client,
            cached: client.statement_cache.size(),
        })
    }

    /// Reports a wave that `census_wave` started; nothing without a probe.
    fn end(wave: Option<Self>) {
        if let Some(wave) = wave {
            wave.finish();
        }
    }

    fn finish(self) {
        if let Some(wave) = self.wave {
            wave.finish(
                self.client
                    .statement_cache
                    .size()
                    .saturating_sub(self.cached),
            );
        }
    }
}

impl Drop for PostgresAttempt {
    fn drop(&mut self) {
        let reusable = self.is_reusable();
        if let Some(client) = self.client.take()
            && !reusable
        {
            drop(Client::take(client));
        }
    }
}

impl SqlAttempt for PostgresAttempt {
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

    /// A ReadOnly terminal batch: BEGIN, reads and COMMIT in one pipeline. It
    /// commits nothing, so it does not use the commit gate and never reports
    /// an unknown outcome.
    async fn finish_read_batch(
        &mut self,
        observation: SqlStatement,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<crate::SqlReadBatchResult> {
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let input = (|| {
            if self.mode.capability() != storage_types::AttemptMode::ReadOnly {
                return Err(StorageError::internal("final read requires ReadOnly"));
            }
            let mut input = Vec::new();
            input
                .try_reserve_exact(statements.len().saturating_add(1))
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            input.push((observation, ReadProtection::Snapshot));
            input.extend(statements);
            self.settlement.charge_calls(input.len())?;
            for (statement, protection) in &input {
                if *protection != ReadProtection::Snapshot {
                    return Err(StorageError::internal(
                        "final read requires Snapshot protection",
                    ));
                }
                statement.charge_work(&self.settlement)?;
                super::input_limits::require_statement_input(statement, *protection)?;
            }
            Ok(input)
        })();
        let input = call.complete(input)?;
        let expected_reads = input.len().saturating_sub(1);
        let mut needs_begin = matches!(self.state, NativeState::Idle);
        self.state = NativeState::InFlight;
        let mut input = input.into_iter().peekable();
        let mut observation = None;
        let mut pages = Vec::new();
        let mut offset = 0;
        loop {
            let batch = input
                .by_ref()
                .take(POSTGRES_PIPELINE_STATEMENTS)
                .collect::<Vec<_>>();
            let last = input.peek().is_none();
            let wave = census_wave(self.fault_probe.as_deref(), &self.client, || {
                let mut sql = Vec::with_capacity(batch.len() + 2);
                if needs_begin {
                    sql.push(crate::SqlWaveStatement::plain(self.mode.begin_sql()));
                }
                sql.extend(
                    batch
                        .iter()
                        .map(|(statement, _)| crate::SqlWaveStatement::plain(statement.sql())),
                );
                if last {
                    sql.push(crate::SqlWaveStatement::plain("COMMIT"));
                }
                sql
            })
            .await?;
            // Parse every statement before dispatch: observations cannot
            // overtake locks.
            let mut prepared = self.statements()?.prepare_window(batch).await;
            let preparation_failure = pipeline_failure_index(&prepared).and_then(|index| {
                prepared
                    .remove(index)
                    .err()
                    .map(|error| (offset + index, error))
            });
            let ready = prepared
                .into_iter()
                .filter_map(Result::ok)
                .collect::<Vec<_>>();
            if let Some((statement_index, error)) = preparation_failure {
                PostgresWave::end(wave);
                return self
                    .end_read_batch(observation, statement_index, error, !needs_begin)
                    .await;
            }
            if last
                && let Some(observer) = self.lifecycle.observer()
                && let Err(error) = observe_commit_boundary(
                    observer,
                    CommitBoundary::BeforeCommit,
                    self.attempt_mode(),
                )
                .await
            {
                PostgresWave::end(wave);
                self.settlement.reject_error(&error);
                self.state = if needs_begin {
                    NativeState::Idle
                } else {
                    NativeState::Open
                };
                self.abort(None).await;
                return Err(error);
            }
            let begin = match needs_begin.then(|| self.begin_statement()).transpose() {
                Ok(begin) => begin,
                Err(error) => {
                    PostgresWave::end(wave);
                    return Err(self.fail_terminal(error));
                }
            };
            let native = self.statements()?;
            // join! polls these branches in order and drains every response,
            // even on error.
            let (begun, mut results, commit) = futures::join!(
                self.send_wave_begin(begin.as_deref()),
                join_all(
                    ready
                        .into_iter()
                        .map(|statement| native.query_prepared(statement, max_bytes))
                ),
                async {
                    if last {
                        self.client()?
                            .batch_execute("COMMIT")
                            .await
                            .map_err(|error| map_driver_error(&error))
                    } else {
                        Ok(())
                    }
                },
            );
            PostgresWave::end(wave);
            if let Err(error) = begun.and(commit) {
                return Err(self.fail_terminal(error));
            }
            if last {
                if let Some(probe) = self.fault_probe.as_deref() {
                    probe.observe(crate::SqlNativeEvent::Commit);
                }
                if let Err(error) = self.after_commit_boundary().await {
                    return Err(self.fail_terminal(error));
                }
            }
            let selected_failure = pipeline_failure_index(&results).and_then(|index| {
                std::mem::replace(&mut results[index], Ok(SqlQueryPage::new()))
                    .err()
                    .map(|error| (offset + index, error))
            });
            for (index, result) in results.into_iter().enumerate() {
                if let Ok(page) = result {
                    if offset + index == 0 {
                        // The selected failure was replaced only to retain the
                        // other completed pages.
                        if !matches!(&selected_failure, Some((0, _))) {
                            observation = Some(page);
                        }
                    } else {
                        pages.push(page);
                    }
                }
            }
            if let Some((statement_index, error)) = selected_failure {
                return self
                    .end_read_batch(observation, statement_index, error, !last)
                    .await;
            }
            let charged = (|| {
                if let Some(page) = observation.as_ref().filter(|_| offset == 0) {
                    page.charge_work(&self.settlement)?;
                }
                for page in &pages[offset.saturating_sub(1)..] {
                    page.charge_work(&self.settlement)?;
                }
                Ok::<_, StorageError>(())
            })();
            if let Err(error) = charged {
                self.state = if last {
                    NativeState::Committed
                } else {
                    NativeState::Open
                };
                self.abort(None).await;
                return Err(error);
            }
            if last {
                self.state = NativeState::Committed;
                self.settlement.require_open()?;
                return Ok(crate::SqlReadBatchResult {
                    observation,
                    reads: Ok(storage_types::Exact::try_new(expected_reads, pages)?),
                });
            }
            self.state = NativeState::InFlight;
            needs_begin = false;
            offset += POSTGRES_PIPELINE_STATEMENTS;
        }
    }

    async fn finish_write_batch(
        &mut self,
        statements: Vec<crate::SqlCountedWrite>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<Result<Vec<SqlQueryPage>, crate::SqlWriteBatchFailure>> {
        self.mode
            .capability()
            .require_write()
            .inspect_err(|error| self.settlement.reject_error(error))?;
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            if max_bytes.get() < 16 {
                return Err(storage_types::AttemptFailure::CAPACITY.into());
            }
            self.settlement.charge_calls(statements.len())?;
            let mut input = Vec::new();
            let mut factories = Vec::new();
            let mut buffers = Vec::new();
            let mut pages = Vec::new();
            factories
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            input
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            buffers
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            pages
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            for counted in statements {
                let (statement, factory) = counted.into_parts();
                statement.charge_work(&self.settlement)?;
                super::input_limits::require_statement_input(&statement, ReadProtection::Snapshot)?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(2)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                let mut page = SqlQueryPage::new();
                page.rows
                    .try_reserve_exact(1)
                    .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
                // The only result is two integers and one fixed row. Charge it
                // before DML, rather than discovering capacity after COMMIT.
                self.settlement.charge_data(
                    16 + 2 * std::mem::size_of::<crate::SqlValue>()
                        + std::mem::size_of::<crate::SqlRow>(),
                )?;
                input.push((statement, ReadProtection::Snapshot));
                factories.push(factory);
                buffers.push((page, values));
            }
            Ok((input, factories, buffers, pages))
        })();
        let (input, factories, buffers, mut pages) = call.complete(admitted)?;
        let mut input = input.into_iter();
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(factories.len())
            .map_err(|_| StorageError::from(storage_types::AttemptFailure::CAPACITY))
            .inspect_err(|error| self.settlement.reject_error(error))?;
        // All checked binds and result buffers exist before the first mutation.
        // Preparation remains bounded, including when more than one window is
        // needed.
        loop {
            let batch = input
                .by_ref()
                .take(POSTGRES_PIPELINE_STATEMENTS)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                break;
            }
            let call = self.settlement.begin_call()?;
            let native = self.statements()?;
            let mut results = native.prepare_window(batch).await;
            if let Some(index) = pipeline_failure_index(&results)
                && let Err(error) = results.remove(index)
            {
                let statement_index = prepared.len() + index;
                let is_native = error.is_native();
                let error = error.report(NativeOperation::SqlQuery);
                drop(call);
                self.abort_with(&error).await;
                return if is_native {
                    Ok(Err(crate::SqlWriteBatchFailure::Other {
                        statement_index,
                        error,
                    }))
                } else {
                    Err(error)
                };
            }
            let checked = results
                .into_iter()
                .filter_map(Result::ok)
                .map(|statement| native.check_counted(statement))
                .collect::<StorageResult<Vec<_>>>();
            prepared.extend(call.complete(checked)?);
        }
        let mut buffers = buffers.into_iter();
        let mut offset = 0;
        let mut needs_begin = matches!(self.state, NativeState::Idle);
        // Empty accepted writes still settle the native attempt exactly once.
        loop {
            let end = (offset + POSTGRES_PIPELINE_STATEMENTS).min(prepared.len());
            let last = end == prepared.len();
            let call = self.settlement.begin_call()?;
            let wave = match census_wave(self.fault_probe.as_deref(), &self.client, || {
                let mut sql = Vec::with_capacity(end - offset + 2);
                if needs_begin {
                    sql.push(crate::SqlWaveStatement::plain(self.mode.begin_sql()));
                }
                sql.extend(
                    prepared[offset..end]
                        .iter()
                        .map(|statement| crate::SqlWaveStatement::plain(statement.sql())),
                );
                if last {
                    sql.push(crate::SqlWaveStatement::plain("COMMIT"));
                }
                sql
            })
            .await
            {
                Ok(wave) => wave,
                Err(error) => return call.complete(Err(error)),
            };
            if last
                && let Some(observer) = self.lifecycle.observer()
                && let Err(error) = observe_commit_boundary(
                    observer,
                    CommitBoundary::BeforeCommit,
                    self.attempt_mode(),
                )
                .await
            {
                PostgresWave::end(wave);
                drop(call);
                self.abort_with(&error).await;
                return Err(error);
            }
            let lifecycle = Arc::clone(&self.lifecycle);
            let submitted = needs_begin
                .then(|| self.begin_statement())
                .transpose()
                .and_then(|begin| {
                    if last {
                        // The one commit-submission point: this wave carries
                        // COMMIT.
                        lifecycle
                            .submit_commit(&self.deadline)
                            .map(|ticket| (begin, Some(ticket)))
                    } else {
                        Ok((begin, None))
                    }
                });
            let (begin, ticket) = match submitted {
                Ok(submitted) => submitted,
                Err(error) => {
                    PostgresWave::end(wave);
                    drop(call);
                    return Err(self.fail_terminal(error));
                }
            };
            self.state = NativeState::InFlight;
            let native = self.statements()?;
            let (begun, mut results, commit) = futures::join!(
                self.send_wave_begin(begin.as_deref()),
                join_all(
                    prepared[offset..end]
                        .iter()
                        .zip(&factories[offset..end])
                        .map(|(statement, factory)| native.query_counted(statement, *factory))
                ),
                async {
                    if last {
                        let stream = self
                            .client()
                            .map_err(NativeCommitError::from)?
                            .simple_query_raw("COMMIT")
                            .await
                            .map_err(|error| commit_error(&error))?;
                        futures::pin_mut!(stream);
                        let mut completed = false;
                        while let Some(message) = stream
                            .try_next()
                            .await
                            .map_err(|error| commit_error(&error))?
                        {
                            completed |= matches!(
                                message,
                                tokio_postgres::SimpleQueryMessage::CommandComplete(_)
                            );
                        }
                        if completed {
                            Ok(Some(()))
                        } else {
                            Err(NativeCommitError::Unproven {
                                reason: "Postgres commit returned no completion",
                                cause: None,
                            })
                        }
                    } else {
                        Ok::<_, NativeCommitError>(None)
                    }
                },
            );
            PostgresWave::end(wave);
            if let Err(error) = begun {
                drop(call);
                // A failed BEGIN reply cannot show how a sent COMMIT ended.
                return Err(self.fail_terminal(missing_answer(&self.lifecycle, error)));
            }
            // tokio-postgres hides the COMMIT tag. A server error reply in
            // this wave aborted the transaction before COMMIT ran, so COMMIT
            // could only roll back, whether or not its own reply arrived: a
            // known abort, settled as not committed.
            let rolled_back = last
                && !matches!(commit, Ok(None))
                && results.iter().any(|result| {
                    result.as_ref().is_err_and(|error| {
                        error.is_server_error()
                            || matches!(error, super::error::PostgresCallError::CountAssertion(_))
                    })
                });
            let commit = if rolled_back {
                Err(NativeCommitError::NotCommitted(NativeCause {
                    engine: NativeEngine::Postgres,
                    code: NativeCode::Named("ROLLBACK"),
                    phase: NativeCallPhase::Commit,
                }))
            } else {
                commit
            };
            let commit = match ticket {
                Some(ticket) => ticket.settle(commit),
                None => commit.map_err(|error| classify_commit(false, error)),
            };
            if let Err(error) = commit
                && !rolled_back
            {
                drop(call);
                return Err(self.fail_answered_commit(error));
            }
            if last
                && !rolled_back
                && let Err(error) = qualify_commit(self.client()?)
            {
                drop(call);
                return Err(self.fail_terminal(error));
            }
            if last && !rolled_back {
                if let Some(probe) = self.fault_probe.as_deref() {
                    probe.observe(crate::SqlNativeEvent::Commit);
                }
                if let Err(error) = self.after_commit_boundary().await {
                    drop(call);
                    self.settlement.reject_error(&error);
                    self.state = NativeState::Ended {
                        cause: Some(error.clone()),
                        clean: false,
                    };
                    return Err(error);
                }
            }
            let failure = pipeline_failure_index(&results).and_then(|index| {
                let error = std::mem::replace(&mut results[index], Ok([0, 1])).err()?;
                let is_assertion =
                    matches!(error, super::error::PostgresCallError::CountAssertion(_));
                let is_native = error.is_native();
                let is_server_error = error.is_server_error();
                Some((
                    offset + index,
                    is_assertion,
                    is_native,
                    is_server_error,
                    error.report(NativeOperation::SqlQuery),
                ))
            });
            if let Some((statement_index, is_assertion, is_native, is_server_error, error)) =
                failure
            {
                drop(call);
                if last {
                    // A local statement failure (a client-side decode) with no
                    // server abort does not undo a COMMIT that completed.
                    let local_after_commit = !rolled_back && !is_native && !is_assertion;
                    if (is_native && !is_server_error) || local_after_commit {
                        // A missing statement reply after COMMIT was sent, or
                        // a statement failure that cannot show the COMMIT
                        // rolled back.
                        let error = StorageError::commit_unknown(if local_after_commit {
                            "Postgres terminal statement result could not be checked after COMMIT"
                        } else {
                            "Postgres terminal statement reply was not acknowledged"
                        });
                        self.settlement.reject_error(&error);
                        self.state = NativeState::Ended {
                            cause: Some(error.clone()),
                            clean: false,
                        };
                        return Err(error);
                    }
                    // The server aborted the transaction and answered COMMIT.
                    self.settlement.reject_error(&error);
                    self.state = NativeState::Ended {
                        cause: (is_server_error || is_assertion).then(|| error.clone()),
                        clean: true,
                    };
                } else {
                    self.state = NativeState::Open;
                    self.abort_with(&error).await;
                }
                return if is_assertion {
                    Ok(Err(crate::SqlWriteBatchFailure::CountAssertion {
                        statement_index,
                        error,
                    }))
                } else if is_native {
                    Ok(Err(crate::SqlWriteBatchFailure::Other {
                        statement_index,
                        error,
                    }))
                } else {
                    Err(error)
                };
            }
            self.state = if last {
                NativeState::Committed
            } else {
                NativeState::Open
            };
            call.complete(Ok(()))?;
            for count in results.into_iter().filter_map(Result::ok) {
                let (mut page, mut values) = buffers.next().ok_or_else(|| {
                    let error = StorageError::internal(
                        "Postgres counted write result has no reserved buffer",
                    );
                    self.settlement.reject_error(&error);
                    error
                })?;
                values.extend(count.into_iter().map(crate::SqlValue::Integer));
                page.push(crate::SqlRow::new(values), max_bytes)
                    .inspect_err(|error| self.settlement.reject_error(error))?;
                pages.push(page);
            }
            if last {
                return Ok(Ok(pages));
            }
            needs_begin = false;
            offset = end;
        }
    }

    async fn query_batch_open(
        &mut self,
        statements: Vec<(SqlStatement, ReadProtection)>,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<crate::SqlOpenReadBatchResult> {
        if matches!(
            self.state,
            NativeState::Committed | NativeState::Ended { .. }
        ) {
            // A probe after settlement keeps the settled cause.
            return Err(StorageError::internal("Postgres attempt already finished"));
        }
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            self.settlement.charge_calls(statements.len())?;
            for (statement, protection) in &statements {
                statement.charge_work(&self.settlement)?;
                protection.validate(self.mode.capability())?;
                super::input_limits::require_statement_input(statement, *protection)?;
            }
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            Ok(pages)
        })();
        let mut pages = call.complete(admitted)?;
        let mut input = statements.into_iter();
        let mut offset = 0;
        loop {
            let batch = input
                .by_ref()
                .take(POSTGRES_PIPELINE_STATEMENTS)
                .collect::<Vec<_>>();
            if batch.is_empty() {
                return Ok(crate::SqlOpenReadBatchResult {
                    pages,
                    failure: None,
                });
            }
            let call = self.settlement.begin_call()?;
            let wave =
                match census_wave(self.fault_probe.as_deref(), &self.client, || {
                    let mut sql = Vec::with_capacity(batch.len() + 1);
                    if matches!(self.state, NativeState::Idle) {
                        sql.push(crate::SqlWaveStatement::plain(self.mode.begin_sql()));
                    }
                    sql.extend(batch.iter().map(|(statement, protection)| {
                        crate::SqlWaveStatement {
                            sql: statement.sql(),
                            lock_suffix: super::statements::query_protection_suffix(*protection),
                        }
                    }));
                    sql
                })
                .await
                {
                    Ok(wave) => wave,
                    Err(error) => return call.complete(Err(error)),
                };
            let mut prepared = self.statements()?.prepare_window(batch).await;
            let preparation_failure = pipeline_failure_index(&prepared).and_then(|index| {
                prepared
                    .remove(index)
                    .err()
                    .map(|error| (offset + index, error))
            });
            if let Some((statement_index, error)) = preparation_failure {
                drop(call);
                PostgresWave::end(wave);
                return self.abort_open_batch(pages, statement_index, error).await;
            }
            call.complete(Ok(()))?;
            let begin = match self.wave_begin() {
                Ok(begin) => begin,
                Err(error) => {
                    PostgresWave::end(wave);
                    return self.abort_open_batch(pages, offset, error.into()).await;
                }
            };
            let call = self.settlement.begin_call()?;
            self.state = NativeState::Open;
            let native = self.statements()?;
            let ready = prepared.into_iter().filter_map(Result::ok);
            let (begun, mut results) = futures::join!(
                self.send_wave_begin(begin.as_deref()),
                join_all(ready.map(|statement| native.query_prepared(statement, max_bytes))),
            );
            PostgresWave::end(wave);
            if let Err(error) = begun {
                drop(call);
                return self.abort_open_batch(pages, offset, error.into()).await;
            }
            let selected = pipeline_failure_index(&results);
            if let Some(index) = selected
                && let Err(error) = std::mem::replace(&mut results[index], Ok(SqlQueryPage::new()))
            {
                // Only the original prefix is authoritative, even if a later
                // response completed successfully before an earlier error.
                pages.extend(
                    results
                        .into_iter()
                        .take(index)
                        .take_while(Result::is_ok)
                        .filter_map(Result::ok),
                );
                drop(call);
                return self.abort_open_batch(pages, offset + index, error).await;
            }
            let charged = (|| {
                for page in results.iter().filter_map(|result| result.as_ref().ok()) {
                    page.charge_work(&self.settlement)?;
                }
                Ok(())
            })();
            if let Err(error) = call.complete(charged) {
                return self.abort_open_batch(pages, offset, error.into()).await;
            }
            pages.extend(results.into_iter().filter_map(Result::ok));
            offset += POSTGRES_PIPELINE_STATEMENTS;
        }
    }

    async fn query(
        &mut self,
        statement: SqlStatement,
        protection: ReadProtection,
        max_bytes: NonZeroUsize,
    ) -> StorageResult<SqlQueryPage> {
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            statement.charge_work(&self.settlement)?;
            protection.validate(self.mode.capability())?;
            super::input_limits::require_statement_input(&statement, protection)
        })();
        if let Err(error) = admitted {
            return call.complete(Err(error));
        }
        let wave = match census_wave(self.fault_probe.as_deref(), &self.client, || {
            self.wave_statements([crate::SqlWaveStatement {
                sql: statement.sql(),
                lock_suffix: super::statements::query_protection_suffix(protection),
            }])
        })
        .await
        {
            Ok(wave) => wave,
            Err(error) => return call.complete(Err(error)),
        };
        let prepared = match self
            .statements()?
            .prepare_query(statement, protection)
            .await
        {
            Ok(prepared) => prepared,
            Err(error) => {
                PostgresWave::end(wave);
                return call.complete(Err(error.report(NativeOperation::SqlQuery)));
            }
        };
        let begin = match self.wave_begin() {
            Ok(begin) => begin,
            Err(error) => {
                PostgresWave::end(wave);
                return call.complete(Err(error));
            }
        };
        if begin.is_some() {
            self.state = NativeState::Open;
        }
        let native = self.statements()?;
        let (begun, page) = futures::join!(
            self.send_wave_begin(begin.as_deref()),
            native.query_prepared(prepared, max_bytes),
        );
        PostgresWave::end(wave);
        let result = begun
            .and_then(|()| page.map_err(|error| error.report(NativeOperation::SqlQuery)))
            .and_then(|page| {
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
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            self.settlement.charge_calls(statements.len())?;
            for (statement, _) in &statements {
                statement.charge_work(&self.settlement)?;
            }
            for (statement, protection) in &statements {
                protection.validate(self.mode.capability())?;
                super::input_limits::require_statement_input(statement, *protection)?;
            }
            let mut pages = Vec::new();
            pages
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            Ok(pages)
        })();
        let mut pages: Vec<SqlQueryPage> = match admitted {
            Ok(pages) => pages,
            Err(error) => return call.complete(Err(error)),
        };
        let mut statements = statements.into_iter();
        loop {
            let mut batch = Vec::new();
            if batch
                .try_reserve_exact(statements.len().min(POSTGRES_PIPELINE_STATEMENTS))
                .is_err()
            {
                return call.complete(Err(storage_types::AttemptFailure::CAPACITY.into()));
            }
            batch.extend(statements.by_ref().take(POSTGRES_PIPELINE_STATEMENTS));
            if batch.is_empty() {
                break;
            }
            let wave = match census_wave(self.fault_probe.as_deref(), &self.client, || {
                self.wave_statements(batch.iter().map(|(statement, protection)| {
                    crate::SqlWaveStatement {
                        sql: statement.sql(),
                        lock_suffix: super::statements::query_protection_suffix(*protection),
                    }
                }))
            })
            .await
            {
                Ok(wave) => wave,
                Err(error) => return call.complete(Err(error)),
            };
            // Parse every statement before dispatch: observations cannot
            // overtake locks.
            let prepared = collect_pipeline(
                self.statements()?.prepare_window(batch).await,
                NativeOperation::SqlQuery,
            );
            let window = match prepared {
                Ok(prepared) => match self.wave_begin() {
                    Ok(begin) => {
                        if begin.is_some() {
                            self.state = NativeState::Open;
                        }
                        self.read_wave(begin.as_deref(), prepared, max_bytes).await
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            PostgresWave::end(wave);
            match window {
                Ok(window) => pages.extend(window),
                Err(error) => return call.complete(Err(error)),
            }
        }
        let charged = pages
            .iter()
            .try_for_each(|page| page.charge_work(&self.settlement));
        call.complete(charged.map(|()| pages))
    }

    async fn execute(&mut self, statement: SqlStatement) -> StorageResult<u64> {
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            statement.charge_work(&self.settlement)?;
            self.mode.capability().require_write()?;
            super::input_limits::require_statement_input(&statement, ReadProtection::Snapshot)
        })();
        if let Err(error) = admitted {
            return call.complete(Err(error));
        }
        let wave = match census_wave(self.fault_probe.as_deref(), &self.client, || {
            self.wave_statements([crate::SqlWaveStatement::plain(statement.sql())])
        })
        .await
        {
            Ok(wave) => wave,
            Err(error) => return call.complete(Err(error)),
        };
        let prepared = match self.statements()?.prepare_execute(statement).await {
            Ok(prepared) => prepared,
            Err(error) => {
                PostgresWave::end(wave);
                return call.complete(Err(error.report(NativeOperation::SqlExecute)));
            }
        };
        let begin = match self.wave_begin() {
            Ok(begin) => begin,
            Err(error) => {
                PostgresWave::end(wave);
                return call.complete(Err(error));
            }
        };
        if begin.is_some() {
            self.state = NativeState::Open;
        }
        let native = self.statements()?;
        let (begun, changed) = futures::join!(
            self.send_wave_begin(begin.as_deref()),
            native.execute_prepared(&prepared),
        );
        PostgresWave::end(wave);
        let result = begun
            .and_then(|()| changed.map_err(|error| error.report(NativeOperation::SqlExecute)))
            .and_then(|count| {
                self.settlement.charge_data(std::mem::size_of::<u64>())?;
                Ok(count)
            });
        call.complete(result)
    }

    async fn execute_batch(&mut self, statements: Vec<SqlStatement>) -> StorageResult<Vec<u64>> {
        self.require_usable()?;
        let call = self.settlement.begin_call()?;
        let admitted = (|| {
            self.settlement.charge_calls(statements.len())?;
            for statement in &statements {
                statement.charge_work(&self.settlement)?;
            }
            self.mode.capability().require_write()?;
            for statement in &statements {
                super::input_limits::require_statement_input(statement, ReadProtection::Snapshot)?;
            }
            let mut changed = Vec::new();
            changed
                .try_reserve_exact(statements.len())
                .map_err(|_| storage_types::AttemptFailure::CAPACITY)?;
            Ok(changed)
        })();
        let mut changed: Vec<u64> = match admitted {
            Ok(changed) => changed,
            Err(error) => return call.complete(Err(error)),
        };
        let mut statements = statements.into_iter();
        loop {
            let mut batch = Vec::new();
            if batch
                .try_reserve_exact(statements.len().min(POSTGRES_PIPELINE_STATEMENTS))
                .is_err()
            {
                return call.complete(Err(storage_types::AttemptFailure::CAPACITY.into()));
            }
            batch.extend(statements.by_ref().take(POSTGRES_PIPELINE_STATEMENTS));
            if batch.is_empty() {
                break;
            }
            let wave = match census_wave(self.fault_probe.as_deref(), &self.client, || {
                self.wave_statements(
                    batch
                        .iter()
                        .map(|statement| crate::SqlWaveStatement::plain(statement.sql())),
                )
            })
            .await
            {
                Ok(wave) => wave,
                Err(error) => return call.complete(Err(error)),
            };
            let prepared = {
                let native = self.statements()?;
                collect_pipeline(
                    join_all(
                        batch
                            .into_iter()
                            .map(|statement| native.prepare_execute(statement)),
                    )
                    .await,
                    NativeOperation::SqlExecute,
                )
            };
            let window = match prepared {
                Ok(prepared) => match self.wave_begin() {
                    Ok(begin) => {
                        if begin.is_some() {
                            self.state = NativeState::Open;
                        }
                        self.execute_wave(begin.as_deref(), prepared).await
                    }
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            PostgresWave::end(wave);
            match window {
                Ok(window) => changed.extend(window),
                Err(error) => return call.complete(Err(error)),
            }
        }
        let charged = changed
            .len()
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| StorageError::from(storage_types::AttemptFailure::CAPACITY))
            .and_then(|bytes| self.settlement.charge_data(bytes));
        call.complete(charged.map(|()| changed))
    }
}
