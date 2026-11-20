//! Counts the native events of the attempts of one store (the wave census)
//! and holds one chosen wave until the test releases it.
//!
//! One `NativeCensus` implements both native probes. Install it with
//! `TestStore::new_with_census`, run the operation once to warm the
//! statement and binding caches, `reset`, run it again, then `snapshot`.

use std::{
    collections::BTreeSet,
    fmt,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use futures::future::BoxFuture;
use storage_types::{StorageError, StorageResult};
use tokio::sync::Notify;

/// The counts of one census window. SQL providers count statements; KV
/// providers count point keys and ranges. Durable work and the FoundationDB
/// conflict surface are deterministic counts, never times.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CensusCounts {
    /// Round trips or worker hops, including `BEGIN`, `COMMIT` and
    /// `ROLLBACK` where they are their own round trip, and the implicit
    /// FoundationDB read version.
    pub(crate) waves: u64,
    pub(crate) statements: u64,
    pub(crate) keys: u64,
    pub(crate) ranges: u64,
    pub(crate) rows_read: u64,
    /// SQL attempted row changes (including later-aborted writes); KV staged
    /// puts, deletes and range deletes. Committed durable work is separate.
    pub(crate) rows_written: u64,
    /// Native attempts begun, including metadata preparation and retries.
    pub(crate) begins: u64,
    /// Postgres waves whose only statement is the BEGIN batch (lazy BEGIN makes
    /// this 0).
    pub(crate) lone_begins: u64,
    /// Acknowledged writing attempts: Turso `BEGIN IMMEDIATE` and Postgres
    /// `START TRANSACTION` without `READ ONLY`. Read-only catalog attempts
    /// (a cold cache) commit too but are not counted.
    pub(crate) write_commits: u64,
    /// Writing attempts begun (acknowledged or not), as `write_commits`
    /// classifies them. Zero: every attempt was read-only.
    pub(crate) write_begins: u64,
    pub(crate) write_attempt: bool,
    pub(crate) commits: u64,
    pub(crate) rollbacks: u64,
    pub(crate) barriers: u64,
    /// Postgres statements whose plan was not cached (one Parse round trip
    /// each, not counted in `waves`).
    pub(crate) cold_prepares: u64,
    /// Read-version waves requesting Cached; actual SDK cache reuse is unknown.
    pub(crate) cached_read_versions: u64,
    /// SQL relations or KV key families that the operation reads (`r=`) and
    /// writes (`w=`): the proxy for random reads.
    pub(crate) touched_read: BTreeSet<String>,
    pub(crate) touched_written: BTreeSet<String>,
    pub(crate) locks: SqlLocks,
    pub(crate) conflicts: KvConflicts,
    pub(crate) durable: DurableWork,
}

/// Postgres lock statements by kind, classified from the statement text
/// that the driver sends (with its read protection suffix).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct SqlLocks {
    pub(crate) advisory_catalog_shared: u64,
    pub(crate) advisory_catalog_exclusive: u64,
    pub(crate) advisory_item: u64,
    /// Other advisory locks (maintenance admission).
    pub(crate) advisory_other: u64,
    pub(crate) for_share: u64,
    pub(crate) for_update: u64,
    pub(crate) for_no_key_update: u64,
    pub(crate) skip_locked: u64,
    pub(crate) lock_table: u64,
}

/// Keys and ranges that the attempts add to the conflict set: FoundationDB
/// read and write conflicts; RocksDB written keys that the optimistic commit
/// validates (its reads add none).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct KvConflicts {
    pub(crate) read_keys: u64,
    pub(crate) read_ranges: u64,
    pub(crate) write_keys: u64,
    pub(crate) write_ranges: u64,
}

/// What the commits make durable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DurableWork {
    /// Postgres WAL records and bytes of the pool backends.
    pub(crate) wal_records: u64,
    pub(crate) wal_bytes: u64,
    /// Postgres full-page images; not shown: they appear in some runs only.
    pub(crate) wal_full_pages: u64,
    /// SQLite WAL frames (pages) appended.
    pub(crate) wal_frames: u64,
    /// Turso MVCC logical log bytes appended.
    pub(crate) log_bytes: u64,
    /// RocksDB serialized write batch bytes.
    pub(crate) write_batch_bytes: u64,
    /// FoundationDB mutation bytes: physical key plus value of each set and
    /// atomic operation, key of each clear, begin plus end of a clear range.
    /// Includes staged work from aborted attempts, not only durable mutations.
    pub(crate) mutation_bytes: u64,
}

impl CensusCounts {
    /// The measured durable work, for a diagnostic line.
    pub(crate) fn durable_summary(&self) -> String {
        format!("{:?}", self.durable)
    }
}

impl fmt::Display for CensusCounts {
    /// Fields with a zero value or an empty set are left out:
    /// `waves=6 stmts=13 read=3 written=6 begin=1 commit=1 r=cat+item w=item`.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let counts = [
            ("waves", self.waves),
            ("stmts", self.statements),
            ("keys", self.keys),
            ("ranges", self.ranges),
            ("read", self.rows_read),
            ("written", self.rows_written),
            ("begin", self.begins),
            ("commit", self.commits),
            ("rollback", self.rollbacks),
            ("barrier", self.barriers),
            ("cold", self.cold_prepares),
            ("cached_rv", self.cached_read_versions),
            ("lone_begin", self.lone_begins),
        ];
        let locks = &self.locks;
        let conflicts = &self.conflicts;
        let durable = &self.durable;
        let costs = [
            ("adv_cat_sh", locks.advisory_catalog_shared),
            ("adv_cat_ex", locks.advisory_catalog_exclusive),
            ("adv_item", locks.advisory_item),
            ("adv_other", locks.advisory_other),
            ("for_share", locks.for_share),
            ("for_update", locks.for_update),
            ("for_nk_update", locks.for_no_key_update),
            ("skip_locked", locks.skip_locked),
            ("lock_table", locks.lock_table),
            ("rck", conflicts.read_keys),
            ("rcr", conflicts.read_ranges),
            ("wck", conflicts.write_keys),
            ("wcr", conflicts.write_ranges),
            ("wal_rec", durable.wal_records),
            ("wal_b", durable.wal_bytes),
            ("wal_frames", durable.wal_frames),
            ("log_b", durable.log_bytes),
            ("batch_b", durable.write_batch_bytes),
            ("mut_b", durable.mutation_bytes),
        ];
        let mut fields = counts
            .into_iter()
            .filter(|(_, value)| *value != 0)
            .map(|(name, value)| format!("{name}={value}"))
            .collect::<Vec<_>>();
        for (name, set) in [("r", &self.touched_read), ("w", &self.touched_written)] {
            if !set.is_empty() {
                fields.push(format!(
                    "{name}={}",
                    set.iter().map(String::as_str).collect::<Vec<_>>().join("+")
                ));
            }
        }
        fields.extend(
            costs
                .into_iter()
                .filter(|(_, value)| *value != 0)
                .map(|(name, value)| format!("{name}={value}")),
        );
        formatter.write_str(&fields.join(" "))
    }
}

/// The wave that a hold filter sees before the wave is sent.
pub(crate) enum CensusWave<'a> {
    /// The statements of one SQL wave: text and the lock suffix that the
    /// driver appends.
    #[cfg_attr(
        not(any(feature = "sqlite", feature = "turso", feature = "postgres")),
        expect(
            dead_code,
            reason = "only SQL providers construct SQL waves; test code matches the variant \
                      unconditionally"
        )
    )]
    Sql(&'a [(&'a str, &'static str)]),
    #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
    Kv(kv_backend::KvWave),
}

impl CensusWave<'_> {
    /// Whether one statement of a SQL wave contains `text`.
    pub(crate) fn sql_contains(&self, text: &str) -> bool {
        match self {
            Self::Sql(sql) => sql.iter().any(|(statement, _)| statement.contains(text)),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            Self::Kv(_) => false,
        }
    }

    /// Whether the wave reads or writes data (not `BEGIN`, `COMMIT`,
    /// `ROLLBACK`, or a KV commit or abort).
    pub(crate) fn is_data(&self) -> bool {
        match self {
            Self::Sql(sql) => sql.iter().any(|(statement, _)| {
                !["START", "BEGIN", "COMMIT", "ROLLBACK"]
                    .iter()
                    .any(|keyword| statement.starts_with(keyword))
            }),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            Self::Kv(wave) => !matches!(
                wave.kind,
                kv_backend::KvWaveKind::Commit | kv_backend::KvWaveKind::Abort
            ),
        }
    }
}

pub(crate) type WaveFilter = fn(&CensusWave<'_>) -> bool;

/// One armed hold. The first wave that matches the filter waits in
/// `before_wave` until `release`.
#[derive(Default)]
pub(crate) struct HeldWave {
    reached: Notify,
    release: Notify,
}

impl HeldWave {
    /// Waits until a matching wave is held (20 s budget).
    pub(crate) async fn reached(&self) {
        tokio::time::timeout(Duration::from_secs(20), self.reached.notified())
            .await
            .expect("no wave matched the hold filter");
    }

    pub(crate) fn release(&self) {
        self.release.notify_one();
    }
}

#[derive(Default)]
pub(crate) struct NativeCensus {
    counts: Mutex<CensusCounts>,
    native_events: Mutex<Option<(Vec<String>, bool)>>,
    entry_blob_bytes: Mutex<u64>,
    chunk_key_reads: Mutex<u64>,
    returned_data: Mutex<(u64, u64)>,
    hold: Mutex<Option<(WaveFilter, Arc<HeldWave>)>>,
    /// SQL statement texts of every wave since `record`, in send order.
    recorded: Mutex<Option<Vec<String>>>,
}

impl NativeCensus {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn reset(&self) {
        *self.counts.lock().unwrap_or_else(PoisonError::into_inner) = CensusCounts::default();
        *self
            .entry_blob_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = 0;
        *self
            .chunk_key_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = 0;
        *self
            .returned_data
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = (0, 0);
    }

    /// Record only the next awaited operation. The cap bounds diagnostics,
    /// while overflow remains explicit rather than losing evidence silently.
    pub(crate) fn start_native_trace(&self) {
        *self
            .native_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some((Vec::new(), false));
    }

    pub(crate) fn finish_native_trace(&self, label: &str) -> CensusCounts {
        let counts = self.snapshot();
        let trace = self
            .native_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((events, overflow)) = trace {
            println!("N9_ATTEMPT_TRACE {label} {counts} overflow={overflow} events={events:?}");
        }
        counts
    }

    pub(crate) fn returned_data(&self) -> (u64, u64) {
        *self
            .returned_data
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn chunk_key_reads(&self) -> u64 {
        *self
            .chunk_key_reads
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn entry_blob_bytes(&self) -> u64 {
        *self
            .entry_blob_bytes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn snapshot(&self) -> CensusCounts {
        self.counts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Arms a one-time hold for the first wave that `filter` accepts.
    pub(crate) fn hold(&self, filter: WaveFilter) -> Arc<HeldWave> {
        let held = Arc::new(HeldWave::default());
        *self.hold.lock().unwrap_or_else(PoisonError::into_inner) =
            Some((filter, Arc::clone(&held)));
        held
    }

    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    /// Starts (or restarts) recording the SQL statement texts of each wave.
    pub(crate) fn record(&self) {
        *self.recorded.lock().unwrap_or_else(PoisonError::into_inner) = Some(Vec::new());
    }

    #[cfg(any(feature = "sqlite", feature = "turso", feature = "postgres"))]
    /// The statement texts recorded since `record`, without the read
    /// protection suffix that the driver appends.
    pub(crate) fn recorded(&self) -> Vec<String> {
        self.recorded
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .unwrap_or_default()
    }

    fn count(&self, update: impl FnOnce(&mut CensusCounts)) {
        update(&mut self.counts.lock().unwrap_or_else(PoisonError::into_inner));
    }

    async fn before(&self, wave: CensusWave<'_>) {
        match &wave {
            CensusWave::Sql(sql) => {
                self.count(|counts| {
                    for (statement, lock_suffix) in *sql {
                        classify_statement(counts, statement, lock_suffix);
                    }
                });
                if let Some(recorded) = self
                    .recorded
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .as_mut()
                {
                    recorded.extend(sql.iter().map(|(statement, _)| (*statement).to_owned()));
                }
            }
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            CensusWave::Kv(_) => {}
        }
        let held = {
            let mut slot = self.hold.lock().unwrap_or_else(PoisonError::into_inner);
            if slot.as_ref().is_some_and(|(filter, _)| filter(&wave)) {
                slot.take().map(|(_, held)| held)
            } else {
                None
            }
        };
        if let Some(held) = held {
            held.reached.notify_one();
            tokio::time::timeout(Duration::from_secs(20), held.release.notified())
                .await
                .expect("held wave was not released");
        }
    }
}

fn to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A short label for a SQL relation name, or `None` for a word that is not
/// a relation. Relations of one table or index share one label.
fn relation_label(word: &str) -> Option<String> {
    let hex = |rest: &str| !rest.is_empty() && rest.bytes().all(|byte| byte.is_ascii_hexdigit());
    if word.strip_prefix("table_").is_some_and(hex) {
        return Some("item".to_owned());
    }
    if word.strip_prefix("ttl_index_").is_some_and(hex) {
        return Some("ttl".to_owned());
    }
    if word == "regional_markers" || word == "regional_progress" {
        return Some(word.to_owned());
    }
    word.strip_prefix("sys_").map(|name| {
        if name == "infrastructure_records" {
            return "cat".to_owned();
        }
        let short = name
            .strip_prefix("dynamo_stream_")
            .map(|rest| format!("ds_{rest}"))
            .or_else(|| {
                name.strip_prefix("system_stream_")
                    .map(|rest| format!("ss_{rest}"))
            });
        short.unwrap_or_else(|| name.to_owned())
    })
}

/// Counts the relations and the locks of one SQL statement. The relation
/// after `INTO`, `UPDATE` or `DELETE FROM` is written; every other relation
/// is read; a `LOCK TABLE` relation is a lock only.
fn classify_statement(counts: &mut CensusCounts, sql: &str, lock_suffix: &str) {
    let text = format!("{sql}{lock_suffix}");
    let postgres_begin = sql.starts_with("START TRANSACTION");
    if matches!(sql, "BEGIN" | "BEGIN IMMEDIATE") || postgres_begin {
        counts.write_attempt =
            sql == "BEGIN IMMEDIATE" || (postgres_begin && !sql.contains("READ ONLY"));
        counts.write_begins += u64::from(counts.write_attempt);
    }
    let locks = &mut counts.locks;
    let catalog = text.contains("hashtext(current_schema()");
    if text.contains("pg_advisory_xact_lock_shared(") {
        if catalog {
            locks.advisory_catalog_shared += 1;
        } else {
            locks.advisory_other += 1;
        }
    } else if text.contains("pg_advisory_xact_lock(") {
        if catalog {
            locks.advisory_catalog_exclusive += 1;
        } else if text.contains("hashtextextended(") {
            locks.advisory_item += 1;
        } else {
            locks.advisory_other += 1;
        }
    }
    let present = |clause: &str| u64::from(text.contains(clause));
    locks.for_share += present("FOR SHARE");
    locks.for_update += present("FOR UPDATE");
    locks.for_no_key_update += present("FOR NO KEY UPDATE");
    locks.skip_locked += present("SKIP LOCKED");
    locks.lock_table += to_u64(text.matches("LOCK TABLE").count());
    let words = text
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    for (index, word) in words.iter().enumerate() {
        let Some(label) = relation_label(word) else {
            continue;
        };
        let before = |back: usize| {
            index
                .checked_sub(back)
                .and_then(|position| words.get(position))
                .map(|word| word.to_ascii_uppercase())
        };
        let previous = before(1);
        if previous.as_deref() == Some("TABLE") && before(2).as_deref() == Some("LOCK") {
            continue;
        }
        let written = matches!(previous.as_deref(), Some("INTO" | "UPDATE"))
            || (previous.as_deref() == Some("FROM") && before(2).as_deref() == Some("DELETE"));
        if written {
            counts.touched_written.insert(label);
        } else {
            counts.touched_read.insert(label);
        }
    }
}

#[cfg(any(feature = "sqlite", feature = "postgres", feature = "turso"))]
impl sql_backend::SqlFaultProbe for NativeCensus {
    fn fail(&self, _call: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn alter_query_page(
        &self,
        call: sql_backend::SqlFaultCall<'_>,
        page: sql_backend::SqlQueryPage,
    ) -> sql_backend::SqlQueryPage {
        if call.sql.starts_with(
            "SELECT table_id, shard_id, lineage_id, offset_order, offset_text, entry_blob",
        ) {
            let bytes = page
                .rows
                .iter()
                .filter_map(|row| match row.value(sql_backend::ColumnIndex::new(5)) {
                    Ok(sql_backend::SqlValue::Blob(value)) => Some(value.len() as u64),
                    _ => None,
                })
                .sum::<u64>();
            *self
                .entry_blob_bytes
                .lock()
                .unwrap_or_else(PoisonError::into_inner) += bytes;
        }
        page
    }

    fn observe(&self, event: sql_backend::SqlNativeEvent) {
        use sql_backend::{SqlFaultOperation, SqlNativeEvent};
        if let Some((events, overflow)) = self
            .native_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            if events.len() < 1024 {
                events.push(format!("{event:?}"));
            } else {
                *overflow = true;
            }
        }
        match event {
            SqlNativeEvent::Begin => self.count(|counts| counts.begins += 1),
            SqlNativeEvent::Commit => self.count(|counts| {
                counts.commits += 1;
                counts.write_commits += u64::from(counts.write_attempt);
                counts.write_attempt = false;
            }),
            SqlNativeEvent::Rollback => self.count(|counts| {
                counts.rollbacks += 1;
                counts.write_attempt = false;
            }),
            SqlNativeEvent::Wave { cold_prepares, .. } => self.count(|counts| {
                counts.waves += 1;
                counts.cold_prepares += to_u64(cold_prepares);
            }),
            SqlNativeEvent::Statement { operation, rows } => self.count(|counts| {
                counts.statements += 1;
                match operation {
                    SqlFaultOperation::Query => counts.rows_read += rows,
                    SqlFaultOperation::Execute => counts.rows_written += rows,
                }
            }),
            SqlNativeEvent::RowsWritten { rows } => {
                self.count(|counts| counts.rows_written += rows)
            }
            SqlNativeEvent::DurabilityBarrier => self.count(|counts| counts.barriers += 1),
            SqlNativeEvent::WalFrames { frames } => {
                self.count(|counts| counts.durable.wal_frames += frames);
            }
        }
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [sql_backend::SqlWaveStatement<'a>],
    ) -> BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            if let [only] = statements
                && only.sql.starts_with("START TRANSACTION")
            {
                self.count(|counts| counts.lone_begins += 1);
            }
            let statements = statements
                .iter()
                .map(|statement| (statement.sql, statement.lock_suffix))
                .collect::<Vec<_>>();
            self.before(CensusWave::Sql(&statements)).await;
            Ok(())
        })
    }

    fn flush_native_statistics(&self) -> bool {
        true
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for NativeCensus {
    fn fail(&self, _call: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        use kv_backend::KvNativeEvent;
        if matches!(
            event,
            KvNativeEvent::Begin
                | KvNativeEvent::AttemptRequested { .. }
                | KvNativeEvent::CommitFailed { .. }
                | KvNativeEvent::Commit
                | KvNativeEvent::Abort
                | KvNativeEvent::Mutations { .. }
                | KvNativeEvent::Wave { .. }
        ) && let Some((events, overflow)) = self
            .native_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            if events.len() < 1024 {
                events.push(format!("{event:?}"));
            } else {
                *overflow = true;
            }
        }
        match event {
            KvNativeEvent::AttemptRequested { .. } | KvNativeEvent::CommitFailed { .. } => {}
            KvNativeEvent::Begin => self.count(|counts| counts.begins += 1),
            KvNativeEvent::Commit => self.count(|counts| counts.commits += 1),
            KvNativeEvent::Abort => self.count(|counts| counts.rollbacks += 1),
            KvNativeEvent::Wave {
                cached_read_version,
                ..
            } => self.count(|counts| {
                counts.waves += 1;
                counts.cached_read_versions += u64::from(cached_read_version);
            }),
            KvNativeEvent::KeysRead { requested, found } => self.count(|counts| {
                counts.keys += to_u64(requested);
                counts.rows_read += to_u64(found);
            }),
            KvNativeEvent::RangesRead { ranges, entries } => self.count(|counts| {
                counts.ranges += to_u64(ranges);
                counts.rows_read += to_u64(entries);
            }),
            KvNativeEvent::ReturnedData { rows, bytes } => {
                let mut returned = self
                    .returned_data
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                returned.0 += to_u64(rows);
                returned.1 += to_u64(bytes);
            }
            KvNativeEvent::Mutations {
                puts,
                deletes,
                range_deletes,
            } => self.count(|counts| {
                counts.rows_written += to_u64(puts) + to_u64(deletes) + to_u64(range_deletes);
            }),
            KvNativeEvent::DurabilityBarrier => self.count(|counts| counts.barriers += 1),
            KvNativeEvent::WriteBatchBytes { bytes } => {
                self.count(|counts| counts.durable.write_batch_bytes += to_u64(bytes));
            }
        }
    }

    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        use kv_backend::KvAccessKind;
        let family = kv_backend::key_family(access.key);
        if let Some((events, overflow)) = self
            .native_events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            if events.len() < 1024 {
                events.push(format!(
                    "Access kind={:?} family={family} native_bytes={} conflict={}",
                    access.kind, access.native_bytes, access.conflict
                ));
            } else {
                *overflow = true;
            }
        }
        if matches!(access.kind, KvAccessKind::ReportedConflict) {
            return;
        }
        if matches!(access.kind, KvAccessKind::Read) && kv_backend::is_value_chunk_key(access.key) {
            *self
                .chunk_key_reads
                .lock()
                .unwrap_or_else(PoisonError::into_inner) += 1;
        }
        self.count(|counts| {
            counts.durable.mutation_bytes += to_u64(access.native_bytes);
            let conflicts = &mut counts.conflicts;
            let conflict = u64::from(access.conflict);
            match access.kind {
                KvAccessKind::Read | KvAccessKind::ProtectKey => conflicts.read_keys += conflict,
                KvAccessKind::ReadRange | KvAccessKind::ProtectRange => {
                    conflicts.read_ranges += conflict
                }
                KvAccessKind::Write | KvAccessKind::Clear => conflicts.write_keys += conflict,
                KvAccessKind::ClearRange => conflicts.write_ranges += conflict,
                KvAccessKind::ReportedConflict => {}
            }
            match access.kind {
                KvAccessKind::Read | KvAccessKind::ReadRange => {
                    counts.touched_read.insert(family);
                }
                KvAccessKind::Write | KvAccessKind::Clear | KvAccessKind::ClearRange => {
                    counts.touched_written.insert(family);
                }
                KvAccessKind::ProtectKey
                | KvAccessKind::ProtectRange
                | KvAccessKind::ReportedConflict => {}
            }
        });
    }

    fn before_wave(&self, wave: kv_backend::KvWave) -> BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            self.before(CensusWave::Kv(wave)).await;
            Ok(())
        })
    }
}
