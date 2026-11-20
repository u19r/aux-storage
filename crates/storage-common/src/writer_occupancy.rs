//! Occupancy of a serialized native writer: the SQLite writer connection or
//! the RocksDB writer thread. Every write waits behind the attempt that holds
//! the writer, so that attempt may wait only for its own native requests. The
//! guard finds a write operation that waits for anything else (a timer, a
//! lock, a channel, another attempt) and applies the backend's policy (W4
//! design, section 7).
//!
//! Times come from the backend's injected clock, which has millisecond
//! resolution: one hold counts whole milliseconds, and the sum over many
//! holds is an unbiased estimate of the time in microseconds.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use metrics::{Counter, Gauge, Histogram};
use storage_types::{Clock, StorageError, StorageResult, TimestampMillis};

/// What a foreign await in a serialized write attempt does.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ForeignAwaitPolicy {
    /// Production: count it in `storage.native.writer.foreign_wait.total`.
    #[default]
    Count,
    /// Tests: also fail the attempt with an internal error.
    Fail,
}

/// The provider label of a serialized writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializedWriter {
    Sqlite,
    RocksDb,
}

impl SerializedWriter {
    const fn label(self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::RocksDb => "rocksdb",
        }
    }
}

/// Counts of one backend's writer since its construction.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriterOccupancyCounts {
    /// Write attempts that wait for the writer now.
    pub queue_depth: u64,
    pub busy_micros: u64,
    pub native_micros: u64,
    pub foreign_waits: u64,
}

pub struct WriterOccupancy {
    policy: ForeignAwaitPolicy,
    queue_depth: AtomicU64,
    busy_micros: AtomicU64,
    native_micros: AtomicU64,
    foreign_waits: AtomicU64,
    metrics: WriterMetrics,
}

/// Handles labeled only by provider: no key, item or statement text.
struct WriterMetrics {
    queue_depth: Gauge,
    queue_wait_ms: Histogram,
    busy_micros: Counter,
    native_micros: Counter,
    foreign_waits: Counter,
}

impl WriterOccupancy {
    #[must_use]
    pub fn new(writer: SerializedWriter, policy: ForeignAwaitPolicy) -> Self {
        let provider = writer.label();
        Self {
            policy,
            queue_depth: AtomicU64::new(0),
            busy_micros: AtomicU64::new(0),
            native_micros: AtomicU64::new(0),
            foreign_waits: AtomicU64::new(0),
            metrics: WriterMetrics {
                queue_depth: metrics::gauge!("storage.native.writer.queue.depth", "provider" => provider),
                queue_wait_ms: metrics::histogram!("storage.native.writer.queue.wait.ms", "provider" => provider),
                busy_micros: metrics::counter!("storage.native.writer.busy.micros.total", "provider" => provider),
                native_micros: metrics::counter!("storage.native.writer.native.micros.total", "provider" => provider),
                foreign_waits: metrics::counter!("storage.native.writer.foreign_wait.total", "provider" => provider),
            },
        }
    }

    #[must_use]
    pub fn counts(&self) -> WriterOccupancyCounts {
        WriterOccupancyCounts {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            busy_micros: self.busy_micros.load(Ordering::Relaxed),
            native_micros: self.native_micros.load(Ordering::Relaxed),
            foreign_waits: self.foreign_waits.load(Ordering::Relaxed),
        }
    }

    /// A write attempt starts to wait for the writer. It leaves the queue
    /// when it starts on the writer or when it is dropped unstarted.
    pub fn enqueue(self: &Arc<Self>, clock: Arc<dyn Clock>) -> QueuedWriter {
        self.queue_depth.fetch_add(1, Ordering::Relaxed);
        self.metrics.queue_depth.increment(1.0);
        QueuedWriter {
            occupancy: Arc::clone(self),
            since: clock.now(),
            clock,
            is_started: false,
        }
    }

    /// Polls a write operation once. Each `Pending` must have a native request
    /// outstanding (checked after the poll) or come from an [`exempt_wait`];
    /// any other `Pending` is a foreign await.
    pub fn poll_write_operation<F, R>(
        &self,
        operation: Pin<&mut F>,
        context: &mut Context<'_>,
        has_native_request: impl FnOnce() -> bool,
    ) -> Poll<StorageResult<R>>
    where
        F: Future<Output = StorageResult<R>> + ?Sized,
    {
        IS_EXEMPT_WAIT.set(false);
        let poll = operation.poll(context);
        if poll.is_ready() || IS_EXEMPT_WAIT.replace(false) || has_native_request() {
            return poll;
        }
        self.foreign_waits.fetch_add(1, Ordering::Relaxed);
        self.metrics.foreign_waits.increment(1);
        match self.policy {
            ForeignAwaitPolicy::Count => Poll::Pending,
            ForeignAwaitPolicy::Fail => Poll::Ready(Err(StorageError::internal(
                "foreign await in a serialized write attempt",
            ))),
        }
    }

    fn leave_queue(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
        self.metrics.queue_depth.decrement(1.0);
    }
}

thread_local! {
    /// Set by an exempt wait that returned `Pending` during the current poll.
    static IS_EXEMPT_WAIT: Cell<bool> = const { Cell::new(false) };
}

/// A test fault-probe gate that holds an attempt on purpose: its `Pending` is
/// not a foreign await.
pub async fn exempt_wait<F: Future>(wait: F) -> F::Output {
    let mut wait = std::pin::pin!(wait);
    std::future::poll_fn(|context| {
        let poll = wait.as_mut().poll(context);
        if poll.is_pending() {
            IS_EXEMPT_WAIT.set(true);
        }
        poll
    })
    .await
}

/// A write attempt that waits for the writer.
pub struct QueuedWriter {
    occupancy: Arc<WriterOccupancy>,
    clock: Arc<dyn Clock>,
    since: TimestampMillis,
    is_started: bool,
}

impl QueuedWriter {
    /// The attempt now holds the writer on the calling thread.
    #[must_use]
    pub fn start(mut self) -> WriterHold {
        self.is_started = true;
        self.occupancy.leave_queue();
        let now = self.clock.now();
        self.occupancy
            .metrics
            .queue_wait_ms
            .record(elapsed_millis(self.since, now) as f64);
        WriterHold {
            occupancy: Arc::clone(&self.occupancy),
            clock: Arc::clone(&self.clock),
            since: now,
            native_micros: Cell::new(0),
        }
    }
}

impl Drop for QueuedWriter {
    fn drop(&mut self) {
        if !self.is_started {
            self.occupancy.leave_queue();
        }
    }
}

/// The writer is held until drop, which counts the hold.
pub struct WriterHold {
    occupancy: Arc<WriterOccupancy>,
    clock: Arc<dyn Clock>,
    since: TimestampMillis,
    native_micros: Cell<u64>,
}

impl WriterHold {
    #[must_use]
    pub fn occupancy(&self) -> &WriterOccupancy {
        &self.occupancy
    }
}

/// Runs one native call; counts its time when it runs on the held writer.
pub fn time_native_call<T>(writer: Option<&WriterHold>, call: impl FnOnce() -> T) -> T {
    let Some(writer) = writer else {
        return call();
    };
    let start = writer.clock.now();
    let value = call();
    let micros = elapsed_micros(start, writer.clock.now());
    writer
        .native_micros
        .set(writer.native_micros.get().saturating_add(micros));
    value
}

impl Drop for WriterHold {
    fn drop(&mut self) {
        let busy = elapsed_micros(self.since, self.clock.now());
        let native = self.native_micros.get();
        let occupancy = &self.occupancy;
        occupancy.busy_micros.fetch_add(busy, Ordering::Relaxed);
        occupancy.native_micros.fetch_add(native, Ordering::Relaxed);
        occupancy.metrics.busy_micros.increment(busy);
        occupancy.metrics.native_micros.increment(native);
    }
}

fn elapsed_millis(since: TimestampMillis, now: TimestampMillis) -> u64 {
    // A wall-clock step backwards counts as no time.
    u64::try_from(
        now.timestamp_millis()
            .saturating_sub(since.timestamp_millis()),
    )
    .unwrap_or(0)
}

fn elapsed_micros(since: TimestampMillis, now: TimestampMillis) -> u64 {
    elapsed_millis(since, now).saturating_mul(1000)
}
