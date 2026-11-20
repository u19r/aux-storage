//! Deadlines of native attempts and the request that selects them.

use std::time::Duration;

use tokio::time::Instant;

use crate::{
    AttemptFailure, AttemptMode, AttemptPhase, NativeUnavailable, StorageError, StorageResult,
    TimestampMillis, WriteRetryPolicy,
};

/// The budget a domain operation asks of its backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum DeadlineClass {
    PointRead,
    RangeRead,
    Write,
    Transaction,
    SchemaChange,
    BackgroundPage,
    LongPollProbe,
}

impl DeadlineClass {
    /// Number of classes, and the length of the budget table.
    pub const COUNT: usize = 7;

    /// Every class in budget-table order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::PointRead,
        Self::RangeRead,
        Self::Write,
        Self::Transaction,
        Self::SchemaChange,
        Self::BackgroundPage,
        Self::LongPollProbe,
    ];

    #[must_use]
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Default budgets in milliseconds, in `DeadlineClass::ALL` order.
pub const DEFAULT_ATTEMPT_BUDGETS_MS: [u64; DeadlineClass::COUNT] =
    [5_000, 10_000, 10_000, 15_000, 30_000, 10_000, 1_000];

/// Default wait after a deadline once a commit has begun.
pub const DEFAULT_COMMIT_GRACE_MS: u64 = 2_000;

/// Budgets owned by one initialized backend. Built once from configuration.
#[derive(Clone, Copy, Debug)]
pub struct AttemptDeadlines {
    budgets: [Duration; DeadlineClass::COUNT],
    commit_grace: Duration,
}

impl AttemptDeadlines {
    /// Budgets are in `DeadlineClass::ALL` order. Every value must be positive.
    pub fn try_new(
        budgets: [Duration; DeadlineClass::COUNT],
        commit_grace: Duration,
    ) -> StorageResult<Self> {
        if budgets.iter().any(Duration::is_zero) || commit_grace.is_zero() {
            return Err(StorageError::validation(
                "attempt deadline budgets and commit grace must be positive",
            ));
        }
        Ok(Self {
            budgets,
            commit_grace,
        })
    }

    #[must_use]
    pub fn budget(&self, class: DeadlineClass) -> Duration {
        self.budgets[class.index()]
    }

    #[must_use]
    pub const fn commit_grace(&self) -> Duration {
        self.commit_grace
    }

    /// The deadline expires one budget from now.
    #[must_use]
    pub fn start(&self, class: DeadlineClass) -> AttemptDeadline {
        AttemptDeadline {
            class,
            expires_at: deadline_instant() + self.budget(class),
            commit_grace: self.commit_grace,
        }
    }
}

/// A port that owns the budgets of the backend it runs on.
pub trait AttemptDeadlineSource {
    fn attempt_deadlines(&self) -> AttemptDeadlines;
}

/// The documented default budgets. A host builds its value from configuration;
/// tests and tools without configuration use this one.
impl Default for AttemptDeadlines {
    fn default() -> Self {
        Self {
            budgets: DEFAULT_ATTEMPT_BUDGETS_MS.map(Duration::from_millis),
            commit_grace: Duration::from_millis(DEFAULT_COMMIT_GRACE_MS),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AttemptDeadline {
    pub class: DeadlineClass,
    pub expires_at: Instant,
    pub commit_grace: Duration,
}

impl AttemptDeadline {
    /// Known no-effects expiry, before dispatching another native attempt.
    pub fn require_remaining(&self, phase: AttemptPhase) -> StorageResult<()> {
        if deadline_instant() >= self.expires_at {
            return Err(expired_in(phase));
        }
        Ok(())
    }

    /// The deadline whose expiry is `settle_by`: the bound of a commit that has
    /// begun and of its acknowledgement.
    #[must_use]
    pub fn settle(self) -> Self {
        Self {
            expires_at: self.settle_by(),
            ..self
        }
    }

    /// Completes once this deadline has expired on `clock`. A failed sleep
    /// counts as expiry: the attempt then ends as at its deadline.
    pub async fn expired(self, clock: &dyn crate::Clock) {
        loop {
            let remaining = self.remaining();
            if remaining.is_zero() || clock.sleep(remaining).await.is_err() {
                return;
            }
        }
    }

    #[must_use]
    pub fn remaining(&self) -> Duration {
        self.expires_at
            .saturating_duration_since(deadline_instant())
    }

    /// The latest instant at which a commit that began before `expires_at`
    /// can still report its outcome.
    #[must_use]
    pub fn settle_by(&self) -> Instant {
        self.expires_at + self.commit_grace
    }
}

fn deadline_instant() -> Instant {
    crate::ambient::source_instant()
        .map(Instant::from_std)
        .unwrap_or_else(Instant::now)
}

/// Known no-effects expiry in `phase`: the one constructor of a deadline error.
#[must_use]
pub fn expired_in(phase: AttemptPhase) -> StorageError {
    AttemptFailure::Unavailable(NativeUnavailable::Deadline(phase)).into()
}

/// Whether an attempt waits for the disk before it reports success.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AttemptDurability {
    #[default]
    Durable,
    Relaxed,
}

/// Whether protected observations may use a locally cached read version.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReadVersionPolicy {
    #[default]
    Fresh,
    Cached,
}

/// The admission work of one native attempt. A store in the foreground lane
/// takes a permit of this work; a store in the background lane takes a free
/// slot (`Background`), or the recovery reserve for `Recovery`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeWork {
    Recovery,
    PointRead,
    RangeRead,
    Write,
    Background,
}

impl NativeWork {
    /// The work of a request that names none: reads are point reads, writes are
    /// writes.
    #[must_use]
    pub const fn of_mode(mode: AttemptMode) -> Self {
        match mode {
            AttemptMode::ReadOnly => Self::PointRead,
            AttemptMode::ReadWrite | AttemptMode::ReadWriteItems => Self::Write,
        }
    }
}

/// What a native attempt protects beyond its mode. Postgres maps it to its
/// isolation and the stream topology fence (`PostgresTransaction::of`); the
/// other engines ignore it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptScope {
    /// The lending port's own scope: a Dynamo or stream port of a SQL store
    /// lends `Data`; every other port lends an attempt without the fence.
    Default,
    /// A catalog or schema change of a Dynamo port: no topology fence.
    Schema,
    /// Dynamo item or stream data: writers hold the shared topology fence.
    Data,
    /// `Data` whose staged item writes are the step's last native calls
    /// (Postgres sends them with COMMIT).
    ItemWrite,
    /// A stream topology change: drains `Data` writers.
    Topology,
}

/// One native attempt's request: an `AttemptPlan` whose operation deadline has
/// started.
#[derive(Clone, Copy, Debug)]
pub struct AttemptRequest {
    pub read_version: ReadVersionPolicy,
    pub mode: AttemptMode,
    pub scope: AttemptScope,
    pub deadline: AttemptDeadline,
    pub durability: AttemptDurability,
    /// Tries of a domain `run`; drivers ignore it.
    pub retry: WriteRetryPolicy,
    /// Admission work in the store's lane; drivers ignore it.
    pub work: NativeWork,
}

/// The template of one operation's attempts: a `const` of the domain that owns
/// the operation. It names the budget class; the deadline starts once, when the
/// operation's retry owner or one-try caller calls `start`. Drivers never start
/// a deadline.
#[derive(Clone, Copy, Debug)]
pub struct AttemptPlan {
    pub read_version: ReadVersionPolicy,
    pub mode: AttemptMode,
    pub scope: AttemptScope,
    pub class: DeadlineClass,
    pub durability: AttemptDurability,
    /// Tries of a domain `run`; drivers ignore it.
    pub retry: WriteRetryPolicy,
    /// Admission work in the store's lane; drivers ignore it.
    pub work: NativeWork,
}

impl AttemptPlan {
    /// A durable plan of `mode` with the budget of `class`.
    #[must_use]
    pub const fn new(mode: AttemptMode, class: DeadlineClass) -> Self {
        Self {
            mode,
            scope: AttemptScope::Default,
            class,
            read_version: ReadVersionPolicy::Fresh,
            durability: AttemptDurability::Durable,
            retry: WriteRetryPolicy::no_retry(),
            work: NativeWork::of_mode(mode),
        }
    }

    /// The same plan with another scope.
    #[must_use]
    pub const fn scoped(self, scope: AttemptScope) -> Self {
        Self { scope, ..self }
    }

    #[must_use]
    pub const fn retrying(self, retry: WriteRetryPolicy) -> Self {
        Self { retry, ..self }
    }

    #[must_use]
    pub const fn with_durability(self, durability: AttemptDurability) -> Self {
        Self { durability, ..self }
    }

    /// The same plan with another admission work (range reads, recovery).
    #[must_use]
    pub const fn work(self, work: NativeWork) -> Self {
        Self { work, ..self }
    }

    /// Start the operation deadline now, from `budgets` (the budgets of the
    /// backend that runs the attempts). Called once per operation.
    #[must_use]
    pub fn start(self, budgets: AttemptDeadlines) -> AttemptRequest {
        self.started(budgets.start(self.class))
    }

    /// This plan under an operation deadline that its owner already started;
    /// the request's class is `deadline.class`.
    #[must_use]
    pub const fn started(self, deadline: AttemptDeadline) -> AttemptRequest {
        AttemptRequest {
            read_version: self.read_version,
            mode: self.mode,
            scope: self.scope,
            deadline,
            durability: self.durability,
            retry: self.retry,
            work: self.work,
        }
    }
}

/// The facts of one try of a retried operation; fresh for every try.
#[derive(Clone, Copy, Debug)]
pub struct AttemptTry {
    /// 1 for the first try.
    pub number: u32,
    /// The port clock read just before this try.
    pub started_at: TimestampMillis,
}

impl AttemptTry {
    #[must_use]
    pub const fn is_first(self) -> bool {
        self.number == 1
    }
}

impl AttemptRequest {
    /// The same request with another scope.
    #[must_use]
    pub const fn scoped(self, scope: AttemptScope) -> Self {
        Self { scope, ..self }
    }

    /// The scope a port lends when the caller left `Default`.
    #[must_use]
    pub const fn with_default_scope(self, scope: AttemptScope) -> Self {
        match self.scope {
            AttemptScope::Default => Self { scope, ..self },
            _ => self,
        }
    }

    #[must_use]
    pub const fn class(&self) -> DeadlineClass {
        self.deadline.class
    }
}
