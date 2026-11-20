/// Physical attempt scheduling of an initialized native driver.
/// This report does not qualify transaction bytes or guarantee lock fairness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeAttemptCapacity {
    /// Every native attempt exclusively borrows one interchangeable connection.
    SharedPool { connections: std::num::NonZeroUsize },
    /// Writes share one exclusive writer owner (connection or queue); readers
    /// may use others.
    SerializedWriter,
    /// Attempts are constructed independently without a shared connection pool.
    Independent,
}

impl NativeAttemptCapacity {
    /// A shared pool must retain foreground connections after recovery
    /// reservation. Other scheduling shapes do not establish bytes,
    /// duration or fairness.
    pub fn require_foreground_attempts(
        self,
        reserved: usize,
        minimum: usize,
    ) -> crate::StorageResult<()> {
        let Self::SharedPool { connections } = self else {
            return Ok(());
        };
        let foreground = connections.get().checked_sub(reserved).ok_or_else(|| {
            crate::StorageError::validation("native recovery reserve exceeds pool capacity")
        })?;
        if foreground < minimum {
            return Err(crate::StorageError::validation(format!(
                "native foreground pool capacity ({foreground}) is below admission \
                 minimum_concurrency ({minimum})"
            )));
        }
        Ok(())
    }
}

impl crate::StorageError {
    /// Native owners use this after operation failure, without replacing an
    /// unknown outcome. The owner must still acknowledge abort before returning
    /// it.
    pub fn preserve_native_settlement(self, settlement: crate::StorageResult<()>) -> Self {
        if self.is_commit_unknown() {
            return self;
        }
        match settlement {
            Err(error)
                if !matches!(
                    error.attempt_kind(),
                    Some(crate::AttemptKind::Conflict | crate::AttemptKind::FreshValidation)
                ) && !error.is_attempt_abandoned() =>
            {
                error
            }
            _ => self,
        }
    }
}

/// A workflow failure may follow native writes even when its caller catches it.
/// Rejection is irreversible for this attempt; a retry needs a fresh attempt.
pub struct NativeAttemptSettlement {
    /// False once any verdict other than `Open` was merged: the lock-free
    /// check of every native call.
    open: std::sync::atomic::AtomicBool,
    verdict: std::sync::Mutex<AttemptVerdict>,
    work: crate::native_work::NativeWorkBudget,
}

impl Default for NativeAttemptSettlement {
    fn default() -> Self {
        Self::new(None)
    }
}

/// What a rejected attempt reports. `strength` orders the verdicts; `merge`
/// keeps the stronger one and, at equal strength, the first. Each verdict
/// keeps the first error that set it, so its cause and phase survive.
#[derive(Clone, Debug)]
enum AttemptVerdict {
    Open,
    /// A call failed without a classified error or was abandoned.
    Abandoned,
    /// A conflict or fresh-validation request: the retry owner decides.
    Conflict(crate::StorageError),
    /// A native deadline or lost service. `first_deadline` is the first
    /// deadline error (it names its phase for the caller and the metric).
    Unavailable {
        first: crate::StorageError,
        first_deadline: Option<crate::StorageError>,
    },
    Capacity(crate::StorageError),
    TransactionBytes(crate::StorageError),
    /// An error that is no attempt failure (validation, internal): the
    /// caller sees it.
    Permanent(crate::StorageError),
    Unknown(crate::StorageError),
}

impl AttemptVerdict {
    const fn strength(&self) -> u8 {
        match self {
            Self::Open => 0,
            Self::Abandoned => 1,
            Self::Conflict(_) => 2,
            Self::Unavailable { .. } => 3,
            Self::Capacity(_) => 4,
            Self::TransactionBytes(_) => 5,
            Self::Permanent(_) => 6,
            Self::Unknown(_) => 7,
        }
    }

    /// The verdict that one failed call contributes.
    fn of_error(error: &crate::StorageError) -> Self {
        if error.is_attempt_abandoned() {
            return Self::Abandoned;
        }
        let Some(kind) = error.attempt_kind() else {
            return Self::Permanent(error.clone());
        };
        match kind {
            crate::AttemptKind::CommitUnknown => Self::Unknown(error.clone()),
            crate::AttemptKind::TransactionBytes => Self::TransactionBytes(error.clone()),
            crate::AttemptKind::Capacity => Self::Capacity(error.clone()),
            crate::AttemptKind::Deadline => Self::Unavailable {
                first: error.clone(),
                first_deadline: Some(error.clone()),
            },
            crate::AttemptKind::Unavailable => Self::Unavailable {
                first: error.clone(),
                first_deadline: None,
            },
            crate::AttemptKind::Conflict | crate::AttemptKind::FreshValidation => {
                Self::Conflict(error.clone())
            }
        }
    }

    fn merge(self, other: Self) -> Self {
        match (self, other) {
            // A deadline after another unavailability still names its phase.
            (
                Self::Unavailable {
                    first,
                    first_deadline: None,
                },
                Self::Unavailable {
                    first_deadline: Some(deadline),
                    ..
                },
            ) => Self::Unavailable {
                first,
                first_deadline: Some(deadline),
            },
            (current, other) if other.strength() > current.strength() => other,
            (current, _) => current,
        }
    }

    fn require_open(&self) -> crate::StorageResult<()> {
        match self {
            Self::Open => Ok(()),
            Self::Abandoned => Err(crate::StorageError::attempt_abandoned()),
            Self::Conflict(error)
            | Self::Capacity(error)
            | Self::TransactionBytes(error)
            | Self::Permanent(error)
            | Self::Unknown(error) => Err(error.clone()),
            Self::Unavailable {
                first,
                first_deadline,
            } => Err(first_deadline.as_ref().unwrap_or(first).clone()),
        }
    }
}

impl NativeAttemptSettlement {
    pub fn new(limits: Option<crate::NativeWorkLimits>) -> Self {
        Self {
            open: std::sync::atomic::AtomicBool::new(true),
            verdict: std::sync::Mutex::new(AttemptVerdict::Open),
            work: crate::native_work::NativeWorkBudget::new(limits),
        }
    }

    /// Charge cumulative serialized inputs/observations before exposing more
    /// work. Exhaustion irreversibly rejects this attempt, including when
    /// caught by its caller.
    pub fn charge_data(&self, bytes: usize) -> crate::StorageResult<()> {
        self.require_open()?;
        self.charge_work(bytes, 0)
    }

    pub fn charge_calls(&self, calls: usize) -> crate::StorageResult<()> {
        self.require_open()?;
        self.charge_work(0, calls)
    }

    fn charge_work(&self, bytes: usize, calls: usize) -> crate::StorageResult<()> {
        let result = self.work.charge(bytes, calls);
        if let Err(error) = &result {
            self.reject_error(error);
        }
        result
    }

    pub fn reject(&self) {
        self.merge(AttemptVerdict::Abandoned);
    }

    /// Preserve a synchronous preparation failure before native dispatch.
    /// The owning attempt must still acknowledge abort before reporting
    /// nonacceptance.
    pub fn reject_error(&self, error: &crate::StorageError) {
        self.merge(AttemptVerdict::of_error(error));
    }

    fn merge(&self, verdict: AttemptVerdict) {
        let mut current = self
            .verdict
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *current = std::mem::replace(&mut *current, AttemptVerdict::Open).merge(verdict);
        self.open.store(false, std::sync::atomic::Ordering::Release);
    }

    /// Native calls may observe concurrently. Any failed or abandoned call
    /// rejects their shared attempt; successful calls never reset rejection.
    pub fn begin_call(&self) -> crate::StorageResult<NativeAttemptCall<'_>> {
        self.require_open()?;
        self.charge_work(0, 1)?;
        Ok(NativeAttemptCall {
            settlement: Some(self),
        })
    }

    pub fn require_open(&self) -> crate::StorageResult<()> {
        if self.open.load(std::sync::atomic::Ordering::Acquire) {
            return Ok(());
        }
        self.verdict
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .require_open()
    }
}

/// A native call's borrow also owns rejection on error or cancellation.
pub struct NativeAttemptCall<'attempt> {
    settlement: Option<&'attempt NativeAttemptSettlement>,
}

impl NativeAttemptCall<'_> {
    pub fn complete<R>(mut self, result: crate::StorageResult<R>) -> crate::StorageResult<R> {
        if let Err(error) = &result
            && let Some(settlement) = self.settlement
        {
            settlement.reject_error(error);
        }
        self.settlement = None;
        result
    }
}

impl Drop for NativeAttemptCall<'_> {
    fn drop(&mut self) {
        if let Some(settlement) = &mut self.settlement {
            settlement.reject();
        }
    }
}

/// Native transaction capability shared by SQL and KV backends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptMode {
    /// A coherent native snapshot. Providers currently serve both Dynamo
    /// read modes from this strong snapshot; an eventually consistent replica
    /// would need a separate capability rather than weakening this mode.
    ReadOnly,
    ReadWrite,
    /// Write or check a bounded set of exact items. Providers may use statement
    /// snapshots (Postgres `READ COMMITTED`) when every write targets a row
    /// that an earlier protected read of the same attempt holds, or uses a
    /// conditional insert whose miss is a conflict; other providers retain
    /// their native write isolation.
    ReadWriteItems,
}

impl AttemptMode {
    #[must_use]
    pub const fn read_protection(self) -> ReadProtection {
        match self {
            Self::ReadOnly => ReadProtection::Snapshot,
            Self::ReadWrite | Self::ReadWriteItems => ReadProtection::Conflict,
        }
    }

    pub fn require_write(self) -> crate::StorageResult<()> {
        match self {
            Self::ReadWrite | Self::ReadWriteItems => Ok(()),
            Self::ReadOnly => Err(crate::StorageError::internal(
                "native write requires a read-write attempt",
            )),
        }
    }
}

/// Whether a physical read contributes a conflict dependency to a write.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReadProtection {
    Snapshot,
    /// Protect a read without excluding another protected reader. SQL uses a
    /// shared row lock; KV uses its ordinary conflict read.
    Shared,
    Conflict,
    /// Protect for write as `Conflict` does, but omit a row that another
    /// attempt protects now. Postgres renders `FOR UPDATE SKIP LOCKED`; other
    /// providers treat it as `Conflict`.
    Claim,
}

impl ReadProtection {
    /// Preserve a coherent snapshot or protect an existing row from retirement
    /// while allowing other protected readers in this same native attempt.
    #[must_use]
    pub const fn shared(self) -> Self {
        match self {
            Self::Snapshot => Self::Snapshot,
            Self::Shared | Self::Conflict | Self::Claim => Self::Shared,
        }
    }

    pub fn validate(self, mode: AttemptMode) -> crate::StorageResult<()> {
        match self {
            Self::Snapshot => Ok(()),
            Self::Shared | Self::Conflict | Self::Claim => mode.require_write(),
        }
    }
}
