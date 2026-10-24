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

/// The native owner has proved that this complete attempt did not commit.
/// Only a conflict permits the shared retry owner to repeat fresh observations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeAttemptRejection {
    Conflict,
    Capacity,
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
                if error.is_commit_unknown()
                    || error.native_attempt_rejection()
                        == Some(NativeAttemptRejection::Capacity) =>
            {
                error
            }
            _ => self,
        }
    }

    pub fn native_attempt_rejection(&self) -> Option<NativeAttemptRejection> {
        match self.as_ref() {
            crate::StorageEnum::TransactionConflict { .. } => {
                Some(NativeAttemptRejection::Conflict)
            }
            crate::StorageEnum::InfrastructureResult(
                crate::InfrastructureResultKind::NativeCapacityRejected,
            ) => Some(NativeAttemptRejection::Capacity),
            _ => None,
        }
    }
}

/// A workflow failure may follow native writes even when its caller catches it.
/// Rejection is irreversible for this attempt; a retry needs a fresh attempt.
#[derive(Default)]
pub struct NativeAttemptSettlement {
    state: std::sync::atomic::AtomicU8,
    work: crate::native_work::NativeWorkBudget,
}

/// Discriminants order irreversible rejection strength for concurrent calls.
#[repr(u8)]
enum SettlementState {
    Open,
    Rejected,
    CapacityRejected,
    UnknownRejected,
}

impl NativeAttemptSettlement {
    pub fn new(limits: Option<crate::NativeWorkLimits>) -> Self {
        Self {
            state: Default::default(),
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
        self.state.fetch_max(
            SettlementState::Rejected as u8,
            std::sync::atomic::Ordering::AcqRel,
        );
    }

    /// Preserve a synchronous preparation failure before native dispatch.
    /// The owning attempt must still acknowledge abort before reporting
    /// nonacceptance.
    pub fn reject_error(&self, error: &crate::StorageError) {
        let state = if error.is_commit_unknown() {
            SettlementState::UnknownRejected
        } else {
            match error.native_attempt_rejection() {
                Some(NativeAttemptRejection::Capacity) => SettlementState::CapacityRejected,
                _ => SettlementState::Rejected,
            }
        };
        self.state
            .fetch_max(state as u8, std::sync::atomic::Ordering::AcqRel);
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
        let state = self.state.load(std::sync::atomic::Ordering::Acquire);
        if state == SettlementState::Open as u8 {
            return Ok(());
        }
        if state == SettlementState::UnknownRejected as u8 {
            return Err(crate::StorageError::sync_submission_ambiguous(
                None,
                "Native attempt outcome is unknown",
            ));
        }
        if state == SettlementState::CapacityRejected as u8 {
            return Err(crate::InfrastructureResultKind::NativeCapacityRejected.into());
        }
        Err(crate::StorageEnum::TransactionConflict {
            message: "Native attempt was rejected before settlement".into(),
        }
        .into())
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
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum AttemptMode {
    /// A coherent native snapshot. Providers currently serve both Dynamo
    /// read modes from this strong snapshot; an eventually consistent replica
    /// would need a separate capability rather than weakening this mode.
    ReadOnly,
    ReadWrite,
    /// Write or check a bounded set of exact items. Providers may use statement
    /// snapshots when they protect all targets, including absent items, before
    /// observing them; other providers retain their native write isolation.
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
}

impl ReadProtection {
    /// Preserve a coherent snapshot or protect an existing row from retirement
    /// while allowing other protected readers in this same native attempt.
    #[must_use]
    pub const fn shared(self) -> Self {
        match self {
            Self::Snapshot => Self::Snapshot,
            Self::Shared | Self::Conflict => Self::Shared,
        }
    }

    pub fn validate(self, mode: AttemptMode) -> crate::StorageResult<()> {
        match self {
            Self::Snapshot => Ok(()),
            Self::Shared | Self::Conflict => mode.require_write(),
        }
    }
}
