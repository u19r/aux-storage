use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicUsize, Ordering},
};

use serde::{Deserialize, Serialize};

/// Cumulative driver-visible data and call-guard admission for one native
/// attempt. These are enforced budgets, not estimates of SDK heap or physical
/// settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWorkLimits {
    pub data_bytes: NonZeroUsize,
    pub call_guards: NonZeroUsize,
}

/// The work budgets bound once to an initialized driver.
#[derive(Debug, Default)]
pub struct BoundWorkLimits(std::sync::OnceLock<NativeWorkLimits>);

impl BoundWorkLimits {
    pub fn bind(&self, limits: NativeWorkLimits) -> crate::StorageResult<()> {
        self.0
            .set(limits)
            .map_err(|_| crate::StorageError::validation("native work budgets are already bound"))
    }

    #[must_use]
    pub fn get(&self) -> Option<NativeWorkLimits> {
        self.0.get().copied()
    }
}

#[derive(Default)]
pub(crate) struct NativeWorkBudget {
    limits: Option<NativeWorkLimits>,
    bytes: AtomicUsize,
    calls: AtomicUsize,
}

impl NativeWorkBudget {
    pub(crate) fn new(limits: Option<NativeWorkLimits>) -> Self {
        Self {
            limits,
            bytes: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
        }
    }

    pub(crate) fn charge(&self, bytes: usize, calls: usize) -> crate::StorageResult<()> {
        let Some(limits) = self.limits else {
            return Ok(());
        };
        self.charge_counter(&self.bytes, bytes, limits.data_bytes)?;
        self.charge_counter(&self.calls, calls, limits.call_guards)
    }

    fn charge_counter(
        &self,
        counter: &AtomicUsize,
        amount: usize,
        maximum: NonZeroUsize,
    ) -> crate::StorageResult<()> {
        counter
            .try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(amount)
                    .filter(|total| *total <= maximum.get())
            })
            .map(|_| ())
            .map_err(|_| crate::AttemptFailure::CAPACITY.into())
    }
}
