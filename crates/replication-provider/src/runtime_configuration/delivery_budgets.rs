use std::{num::NonZeroUsize, time::Duration};

use super::{deployment::TimingConfiguration, error::ConfigurationError};
use crate::constants::{
    DELIVERY_EXCHANGES_PER_FRAME_EVENT, DELIVERY_JOB_PHASES, DELIVERY_SOURCE_PHASES,
};

/// Finite complete-source delivery budgets; individual requests remain
/// separately bounded.
#[derive(Clone, Copy, Debug)]
pub struct DeliveryBudgets {
    transfer: Duration,
    source: Duration,
    job: Duration,
}

impl DeliveryBudgets {
    pub(super) fn try_new(
        timing: &TimingConfiguration,
        events: NonZeroUsize,
    ) -> Result<Self, ConfigurationError> {
        let invalid = || ConfigurationError::Invalid("complete delivery exchange count overflowed");
        let exchanges = u32::try_from(events.get())
            .ok()
            .and_then(|events| events.checked_mul(DELIVERY_EXCHANGES_PER_FRAME_EVENT))
            .ok_or_else(invalid)?;
        let source = exchanges
            .checked_add(DELIVERY_SOURCE_PHASES)
            .ok_or_else(invalid)?;
        let job = exchanges
            .checked_add(DELIVERY_JOB_PHASES)
            .ok_or_else(invalid)?;
        Ok(Self {
            transfer: timing.request_budget(exchanges)?,
            source: timing.request_budget(source)?,
            job: timing.request_budget(job)?,
        })
    }

    pub fn transfer(self) -> Duration {
        self.transfer
    }
    pub fn source(self) -> Duration {
        self.source
    }
    pub fn job(self) -> Duration {
        self.job
    }
}
