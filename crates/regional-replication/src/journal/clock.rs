//! Clock warnings never change accepted HLC ordering or authorize retention.

use replication_provider::{
    ordering::clock::{CapturedClockPlan, ClockFloor},
    regional_application::error::RegionalApplicationError,
};
use replication_types::event::order::HybridLogicalClock;
use storage_types::StorageResult;

use super::JournalStore;

impl JournalStore {
    pub(in crate::journal) fn issue_regional_clock(
        &self,
        local: (HybridLogicalClock, u64),
        observed: Option<HybridLogicalClock>,
    ) -> StorageResult<HybridLogicalClock> {
        let clock = CapturedClockPlan {
            wall_ms: local.1,
            local: ClockFloor::Recovered(local.0),
            received: observed,
        }
        .issue_clock()
        .map_err(RegionalApplicationError::from)?;
        self.report_clock_skew(clock, i128::from(local.1));
        Ok(clock)
    }

    pub(in crate::journal) fn report_clock_skew(&self, clock: HybridLogicalClock, wall_ms: i128) {
        let ahead = i128::from(clock.physical_ms).saturating_sub(wall_ms).max(0);
        let ahead = u64::try_from(ahead).unwrap_or(u64::MAX);
        if ahead > self.clock_alert_ms.get() {
            tracing::warn!(
                clock_skew_ms = ahead,
                clock_alert_ms = self.clock_alert_ms.get(),
                "regional HLC is ahead of the local wall clock"
            );
        }
    }
}
