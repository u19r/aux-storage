use replication_types::{error::ReplicationProtocolError, event::order::HybridLogicalClock};

use crate::error::{ReplicationDecisionError, ReplicationResult};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClockFloor {
    Unknown,
    Recovered(HybridLogicalClock),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapturedClockPlan {
    pub wall_ms: u64,
    pub local: ClockFloor,
    pub received: Option<HybridLogicalClock>,
}
impl CapturedClockPlan {
    pub fn issue_clock(self) -> ReplicationResult<HybridLogicalClock> {
        let ClockFloor::Recovered(local) = self.local else {
            return Err(ReplicationDecisionError::ClockFloorUnknown {
                context: "clock.issue_clock",
            });
        };
        let received = self.received.unwrap_or(HybridLogicalClock {
            physical_ms: 0,
            logical: 0,
        });
        let physical_ms = self
            .wall_ms
            .max(local.physical_ms)
            .max(received.physical_ms);
        let logical = if physical_ms == local.physical_ms && physical_ms == received.physical_ms {
            checked_increment(local.logical.max(received.logical))?
        } else if physical_ms == local.physical_ms {
            checked_increment(local.logical)?
        } else if physical_ms == received.physical_ms {
            checked_increment(received.logical)?
        } else {
            0
        };
        Ok(HybridLogicalClock {
            physical_ms,
            logical,
        })
    }
    /// Observe before sequencing subsequent same-key/local commands; observing
    /// does not mint an event.
    pub fn observe_clock(self, received: HybridLogicalClock) -> ClockFloor {
        match self.local {
            ClockFloor::Unknown => ClockFloor::Unknown,
            ClockFloor::Recovered(local) => ClockFloor::Recovered(local.max(received)),
        }
    }
}
fn checked_increment(value: u64) -> ReplicationResult<u64> {
    value.checked_add(1).ok_or(
        ReplicationProtocolError::CounterOverflow {
            context: "clock.checked_increment",
        }
        .into(),
    )
}
