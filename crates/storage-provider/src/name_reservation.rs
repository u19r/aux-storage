//! A short name reservation after deleting a table or secondary index.

use storage_types::TimestampMillis;

use crate::constants::NAME_REUSE_DELAY_MILLIS;

#[derive(Clone, Copy)]
pub enum TableNameReservation {
    Available,
    Active,
    LockedUntil(TimestampMillis),
}

impl TableNameReservation {
    pub fn is_available(self, now: TimestampMillis) -> bool {
        match self {
            Self::Available => true,
            Self::Active => false,
            Self::LockedUntil(until) => !name_is_locked(now, until),
        }
    }
}

pub fn name_locked_until(now: TimestampMillis) -> TimestampMillis {
    now + NAME_REUSE_DELAY_MILLIS
}

pub fn name_is_locked(now: TimestampMillis, locked_until: TimestampMillis) -> bool {
    now < locked_until
}
