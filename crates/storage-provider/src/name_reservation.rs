//! Secondary-index names retain their existing short reuse reservation.

use storage_types::TimestampMillis;

use crate::constants::NAME_REUSE_DELAY_MILLIS;

pub fn name_locked_until(now: TimestampMillis) -> TimestampMillis {
    now + NAME_REUSE_DELAY_MILLIS
}

pub fn name_is_locked(now: TimestampMillis, locked_until: TimestampMillis) -> bool {
    now < locked_until
}
