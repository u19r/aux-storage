//! Dual elapsed clocks distinguish process retention from suspend and wall
//! steps.
#[derive(Clone, Copy)]
pub(super) struct ClockObservation {
    pub(super) wall: i128,
    lower: i128,
    upper: i128,
    boot: i128,
}

impl ClockObservation {
    #[cfg(target_os = "linux")]
    pub(super) fn sample() -> Option<Self> {
        use rustix::time::{ClockId, clock_gettime};
        let lower = convert_clock(clock_gettime(ClockId::Monotonic))?;
        let wall = convert_clock(clock_gettime(ClockId::Realtime))?;
        let boot = convert_clock(clock_gettime(ClockId::Boottime))?;
        let upper = convert_clock(clock_gettime(ClockId::Monotonic))?;
        if upper < lower {
            return None;
        }
        Some(Self {
            wall,
            lower,
            upper,
            boot,
        })
    }

    #[cfg(not(target_os = "linux"))]
    pub(super) fn sample() -> Option<Self> {
        None
    }

    pub(super) fn elapsed_since(self, earlier: Self) -> Option<(i128, i128)> {
        let lower = self.lower.checked_sub(earlier.upper)?;
        let upper = self.upper.checked_sub(earlier.lower)?;
        let boot = self.boot.checked_sub(earlier.boot)?;
        if lower < 0 || boot < lower || boot > upper {
            return None;
        }
        Some((lower, upper))
    }

    pub(super) fn require_continuity(
        self,
        earlier: Self,
        errors: i128,
        frequency: i128,
    ) -> Option<i128> {
        let (lower, upper) = self.elapsed_since(earlier)?;
        let drift = upper
            .checked_mul(frequency)?
            .checked_add(999_999_999)?
            .checked_div(1_000_000_000)?;
        let wall = self.wall.checked_sub(earlier.wall)?;
        if wall < lower.checked_sub(errors)?.checked_sub(drift)?
            || wall > upper.checked_add(errors)?.checked_add(drift)?
        {
            return None;
        }
        lower.checked_sub(drift).filter(|elapsed| *elapsed >= 0)
    }
}

#[cfg(target_os = "linux")]
fn convert_clock(time: rustix::time::Timespec) -> Option<i128> {
    if time.tv_sec < 0 || !(0..1_000_000_000).contains(&time.tv_nsec) {
        return None;
    }
    i128::from(time.tv_sec)
        .checked_mul(1_000_000_000)?
        .checked_add(i128::from(time.tv_nsec))
}
