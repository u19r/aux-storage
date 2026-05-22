use std::ops::{Add, Deref};

use serde::{Deserialize, Serialize};

use crate::TimestampMillis;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurationSeconds(u32);

impl DurationSeconds {
    #[must_use]
    pub fn time_from_now(&self) -> TimestampMillis {
        let now = chrono::Utc::now().timestamp_millis();
        TimestampMillis::from_timestamp(now + i64::from(self.0) * 1000)
    }
}

impl Deref for DurationSeconds {
    type Target = u32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Add<u32> for DurationSeconds {
    type Output = Self;

    fn add(self, rhs: u32) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl std::fmt::Display for DurationSeconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for DurationSeconds {
    fn from(value: u32) -> Self {
        Self(value)
    }
}
