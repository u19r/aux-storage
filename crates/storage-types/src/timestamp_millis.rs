use std::ops::{Add, Deref, Sub};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::DurationSeconds;

#[derive(
    Debug,
    Clone,
    PartialEq,
    PartialOrd,
    Copy,
    Serialize,
    Deserialize,
    Eq,
    Ord,
    Hash,
    ToSchema,
    JsonSchema,
)]
pub struct TimestampMillis(i64);

impl TimestampMillis {
    #[must_use]
    pub fn now() -> Self {
        Self(Utc::now().timestamp_millis())
    }

    #[must_use]
    pub fn from_timestamp(timestamp: i64) -> Self {
        Self(timestamp)
    }

    #[must_use]
    pub fn timestamp(self) -> i64 {
        self.0.div_euclid(1000)
    }

    #[must_use]
    pub fn timestamp_millis(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn to_rfc3339(self) -> String {
        DateTime::<Utc>::from(self).to_rfc3339()
    }

    #[must_use]
    pub fn checked_add_signed(self, duration: ChronoDuration) -> Option<Self> {
        self.0
            .checked_add(duration.num_milliseconds())
            .map(Self::from_timestamp)
    }

    #[must_use]
    pub fn add_duration_seconds(&self, seconds: &DurationSeconds) -> Self {
        Self(self.0 + i64::from(**seconds) * 1000)
    }
}

impl Default for TimestampMillis {
    fn default() -> Self {
        Self::now()
    }
}

impl Add<i64> for TimestampMillis {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        Self(self.0 + rhs)
    }
}

impl Sub<i64> for TimestampMillis {
    type Output = Self;

    fn sub(self, rhs: i64) -> Self::Output {
        Self(self.0 - rhs)
    }
}

impl Add<DurationSeconds> for TimestampMillis {
    type Output = Self;

    fn add(self, rhs: DurationSeconds) -> Self::Output {
        Self::from_timestamp(self.0 + i64::from(*rhs) * 1000)
    }
}

impl Add<ChronoDuration> for TimestampMillis {
    type Output = Self;

    fn add(self, rhs: ChronoDuration) -> Self::Output {
        Self::from_timestamp(self.0 + rhs.num_milliseconds())
    }
}

impl Sub<ChronoDuration> for TimestampMillis {
    type Output = Self;

    fn sub(self, rhs: ChronoDuration) -> Self::Output {
        Self::from_timestamp(self.0 - rhs.num_milliseconds())
    }
}

impl Deref for TimestampMillis {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::fmt::Display for TimestampMillis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write_rfc3339_utc(
            DateTime::from_timestamp_millis(self.0).unwrap_or_else(Utc::now),
            f,
        )
    }
}

pub(crate) fn write_rfc3339_utc(
    datetime: DateTime<Utc>,
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let year = datetime.year();
    if (0..=9999).contains(&year) {
        write!(formatter, "{year:04}")?;
    } else {
        write!(formatter, "{year:+05}")?;
    }
    write!(
        formatter,
        "-{:02}-{:02}T{:02}:{:02}:{:02}",
        datetime.month(),
        datetime.day(),
        datetime.hour(),
        datetime.minute(),
        datetime.second()
    )?;
    let nanos = datetime.nanosecond();
    if nanos != 0 {
        let (fraction, width) = if nanos.is_multiple_of(1_000_000) {
            (nanos / 1_000_000, 3)
        } else if nanos.is_multiple_of(1_000) {
            (nanos / 1_000, 6)
        } else {
            (nanos, 9)
        };
        write!(formatter, ".{fraction:0width$}")?;
    }
    formatter.write_str("+00:00")
}

impl From<&chrono::DateTime<Utc>> for TimestampMillis {
    fn from(value: &chrono::DateTime<Utc>) -> Self {
        Self(value.timestamp_millis())
    }
}

impl From<chrono::DateTime<Utc>> for TimestampMillis {
    fn from(value: chrono::DateTime<Utc>) -> Self {
        Self(value.timestamp_millis())
    }
}

impl From<TimestampMillis> for chrono::DateTime<Utc> {
    fn from(value: TimestampMillis) -> Self {
        DateTime::<Utc>::from_timestamp_millis(value.0).unwrap_or_else(Utc::now)
    }
}

impl From<i64> for TimestampMillis {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

#[must_use]
pub fn timestamp_bounds<T, U>(
    since: Option<T>,
    until: Option<U>,
) -> (TimestampMillis, TimestampMillis)
where
    T: Into<TimestampMillis>,
    U: Into<TimestampMillis>,
{
    let start = since
        .map(Into::into)
        .unwrap_or_else(|| TimestampMillis::from_timestamp(i64::MIN));
    let end = until
        .map(Into::into)
        .unwrap_or_else(|| TimestampMillis::from_timestamp(i64::MAX));
    (start, end)
}
