use std::ops::{Add, Deref};

use jiff::{SignedDuration, Timestamp};
use schemars::JsonSchema;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use utoipa::ToSchema;

use crate::{TimestampMillis, timestamp_millis::write_rfc3339_utc};
#[derive(Debug, Clone, PartialEq, PartialOrd, Copy, Eq, Ord, Hash, ToSchema, JsonSchema)]
#[schema(value_type = i64, example = 1_700_000_000)]
pub struct TimestampSeconds(i64);

impl TimestampSeconds {
    #[must_use]
    pub fn now() -> Self {
        Self(crate::ambient::now_millis().div_euclid(1_000))
    }

    #[must_use]
    pub fn from_timestamp(timestamp: i64) -> Self {
        Self(timestamp)
    }

    #[must_use]
    pub fn as_seconds(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn timestamp(self) -> i64 {
        self.0
    }

    #[must_use]
    pub fn timestamp_millis(self) -> i64 {
        self.0.saturating_mul(1000)
    }

    #[must_use]
    pub fn to_rfc3339(self) -> String {
        self.to_string()
    }
}

impl Deref for TimestampSeconds {
    type Target = i64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Add<i64> for TimestampSeconds {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        Self(self.0.saturating_add(rhs))
    }
}

impl Add<SignedDuration> for TimestampSeconds {
    type Output = Self;

    fn add(self, rhs: SignedDuration) -> Self::Output {
        Self(self.0.saturating_add(rhs.as_secs()))
    }
}

impl std::fmt::Display for TimestampSeconds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datetime = Timestamp::from_second(self.0).unwrap_or_else(|_| Timestamp::now());
        write_rfc3339_utc(datetime, f)
    }
}

impl From<&Timestamp> for TimestampSeconds {
    fn from(value: &Timestamp) -> Self {
        Self(value.as_second() - i64::from(value.subsec_nanosecond() < 0))
    }
}

impl From<Timestamp> for TimestampSeconds {
    fn from(value: Timestamp) -> Self {
        TimestampSeconds::from(&value)
    }
}

impl From<TimestampMillis> for TimestampSeconds {
    fn from(value: TimestampMillis) -> Self {
        Self(millis_to_seconds_floor(*value))
    }
}

impl From<i64> for TimestampSeconds {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<u64> for TimestampSeconds {
    fn from(value: u64) -> Self {
        Self(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<TimestampSeconds> for TimestampMillis {
    fn from(value: TimestampSeconds) -> Self {
        TimestampMillis::from_timestamp(value.0.saturating_mul(1000))
    }
}

impl Serialize for TimestampSeconds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for TimestampSeconds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        struct TimestampVisitor;

        impl de::Visitor<'_> for TimestampVisitor {
            type Value = TimestampSeconds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("unix timestamp in whole seconds")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where E: de::Error {
                Ok(TimestampSeconds(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where E: de::Error {
                let value =
                    i64::try_from(value).map_err(|_| E::custom("timestamp exceeds i64 range"))?;
                Ok(TimestampSeconds(value))
            }
        }

        deserializer.deserialize_any(TimestampVisitor)
    }
}

fn millis_to_seconds_floor(value: i64) -> i64 {
    value.div_euclid(1000)
}
