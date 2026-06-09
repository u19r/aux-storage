use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use utoipa::ToSchema;

pub const MAX_STREAM_RETENTION_HOURS: u16 = 24 * 365 * 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ToSchema)]
pub enum StreamRetentionDuration {
    Forever,
    FiniteHours(u16),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamRetentionDurationError {
    Zero,
    Negative(i64),
    TooLarge { hours: i64, max_hours: u16 },
}

impl StreamRetentionDuration {
    pub const DEFAULT_TABLE_STREAM_DURATION: Self = Self::FiniteHours(72);

    pub fn from_hours(hours: i64) -> Result<Self, StreamRetentionDurationError> {
        match hours {
            -1 => Ok(Self::Forever),
            0 => Err(StreamRetentionDurationError::Zero),
            value if value < -1 => Err(StreamRetentionDurationError::Negative(value)),
            value if value > i64::from(MAX_STREAM_RETENTION_HOURS) => {
                Err(StreamRetentionDurationError::TooLarge {
                    hours: value,
                    max_hours: MAX_STREAM_RETENTION_HOURS,
                })
            }
            value => Ok(Self::FiniteHours(value as u16)),
        }
    }

    pub fn as_hours_wire_value(self) -> i64 {
        match self {
            Self::Forever => -1,
            Self::FiniteHours(hours) => i64::from(hours),
        }
    }

    pub fn effective_item_retention(table: Self, item: Self) -> Self {
        match (table, item) {
            (Self::Forever, _) | (_, Self::Forever) => Self::Forever,
            (Self::FiniteHours(table_hours), Self::FiniteHours(item_hours)) => {
                Self::FiniteHours(table_hours.max(item_hours))
            }
        }
    }
}

impl Default for StreamRetentionDuration {
    fn default() -> Self {
        Self::DEFAULT_TABLE_STREAM_DURATION
    }
}

impl TryFrom<i64> for StreamRetentionDuration {
    type Error = StreamRetentionDurationError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::from_hours(value)
    }
}

impl fmt::Display for StreamRetentionDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forever => f.write_str("-1"),
            Self::FiniteHours(hours) => write!(f, "{hours}"),
        }
    }
}

impl fmt::Display for StreamRetentionDurationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => {
                f.write_str("stream retention duration must be -1 or a positive integer hour count")
            }
            Self::Negative(value) => write!(
                f,
                "stream retention duration must be -1 or a positive integer hour count, got \
                 {value}"
            ),
            Self::TooLarge { hours, max_hours } => write!(
                f,
                "stream retention duration {hours} exceeds maximum {max_hours} hours"
            ),
        }
    }
}

impl std::error::Error for StreamRetentionDurationError {}

impl Serialize for StreamRetentionDuration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.serialize_i64(self.as_hours_wire_value())
    }
}

impl<'de> Deserialize<'de> for StreamRetentionDuration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let hours = i64::deserialize(deserializer)?;
        Self::from_hours(hours).map_err(serde::de::Error::custom)
    }
}
