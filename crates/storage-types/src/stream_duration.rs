use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{TimestampMillis, constants::MAX_STREAM_RETENTION_HOURS};

/// Finite table retention is always at least the fixed system-stream window.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamRetentionHours(u16);

impl From<StreamRetentionHours> for i64 {
    fn from(hours: StreamRetentionHours) -> Self {
        Self::from(hours.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamRetentionDuration {
    Forever,
    FiniteHours(StreamRetentionHours),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRetentionDurationError(i64);

impl StreamRetentionDuration {
    pub const DEFAULT_TABLE_STREAM_DURATION: Self = Self::FiniteHours(StreamRetentionHours(72));

    pub fn from_hours(hours: i64) -> Result<Self, StreamRetentionDurationError> {
        match hours {
            -1 => Ok(Self::Forever),
            value if (24..=i64::from(MAX_STREAM_RETENTION_HOURS)).contains(&value) => {
                Ok(Self::FiniteHours(StreamRetentionHours(value as u16)))
            }
            _ => Err(StreamRetentionDurationError(hours)),
        }
    }

    pub fn as_hours_wire_value(self) -> i64 {
        match self {
            Self::Forever => -1,
            Self::FiniteHours(hours) => hours.into(),
        }
    }

    pub fn cutoff_at(self, reference: TimestampMillis) -> Option<TimestampMillis> {
        match self {
            Self::Forever => None,
            Self::FiniteHours(hours) => Some(reference - i64::from(hours) * 3600 * 1000),
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
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_hours_wire_value().fmt(formatter)
    }
}

impl fmt::Display for StreamRetentionDurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "stream retention duration must be -1 or between 24 and {MAX_STREAM_RETENTION_HOURS} \
             hours, got {}",
            self.0
        )
    }
}

impl std::error::Error for StreamRetentionDurationError {}

impl utoipa::PartialSchema for StreamRetentionDuration {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::{ObjectBuilder, OneOfBuilder, Type};

        OneOfBuilder::new()
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .enum_values(Some([-1])),
            )
            .item(
                ObjectBuilder::new()
                    .schema_type(Type::Integer)
                    .minimum(Some(24))
                    .maximum(Some(MAX_STREAM_RETENTION_HOURS)),
            )
            .into()
    }
}

impl utoipa::ToSchema for StreamRetentionDuration {}

impl Serialize for StreamRetentionDuration {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.as_hours_wire_value())
    }
}

impl<'de> Deserialize<'de> for StreamRetentionDuration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::from_hours(i64::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
