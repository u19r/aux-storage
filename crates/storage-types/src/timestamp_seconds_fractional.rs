use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::TimestampMillis;

/// Unix timestamp in seconds with optional fractional precision (DynamoDB API
/// shape).
#[derive(
    Debug, Clone, Copy, PartialEq, PartialOrd, Serialize, Deserialize, ToSchema, JsonSchema,
)]
#[serde(transparent)]
#[schema(value_type = f64, example = 1_700_000_000.123)]
pub struct TimestampSecondsFractional(f64);

impl TimestampSecondsFractional {
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "Unix epoch milliseconds are represented as fractional seconds for DynamoDB API \
                  compatibility"
    )]
    pub fn now() -> Self {
        Self(chrono::Utc::now().timestamp_millis() as f64 / 1000.0)
    }

    #[must_use]
    pub fn from_seconds(seconds: f64) -> Self {
        Self(seconds)
    }

    #[must_use]
    pub fn as_seconds(self) -> f64 {
        self.0
    }
}

impl From<f64> for TimestampSecondsFractional {
    fn from(value: f64) -> Self {
        Self(value)
    }
}

impl From<TimestampSecondsFractional> for f64 {
    fn from(value: TimestampSecondsFractional) -> Self {
        value.0
    }
}

impl From<TimestampMillis> for TimestampSecondsFractional {
    #[expect(
        clippy::cast_precision_loss,
        reason = "Unix epoch milliseconds are represented as fractional seconds for DynamoDB API \
                  compatibility"
    )]
    fn from(value: TimestampMillis) -> Self {
        Self(value.timestamp_millis() as f64 / 1000.0)
    }
}

impl From<TimestampSecondsFractional> for TimestampMillis {
    #[expect(
        clippy::cast_precision_loss,
        reason = "Range checks use f64 bounds before conversion to i64 milliseconds"
    )]
    #[expect(
        clippy::cast_possible_truncation,
        reason = "Fractional seconds are intentionally floored to whole milliseconds"
    )]
    fn from(value: TimestampSecondsFractional) -> Self {
        let millis = value.0 * 1000.0;
        if !millis.is_finite() {
            return TimestampMillis::from_timestamp(0);
        }

        let floored = millis.floor();
        if floored <= i64::MIN as f64 {
            return TimestampMillis::from_timestamp(i64::MIN);
        }
        if floored >= i64::MAX as f64 {
            return TimestampMillis::from_timestamp(i64::MAX);
        }

        TimestampMillis::from_timestamp(floored as i64)
    }
}
