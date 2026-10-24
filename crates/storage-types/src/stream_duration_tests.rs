use serde_json::json;

use crate::{StreamRetentionDuration, TimestampMillis, constants::MAX_STREAM_RETENTION_HOURS};

#[test]
fn stream_retention_duration_accepts_forever_and_positive_hours() {
    assert_eq!(
        StreamRetentionDuration::try_from(-1),
        Ok(StreamRetentionDuration::Forever)
    );
    assert_eq!(
        StreamRetentionDuration::try_from(1),
        Ok(StreamRetentionDuration::FiniteHours(1))
    );
    assert_eq!(
        StreamRetentionDuration::try_from(i64::from(MAX_STREAM_RETENTION_HOURS)),
        Ok(StreamRetentionDuration::FiniteHours(
            MAX_STREAM_RETENTION_HOURS
        ))
    );
}

#[test]
fn stream_retention_duration_rejects_zero_negative_and_too_large_hours() {
    assert!(StreamRetentionDuration::try_from(0).is_err());
    assert!(StreamRetentionDuration::try_from(-2).is_err());
    assert!(StreamRetentionDuration::try_from(i64::from(MAX_STREAM_RETENTION_HOURS) + 1).is_err());
}

#[test]
fn stream_retention_duration_rejects_fractional_and_string_values() {
    assert!(serde_json::from_value::<StreamRetentionDuration>(json!(1.5)).is_err());
    assert!(serde_json::from_value::<StreamRetentionDuration>(json!("72")).is_err());
}

#[test]
fn effective_item_retention_uses_max_and_forever_dominates() {
    assert_eq!(
        StreamRetentionDuration::effective_item_retention(
            StreamRetentionDuration::FiniteHours(72),
            StreamRetentionDuration::FiniteHours(1),
        ),
        StreamRetentionDuration::FiniteHours(72)
    );
    assert_eq!(
        StreamRetentionDuration::effective_item_retention(
            StreamRetentionDuration::Forever,
            StreamRetentionDuration::FiniteHours(1),
        ),
        StreamRetentionDuration::Forever
    );
    assert_eq!(
        StreamRetentionDuration::effective_item_retention(
            StreamRetentionDuration::FiniteHours(1),
            StreamRetentionDuration::Forever,
        ),
        StreamRetentionDuration::Forever
    );
}

#[test]
fn finite_retention_computes_its_cutoff_and_forever_has_none() {
    let reference = TimestampMillis::from_timestamp(7_200_001);
    assert_eq!(
        StreamRetentionDuration::FiniteHours(2).cutoff_at(reference),
        Some(TimestampMillis::from_timestamp(1))
    );
    assert_eq!(StreamRetentionDuration::Forever.cutoff_at(reference), None);
}
