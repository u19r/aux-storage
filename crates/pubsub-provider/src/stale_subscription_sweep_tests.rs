use std::{num::NonZeroU32, time::Duration};

use scheduled_bg_jobs::{JobOutcome, Timestamp};
use storage_types::{TimestampMillis, storage_serde};

use super::stale_subscription_sweep::*;
use crate::{
    PendingSubscription, PendingSubscriptionCursor, PendingSubscriptionPage, SubscriptionArn,
};

const HOUR: Duration = Duration::from_secs(3_600);

fn row(index: u8) -> PendingSubscription {
    PendingSubscription {
        cursor: PendingSubscriptionCursor(vec![index]),
        subscription_arn: SubscriptionArn::new(format!(
            "arn:aws:sns:us-east-1:000000000000:t:00000000-0000-7000-8000-0000000000{index:02}"
        ))
        .unwrap(),
        created_at: TimestampMillis::from_timestamp(1_700_000_000_000),
    }
}

fn page(rows: u8, more: bool) -> PendingSubscriptionPage {
    PendingSubscriptionPage {
        rows: (0..rows).map(row).collect(),
        more,
    }
}

#[test]
fn given_created_at_when_47h59m59s999_or_48h_pass_then_stale_only_at_48h() {
    let created = TimestampMillis::from_timestamp(1_700_000_000_000);
    let lifetime_millis = 48 * 3_600 * 1_000;
    let just_before =
        TimestampMillis::from_timestamp(created.timestamp_millis() + lifetime_millis - 1);
    let exactly = TimestampMillis::from_timestamp(created.timestamp_millis() + lifetime_millis);

    assert!(!is_stale(created, just_before));
    assert!(is_stale(created, exactly));
}

#[test]
fn given_page_bounds_when_validated_then_more_without_rows_and_overlong_pages_fail() {
    let limit = NonZeroU32::new(2).unwrap();

    assert!(validate_pending_page(&page(0, true), limit).is_err());
    assert!(validate_pending_page(&page(3, false), limit).is_err());
    assert!(validate_pending_page(&page(2, true), limit).is_ok());
    assert!(validate_pending_page(&page(0, false), limit).is_ok());
}

#[test]
fn given_next_cursor_or_none_when_outcome_built_then_now_or_interval_and_checkpoint_follow() {
    let cursor = PendingSubscriptionCursor(vec![1, 2]);

    let JobOutcome::Continue {
        not_before,
        checkpoint,
    } = sweep_outcome(HOUR, Some(cursor.clone()), Timestamp::ZERO).unwrap()
    else {
        panic!("a sweep turn continues");
    };
    assert!(not_before == Timestamp::ZERO);
    let decoded: PendingSubscriptionCursor =
        storage_serde::from_bytes(checkpoint.as_ref()).unwrap();
    assert!(decoded == cursor);

    let JobOutcome::Continue {
        not_before,
        checkpoint,
    } = sweep_outcome(HOUR, None, Timestamp::ZERO).unwrap()
    else {
        panic!("a sweep turn continues");
    };
    assert!(not_before == Timestamp::ZERO.checked_add(HOUR).unwrap());
    assert!(checkpoint.is_empty());
}
