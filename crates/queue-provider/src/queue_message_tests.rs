//! The claim selection rule of W2 (decisions D3, D14 to D16).

use std::num::NonZeroU32;

use storage_types::TimestampMillis;

use crate::{QueueDueMark, QueueDueRequest, QueueIncarnation};

fn request(limit: u32, first_attempt: bool, rotation: u32) -> QueueDueRequest {
    QueueDueRequest {
        incarnation: QueueIncarnation::new(),
        captured_at: TimestampMillis::from_timestamp(10_000),
        clock_skew_millis: 500,
        limit: NonZeroU32::new(limit).unwrap(),
        window: NonZeroU32::new(8).unwrap(),
        head: NonZeroU32::new(8).unwrap(),
        first_attempt,
        rotation,
        dead_letter_target: None,
    }
}

fn due(at: i64) -> QueueDueMark {
    QueueDueMark {
        due_at: TimestampMillis::from_timestamp(at),
        leased: false,
    }
}

fn leased(at: i64) -> QueueDueMark {
    QueueDueMark {
        due_at: TimestampMillis::from_timestamp(at),
        leased: true,
    }
}

/// D14: a row with a receipt is due only after the clock-skew margin; D15
/// and D16: a selection that leaves a due entry is not drained, and a drained
/// one reports the earliest later entry with its margin.
#[test]
fn given_leased_and_delayed_entries_when_selected_then_guard_drained_and_next_due_follow_the_rule()
{
    let rule = request(2, true, 0);
    let window = [due(1_000), leased(9_800), leased(9_000), due(9_900)];
    let selection = rule.select(&window, false, &[due(12_000)]);
    assert_eq!(
        selection.positions,
        vec![0, 2],
        "the guarded lease is skipped"
    );
    assert!(!selection.drained, "one due entry was left");

    let window = [leased(9_800), due(9_900)];
    let selection = rule.select(&window, false, &[due(12_000)]);
    assert_eq!(selection.positions, vec![1]);
    assert!(selection.drained);
    assert_eq!(
        selection.next_due,
        Some(TimestampMillis::from_timestamp(10_300)),
        "the guarded lease is due at its end plus the margin"
    );

    let selection = rule.select(&[], false, &[leased(11_000), due(11_200)]);
    assert!(selection.drained && selection.positions.is_empty());
    assert_eq!(
        selection.next_due,
        Some(TimestampMillis::from_timestamp(11_200))
    );
    let full = rule.select(&[due(1), due(2)], true, &[]);
    assert!(!full.drained, "a full window is never drained");
}

#[test]
fn given_queue_request_constants_when_read_then_each_has_its_mode_class_durability_retry_and_work()
{
    use storage_types::{
        AttemptDurability::{Durable, Relaxed},
        AttemptMode, DeadlineClass, NativeWork,
    };

    use crate::constants::{
        CATALOG_POINT_READ, CATALOG_RANGE_READ, CATALOG_WRITE, CLEANUP_PAGE, MESSAGE_CLAIM,
        MESSAGE_PUBLISH, RECEIVE_PROBE, message_request,
    };

    let table = [
        (
            "CATALOG_WRITE",
            CATALOG_WRITE,
            AttemptMode::ReadWrite,
            DeadlineClass::SchemaChange,
            Durable,
            NativeWork::Write,
        ),
        (
            "CATALOG_POINT_READ",
            CATALOG_POINT_READ,
            AttemptMode::ReadOnly,
            DeadlineClass::PointRead,
            Durable,
            NativeWork::PointRead,
        ),
        (
            "CATALOG_RANGE_READ",
            CATALOG_RANGE_READ,
            AttemptMode::ReadOnly,
            DeadlineClass::RangeRead,
            Durable,
            NativeWork::RangeRead,
        ),
        (
            "CLEANUP_PAGE",
            CLEANUP_PAGE,
            AttemptMode::ReadWrite,
            DeadlineClass::BackgroundPage,
            Durable,
            NativeWork::Write,
        ),
        (
            "RECEIVE_PROBE",
            RECEIVE_PROBE,
            AttemptMode::ReadOnly,
            DeadlineClass::LongPollProbe,
            Relaxed,
            NativeWork::PointRead,
        ),
        (
            "MESSAGE_CLAIM",
            MESSAGE_CLAIM,
            AttemptMode::ReadWriteItems,
            DeadlineClass::Write,
            Relaxed,
            NativeWork::Write,
        ),
        (
            "MESSAGE_PUBLISH",
            MESSAGE_PUBLISH,
            AttemptMode::ReadWriteItems,
            DeadlineClass::Write,
            Durable,
            NativeWork::Write,
        ),
        (
            "message_request(1, Durable)",
            message_request(1, Durable),
            AttemptMode::ReadWriteItems,
            DeadlineClass::Write,
            Durable,
            NativeWork::Write,
        ),
        (
            "message_request(2, Relaxed)",
            message_request(2, Relaxed),
            AttemptMode::ReadWriteItems,
            DeadlineClass::Transaction,
            Relaxed,
            NativeWork::Write,
        ),
    ];
    for (name, request, mode, class, durability, work) in table {
        assert_eq!(request.mode, mode, "{name} mode");
        assert_eq!(request.class, class, "{name} class");
        assert_eq!(request.durability, durability, "{name} durability");
        assert_eq!(request.work, work, "{name} work");
        assert_eq!(request.retry.max_attempts(), 5, "{name} max attempts");
    }
}
