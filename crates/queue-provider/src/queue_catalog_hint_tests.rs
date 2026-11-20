//! Node-local queue catalog hints (W7-D): lifetime, removal and the entry
//! bound.

use storage_types::TimestampMillis;

use crate::{Queue, QueueCatalogHints, QueueCatalogRecord, QueueId, QueueIncarnation};

fn record(name: &str) -> QueueCatalogRecord {
    QueueCatalogRecord {
        queue: Queue {
            queue_name: name.to_owned(),
            ..Default::default()
        },
        id: QueueId::new(),
        incarnation: QueueIncarnation::new(),
    }
}

fn at(millis: i64) -> TimestampMillis {
    TimestampMillis::from_timestamp(millis)
}

#[test]
fn given_row_observed_when_read_within_five_seconds_then_it_is_returned() {
    let hints = QueueCatalogHints::default();
    let observed = record("q");
    hints.observe("q", Some(&observed), at(1_000));
    let hinted = hints.get("q", at(5_999)).expect("hint within 5 s");
    assert_eq!(hinted.queue.queue_name, "q");
    assert!(hinted.incarnation == observed.incarnation);
}

#[test]
fn given_row_observed_when_read_after_five_seconds_then_none() {
    let hints = QueueCatalogHints::default();
    hints.observe("q", Some(&record("q")), at(1_000));
    assert!(hints.get("q", at(6_000)).is_none());
}

#[test]
fn given_absent_row_observed_when_read_then_hint_is_gone() {
    let hints = QueueCatalogHints::default();
    hints.observe("q", Some(&record("q")), at(1_000));
    hints.observe("q", None, at(1_001));
    assert!(hints.get("q", at(1_002)).is_none());

    hints.observe("q", Some(&record("q")), at(1_003));
    assert!(hints.get("q", at(1_004)).is_some());
    hints.forget("q");
    assert!(hints.get("q", at(1_005)).is_none());
}

#[test]
fn given_full_map_when_new_name_observed_then_old_hints_are_cleared() {
    let hints = QueueCatalogHints::default();
    for index in 0..4_096 {
        let name = format!("q{index}");
        hints.observe(&name, Some(&record(&name)), at(1_000));
    }
    assert!(
        hints.get("q0", at(1_001)).is_some(),
        "the map is full, not over"
    );
    hints.observe("new", Some(&record("new")), at(1_001));
    assert!(hints.get("q0", at(1_002)).is_none());
    assert!(hints.get("q4095", at(1_002)).is_none());
    assert!(hints.get("new", at(1_002)).is_some());
}
