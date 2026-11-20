use std::collections::BTreeSet;

use crate::queue_wake_audit::{CallResult, SlotValues, changed_slots, judge};

fn stamp(n: u8) -> Vec<u8> {
    vec![0, 0, 0, 0, 0, 0, 0, n, 0, 0]
}

fn slots(entries: &[(u16, Vec<u8>)]) -> SlotValues {
    entries.iter().cloned().collect()
}

fn set(values: &[u16]) -> BTreeSet<u16> {
    values.iter().copied().collect()
}

#[test]
fn given_new_and_grown_slots_when_diffed_then_both_changed() {
    let before = slots(&[(1, stamp(1))]);
    let after = slots(&[(1, stamp(2)), (7, stamp(3))]);
    assert_eq!(changed_slots(&before, &after), Ok(set(&[1, 7])));
}

#[test]
fn given_cleared_slot_when_diffed_then_w3() {
    let error = changed_slots(&slots(&[(1, stamp(1))]), &slots(&[])).unwrap_err();
    assert!(error.starts_with("W3"));
}

#[test]
fn given_smaller_value_when_diffed_then_w3() {
    let error = changed_slots(&slots(&[(1, stamp(2))]), &slots(&[(1, stamp(1))])).unwrap_err();
    assert!(error.starts_with("W3"));
}

#[test]
fn given_new_slot_with_short_value_when_diffed_then_w3() {
    let error = changed_slots(&slots(&[]), &slots(&[(4, vec![1])])).unwrap_err();
    assert!(error.starts_with("W3"));
}

#[test]
fn given_done_with_exact_slots_when_judged_then_ok() {
    assert!(judge("send", CallResult::Done, &set(&[1]), &set(&[1])).is_ok());
}

#[test]
fn given_done_with_extra_slot_when_judged_then_w2() {
    let error = judge("send", CallResult::Done, &set(&[1]), &set(&[1, 2])).unwrap_err();
    assert!(error.starts_with("W2"));
}

#[test]
fn given_done_without_change_when_judged_then_w1() {
    let error = judge("send", CallResult::Done, &set(&[1]), &set(&[])).unwrap_err();
    assert!(error.starts_with("W1"));
}

#[test]
fn given_aborted_with_change_when_judged_then_error() {
    assert!(judge("send", CallResult::Aborted, &set(&[1]), &set(&[1])).is_err());
}

#[test]
fn given_unknown_with_subset_when_judged_then_ok() {
    assert!(judge("send", CallResult::Unknown, &set(&[1, 2]), &set(&[2])).is_ok());
    assert!(judge("send", CallResult::Unknown, &set(&[1, 2]), &set(&[])).is_ok());
}

#[test]
fn given_wake_queue_names_when_chosen_then_four_distinct_slots() {
    let queues = crate::queue_wake::wake_queues();
    assert_eq!(queues.len(), 4);
    let distinct: BTreeSet<u16> = queues.iter().map(|(_, slot)| *slot).collect();
    assert_eq!(distinct.len(), 4);
    for (name, slot) in &queues {
        assert_eq!(*slot, queue::QueueWakeSlot::of_queue(name).index());
    }
}
