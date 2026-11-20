//! The feed's pure decision of what a read of the wake slots means. No
//! cluster is needed.

use futures::future::AbortHandle;
use queue_provider::{QueueWakeBoard, QueueWakeSlot};

use super::queue_wake::Feed;

fn slot(index: u16) -> QueueWakeSlot {
    QueueWakeSlot::from_index(index).unwrap()
}

#[test]
fn given_unread_slot_with_interest_when_applied_then_covered_once() {
    let board = QueueWakeBoard::feed(false);
    let mut feed = Feed::for_test(board.clone());
    let s = slot(7);
    let _interest = board.register(s);
    feed.apply(&[s], vec![None]);
    let mark = board.mark(s);
    assert!(mark.covered);
    assert_eq!(mark.generation, 1);
}

#[test]
fn given_covered_slot_when_same_value_applied_then_no_advance() {
    let board = QueueWakeBoard::feed(false);
    let mut feed = Feed::for_test(board.clone());
    let s = slot(7);
    let _interest = board.register(s);
    feed.apply(&[s], vec![Some(b"v".to_vec())]);
    let covered = board.mark(s);
    assert!(covered.covered);
    feed.apply(&[s], vec![Some(b"v".to_vec())]);
    assert_eq!(board.mark(s), covered);
}

#[test]
fn given_covered_slot_when_changed_value_applied_then_one_advance() {
    let board = QueueWakeBoard::feed(false);
    let mut feed = Feed::for_test(board.clone());
    let s = slot(7);
    let _interest = board.register(s);
    feed.apply(&[s], vec![Some(b"v".to_vec())]);
    let covered = board.mark(s);
    feed.apply(&[s], vec![Some(b"v2".to_vec())]);
    let mark = board.mark(s);
    assert!(mark.covered);
    assert_eq!(mark.generation, covered.generation + 1);
}

#[test]
fn given_unwatched_slot_without_interest_when_fallback_reads_it_then_it_stays_uncovered() {
    let board = QueueWakeBoard::feed(false);
    let mut feed = Feed::for_test(board.clone());
    let s = slot(7);
    feed.apply(&[s], vec![Some(b"v".to_vec())]);
    let mark = board.mark(s);
    assert!(!mark.covered);
    assert_eq!(mark.generation, 0);
}

#[test]
fn given_live_watch_without_interest_when_applied_then_covered() {
    let board = QueueWakeBoard::feed(false);
    let mut feed = Feed::for_test(board.clone());
    let s = slot(7);
    feed.watched[usize::from(s.index())] = Some(AbortHandle::new_pair().0);
    feed.apply(&[s], vec![None]);
    assert!(board.mark(s).covered);
}
