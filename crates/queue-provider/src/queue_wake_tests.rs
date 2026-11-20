//! Wake slots and boards (N1).

use std::collections::HashSet;

use futures::FutureExt;

use crate::{QUEUE_WAKE_SLOTS, QueueWakeBoard, QueueWakeSlot, QueueWakeSlots, QueueWakeTasks};

fn slot(index: u16) -> QueueWakeSlot {
    QueueWakeSlot::from_index(index).unwrap()
}

#[test]
fn given_url_forms_of_one_queue_when_slotted_then_one_slot() {
    let forms = [
        QueueWakeSlot::of_queue("https://h/q/123/orders"),
        QueueWakeSlot::of_queue("orders"),
        QueueWakeSlot::of_queue("http://x/orders/"),
    ];
    assert_eq!(forms[0], forms[1]);
    assert_eq!(forms[1], forms[2]);
    assert!(forms.iter().all(|slot| slot.index() < QUEUE_WAKE_SLOTS));
}

#[test]
fn given_10000_names_when_slotted_then_at_least_900_slots_used() {
    let used: HashSet<QueueWakeSlot> = (0..10_000)
        .map(|n| QueueWakeSlot::of_queue(&format!("q-{n}")))
        .collect();
    assert!(used.len() >= 900, "only {} slots used", used.len());
}

#[test]
fn given_slots_staged_twice_when_inserted_then_kept_once() {
    let mut staged = QueueWakeSlots::default();
    assert!(staged.insert(slot(5)));
    assert!(!staged.insert(slot(5)));
    assert_eq!(staged.take(), vec![slot(5)]);
    assert!(staged.take().is_empty());
}

#[test]
fn given_local_board_when_committed_then_each_slot_advances_once() {
    let board = QueueWakeBoard::local();
    let (a, b) = (slot(1), slot(2));
    assert!(board.mark(a).covered);
    assert_eq!(board.mark(a).generation, 0);
    board.committed(&[a, a, b]);
    assert_eq!(board.mark(a).generation, 2);
    assert_eq!(board.mark(b).generation, 1);
}

#[test]
fn given_feed_board_without_forwarding_when_committed_then_nothing_changes() {
    let board = QueueWakeBoard::feed(false);
    let s = slot(3);
    let before = board.mark(s);
    board.committed(&[s]);
    assert_eq!(board.mark(s), before);
    assert!(board.take_committed().is_empty());
}

#[test]
fn given_forwarding_board_when_committed_then_taken_once_in_slot_order() {
    let board = QueueWakeBoard::feed(true);
    let (s2, s9) = (slot(2), slot(9));
    board.committed(&[s9, s2, s9]);
    assert_eq!(board.take_committed(), vec![s2, s9]);
    assert!(board.take_committed().is_empty());
    assert!(board.commits_pending().now_or_never().is_some());
    assert!(board.commits_pending().now_or_never().is_none());
}

#[test]
fn given_uncovered_slot_when_covered_twice_then_generation_advances_once() {
    let board = QueueWakeBoard::feed(false);
    let s = slot(4);
    board.cover(s);
    board.cover(s);
    let mark = board.mark(s);
    assert_eq!(mark.generation, 1);
    assert!(mark.covered);
    board.uncover(s);
    let mark = board.mark(s);
    assert_eq!(mark.generation, 2);
    assert!(!mark.covered);
}

#[test]
fn given_interest_when_registered_and_dropped_then_listed_only_while_held() {
    let board = QueueWakeBoard::feed(false);
    let s = slot(6);
    assert!(!board.has_interest(s));
    let first = board.register(s);
    assert!(board.has_interest(s));
    assert_eq!(board.interested_slots(), vec![s]);
    assert!(board.interest_added().now_or_never().is_some());
    let second = board.register(s);
    assert!(board.interest_added().now_or_never().is_none());
    drop(first);
    assert!(board.has_interest(s));
    drop(second);
    assert!(!board.has_interest(s));
    assert!(board.interested_slots().is_empty());
}

#[tokio::test]
async fn given_board_when_closed_then_closed_resolves_and_slots_are_uncovered() {
    let board = QueueWakeBoard::local();
    assert!(!board.is_closed());
    board.close();
    board.closed().await;
    assert!(board.is_closed());
    assert!(QueueWakeSlot::all().all(|s| !board.mark(s).covered));
}

#[tokio::test]
async fn given_tasks_guard_when_dropped_then_board_closed_and_task_aborted() {
    let board = QueueWakeBoard::local();
    let handle = tokio::spawn(std::future::pending::<()>());
    let guard = QueueWakeTasks::new(board.clone(), vec![handle.abort_handle()]);
    drop(guard);
    assert!(board.is_closed());
    let error = handle.await.unwrap_err();
    assert!(error.is_cancelled());
}
