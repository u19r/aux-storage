//! The NOTIFY payload of queue wake slots. Pure, no server.

use queue_provider::QueueWakeSlot;

use super::queue_wake::{parse_slots, payload};

#[test]
fn given_all_slots_when_payload_built_then_it_fits_the_notify_limit_and_round_trips() {
    let slots = QueueWakeSlot::all().collect::<Vec<_>>();
    assert_eq!(slots.len(), 1_024);
    let text = payload(&slots);
    assert!(text.len() <= 8_000, "payload is {} bytes", text.len());
    assert_eq!(parse_slots(&text).collect::<Vec<_>>(), slots);
}

#[test]
fn given_garbage_payload_when_parsed_then_only_valid_slots_remain() {
    let slots = parse_slots("3,x,1024,,7")
        .map(QueueWakeSlot::index)
        .collect::<Vec<_>>();
    assert_eq!(slots, vec![3, 7]);
}
