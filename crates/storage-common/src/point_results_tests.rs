use std::cell::Cell;

use crate::point_results::place_value;

#[test]
fn given_unrequested_result_when_placed_then_reject_it_without_filling_slots() {
    let keys = ["first", "second"];
    let mut slots = [None, None];
    assert!(place_value(&keys, &mut slots, |requested, _| *requested == "other", 1).is_err());
    assert_eq!(slots, [None, None]);
}

#[test]
fn given_duplicate_native_results_when_placed_then_reject_the_second_result() {
    let keys = ["first", "first"];
    let mut slots = [None, None];
    place_value(&keys, &mut slots, |requested, _| *requested == "first", 1)
        .expect("first physical result");
    assert!(place_value(&keys, &mut slots, |requested, _| *requested == "first", 2).is_err());
    assert_eq!(slots, [Some(1), Some(1)]);
}

#[test]
fn given_invalid_slot_count_when_placed_then_reject_it() {
    let keys = ["first", "second"];
    let mut slots = [None];
    assert!(place_value(&keys, &mut slots, |requested, _| *requested == "first", 1).is_err());
    assert_eq!(slots, [None]);
}

#[test]
fn given_unique_request_when_placed_then_move_the_payload_without_cloning() {
    let clones = Cell::new(0);
    let bytes = vec![1; 1024];
    let pointer = bytes.as_ptr();
    let mut slots = [None];
    place_value(
        &[1],
        &mut slots,
        |requested, _| *requested == 1,
        ObservedValue {
            bytes,
            clones: &clones,
        },
    )
    .expect("unique requested result");
    let value = slots[0].as_ref().expect("present result");
    assert_eq!(value.bytes.as_ptr(), pointer);
    assert_eq!(clones.get(), 0);
}

#[test]
fn given_repeated_requests_when_placed_then_clone_only_the_additional_slots() {
    let clones = Cell::new(0);
    let bytes = vec![1; 1024];
    let pointer = bytes.as_ptr();
    let mut slots = [None, None, None];
    let count = place_value(
        &[1, 1, 1],
        &mut slots,
        |requested, _| *requested == 1,
        ObservedValue {
            bytes,
            clones: &clones,
        },
    )
    .expect("repeated requested result");
    assert_eq!(count, 3);
    assert_eq!(clones.get(), 2);
    assert_eq!(
        slots[0].as_ref().expect("first result").bytes.as_ptr(),
        pointer
    );
    assert!(slots.iter().all(|slot| {
        slot.as_ref()
            .is_some_and(|value| value.bytes.as_slice() == [1; 1024])
    }));
}

struct ObservedValue<'a> {
    bytes: Vec<u8>,
    clones: &'a Cell<usize>,
}

impl Clone for ObservedValue<'_> {
    fn clone(&self) -> Self {
        self.clones.set(self.clones.get() + 1);
        Self {
            bytes: self.bytes.clone(),
            clones: self.clones,
        }
    }
}
