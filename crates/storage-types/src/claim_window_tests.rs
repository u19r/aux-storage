//! W2 test 6, moved with the one owner of the slot rule.

use std::num::NonZeroU32;

use crate::ClaimSlot;

fn slot(limit: u32, first_attempt: bool, rotation: u32) -> ClaimSlot {
    ClaimSlot {
        limit: NonZeroU32::new(limit).unwrap(),
        first_attempt,
        rotation,
    }
}

/// The first attempt with fewer than two full slots takes slot 0; two or more
/// full slots, a full window, or a retry take the slot of `rotation`;
/// positions are ascending, aligned to `limit`, and in bounds.
#[test]
fn given_observed_entries_when_positions_are_chosen_then_slots_are_aligned_and_in_bounds() {
    assert_eq!(slot(3, true, 7).positions(5, false), vec![0, 1, 2]);
    assert_eq!(slot(3, true, 1).positions(7, false), vec![3, 4, 5]);
    assert_eq!(slot(1, true, 1).positions(1, false), vec![0]);
    assert_eq!(slot(1, true, 3).positions(2, false), vec![1]);
    assert_eq!(slot(3, true, 1).positions(8, true), vec![3, 4, 5]);
    assert_eq!(slot(3, false, 1).positions(7, false), vec![3, 4, 5]);
    assert!(slot(3, true, 0).positions(0, false).is_empty());
    assert_eq!(slot(3, false, 9).positions(2, false), vec![0, 1]);
    for rotation in 0..32 {
        for observed in 1..20 {
            let positions = slot(3, false, rotation).positions(observed, false);
            assert!(!positions.is_empty() && positions.len() <= 3);
            assert_eq!(positions[0] % 3, 0, "slot is aligned");
            assert!(positions.windows(2).all(|pair| pair[1] == pair[0] + 1));
            assert!(*positions.last().unwrap() < observed);
        }
    }
}
