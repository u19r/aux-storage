//! Pure judgement of slot-key changes around one queue call (N1).

use std::collections::{BTreeMap, BTreeSet};

pub(crate) type SlotValues = BTreeMap<u16, Vec<u8>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CallResult {
    Done,
    Aborted,
    Unknown,
}

/// The changed slots; an error if a slot was cleared or went backwards (W3).
pub(crate) fn changed_slots(
    before: &SlotValues,
    after: &SlotValues,
) -> Result<BTreeSet<u16>, String> {
    let mut changed = BTreeSet::new();
    for (slot, old) in before {
        match after.get(slot) {
            None => return Err(format!("W3 slot {slot} was cleared")),
            Some(new) if new < old => return Err(format!("W3 slot {slot} went backwards")),
            Some(new) if new != old => {
                changed.insert(*slot);
            }
            Some(_) => {}
        }
    }
    for (slot, new) in after {
        if !before.contains_key(slot) {
            if new.len() != 10 {
                return Err(format!(
                    "W3 slot {slot} value is not a versionstamp ({} bytes)",
                    new.len()
                ));
            }
            changed.insert(*slot);
        }
    }
    Ok(changed)
}

/// W1 and W2 for one call.
pub(crate) fn judge(
    operation: &str,
    result: CallResult,
    expected: &BTreeSet<u16>,
    changed: &BTreeSet<u16>,
) -> Result<(), String> {
    let fits = match result {
        CallResult::Done => changed == expected,
        CallResult::Aborted => changed.is_empty(),
        CallResult::Unknown => changed.is_subset(expected),
    };
    if fits {
        Ok(())
    } else {
        Err(format!(
            "{} {operation} {result:?}: expected {expected:?}, changed {changed:?}",
            if changed.is_subset(expected) {
                "W1"
            } else {
                "W2"
            }
        ))
    }
}
