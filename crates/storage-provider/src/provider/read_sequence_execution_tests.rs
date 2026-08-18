use crate::provider::read_sequence_execution::{
    ReadSequenceMappedEntry, ReadSequenceMappedRangePage,
};

fn entry(parent: &[u8], begin: &[u8], end: &[u8]) -> ReadSequenceMappedEntry {
    ReadSequenceMappedEntry {
        parent_key: parent.to_vec(),
        parent_value: Vec::new(),
        begin: begin.to_vec(),
        end: end.to_vec(),
        key_values: Vec::new(),
    }
}

#[test]
fn mapped_page_requires_complete_stable_ordered_envelope() {
    let incomplete = ReadSequenceMappedRangePage {
        entries: Vec::new(),
        more: true,
    };
    assert!(incomplete.validate_complete(false).is_err());

    let valid = ReadSequenceMappedRangePage {
        entries: vec![entry(b"p1", b"a", b"b"), entry(b"p2", b"a", b"b")],
        more: false,
    };
    assert!(valid.validate_complete(false).is_ok());
}

#[test]
fn mapped_page_rejects_invalid_bounds_and_duplicate_order() {
    assert!(
        ReadSequenceMappedRangePage {
            entries: vec![entry(b"p", b"b", b"a")],
            more: false,
        }
        .validate_complete(false)
        .is_err()
    );
    assert!(
        ReadSequenceMappedRangePage {
            entries: vec![entry(b"p", b"a", b"b"), entry(b"p", b"a", b"b")],
            more: false,
        }
        .validate_complete(false)
        .is_err()
    );
}

#[test]
fn mapped_page_accepts_empty_end_for_point_lookup() {
    assert!(
        ReadSequenceMappedRangePage {
            entries: vec![entry(b"p", b"a", &[])],
            more: false,
        }
        .validate_complete(false)
        .is_ok()
    );
}

#[test]
fn mapped_page_validates_reverse_parent_order() {
    let reverse = ReadSequenceMappedRangePage {
        entries: vec![entry(b"p2", b"a", b"b"), entry(b"p1", b"a", b"b")],
        more: false,
    };
    assert!(reverse.validate_complete(true).is_ok());
    assert!(reverse.validate_complete(false).is_err());
}
