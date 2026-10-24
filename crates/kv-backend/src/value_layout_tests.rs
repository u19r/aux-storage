use crate::{
    KvMutation, constants::FOUNDATIONDB_NATIVE_VALUE_BYTES, mutation_preparation::expand_mutations,
};

#[test]
fn given_fixed_scalar_when_stored_then_no_chunk_span_is_cleared() {
    let changes = expand_mutations(
        vec![KvMutation::PutUnchunked {
            key: b"fixed".to_vec(),
            value: 17_u64.to_be_bytes().to_vec(),
        }],
        FOUNDATIONDB_NATIVE_VALUE_BYTES,
    )
    .unwrap();
    assert_eq!(changes.len(), 1);
    let KvMutation::Put { key, value } = &changes[0] else {
        panic!("expected scalar head")
    };
    assert_eq!(key, b"fixed");
    assert_eq!(value, &[0, 0, 0, 0, 0, 0, 0, 17, 0]);
}

#[test]
fn given_insufficient_native_capacity_when_storing_fixed_scalar_then_storage_is_rejected() {
    let changes = expand_mutations(
        vec![KvMutation::PutUnchunked {
            key: b"fixed".to_vec(),
            value: 17_u64.to_be_bytes().to_vec(),
        }],
        8,
    );
    assert!(changes.is_err());
}

#[test]
fn given_unchunked_format_when_deleted_then_no_chunk_span_is_cleared() {
    let changes = expand_mutations(
        vec![KvMutation::DeleteUnchunked {
            key: b"fixed".to_vec(),
        }],
        FOUNDATIONDB_NATIVE_VALUE_BYTES,
    )
    .unwrap();
    assert_eq!(changes.len(), 1, "fixed-size formats have no chunk span");
    assert!(matches!(&changes[0], KvMutation::Delete { key } if key == b"fixed"));
}

#[test]
fn given_variable_format_when_deleted_then_its_previous_chunks_are_cleared() {
    let changes = expand_mutations(
        vec![KvMutation::Delete {
            key: b"variable".to_vec(),
        }],
        FOUNDATIONDB_NATIVE_VALUE_BYTES,
    )
    .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(matches!(changes[0], KvMutation::DeleteRange { .. }));
    assert!(matches!(&changes[1], KvMutation::Delete { key } if key == b"variable"));
}
