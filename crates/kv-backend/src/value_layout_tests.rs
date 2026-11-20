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
        &mut [],
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
        &mut [],
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
        &mut [],
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
        &mut [],
    )
    .unwrap();
    assert_eq!(changes.len(), 2);
    assert!(matches!(changes[0], KvMutation::DeleteRange { .. }));
    assert!(matches!(&changes[1], KvMutation::Delete { key } if key == b"variable"));
}

#[test]
fn given_observed_primary_layout_when_mutated_then_only_existing_chunks_are_cleared() {
    for previous in [None, Some(8), Some(150_000)] {
        for next in [None, Some(8), Some(150_000)] {
            let key = b"primary".to_vec();
            let mut physical = std::collections::BTreeMap::new();
            if let Some(length) = previous {
                let changes = expand_mutations(
                    vec![KvMutation::Put {
                        key: key.clone(),
                        value: vec![1; length],
                    }],
                    FOUNDATIONDB_NATIVE_VALUE_BYTES,
                    &mut [],
                )
                .unwrap();
                store_physical_mutations(&mut physical, changes);
            }
            let has_chunks =
                previous.is_some_and(|length| length >= FOUNDATIONDB_NATIVE_VALUE_BYTES);
            let mut layouts = vec![(key.clone(), has_chunks)];
            let mutation = match next {
                None => KvMutation::Delete { key: key.clone() },
                Some(length) => KvMutation::Put {
                    key: key.clone(),
                    value: vec![2; length],
                },
            };
            let changes = expand_mutations(
                vec![mutation],
                FOUNDATIONDB_NATIVE_VALUE_BYTES,
                &mut layouts,
            )
            .unwrap();
            assert_eq!(
                changes
                    .iter()
                    .filter(|change| matches!(change, KvMutation::DeleteRange { .. }))
                    .count(),
                usize::from(has_chunks)
            );
            store_physical_mutations(&mut physical, changes);
            require_physical_value(
                &physical,
                &key,
                next.map(|length| vec![2; length]).as_deref(),
            );
            assert_eq!(
                layouts[0].1,
                next.is_some_and(|length| length >= FOUNDATIONDB_NATIVE_VALUE_BYTES)
            );
        }
    }
}

#[test]
fn given_repeated_primary_writes_when_flushed_then_no_previous_chunks_remain() {
    for separate_flushes in [false, true] {
        for lengths in [[150_000, 8], [8, 150_000]] {
            let key = b"primary".to_vec();
            let mut physical = std::collections::BTreeMap::new();
            let mut layouts = vec![(key.clone(), false)];
            // The second order starts with a chunked physical value.
            if lengths[0] == 8 {
                let changes = expand_mutations(
                    vec![KvMutation::Put {
                        key: key.clone(),
                        value: vec![0; 240_000],
                    }],
                    FOUNDATIONDB_NATIVE_VALUE_BYTES,
                    &mut layouts,
                )
                .unwrap();
                store_physical_mutations(&mut physical, changes);
            }
            let changes = lengths
                .into_iter()
                .enumerate()
                .map(|(number, length)| KvMutation::Put {
                    key: key.clone(),
                    value: vec![(number + 1) as u8; length],
                })
                .collect::<Vec<_>>();
            if separate_flushes {
                for change in changes {
                    let expanded = expand_mutations(
                        vec![change],
                        FOUNDATIONDB_NATIVE_VALUE_BYTES,
                        &mut layouts,
                    )
                    .unwrap();
                    store_physical_mutations(&mut physical, expanded);
                }
            } else {
                let expanded =
                    expand_mutations(changes, FOUNDATIONDB_NATIVE_VALUE_BYTES, &mut layouts)
                        .unwrap();
                store_physical_mutations(&mut physical, expanded);
            }
            require_physical_value(&physical, &key, Some(&vec![2; lengths[1]]));
        }
    }
}

fn store_physical_mutations(
    physical: &mut std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    changes: Vec<KvMutation>,
) {
    for change in changes {
        match change {
            KvMutation::Put { key, value } => {
                physical.insert(key, value);
            }
            KvMutation::Delete { key } => {
                physical.remove(&key);
            }
            KvMutation::DeleteRange { begin, end }
            | KvMutation::ClearUnchunkedRange { begin, end } => {
                physical.retain(|key, _| key < &begin || key >= &end);
            }
            KvMutation::PutUnchunked { .. } | KvMutation::DeleteUnchunked { .. } => {
                panic!("unexpanded mutation")
            }
        }
    }
}

fn require_physical_value(
    physical: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    key: &[u8],
    expected: Option<&[u8]>,
) {
    use crate::value_layout::{ChunkAddress, ValueHeader, parse_header};
    let Some(expected) = expected else {
        assert!(physical.is_empty());
        return;
    };
    let head = physical.get(key).unwrap();
    match parse_header(head).unwrap() {
        ValueHeader::Inline(value) => {
            assert_eq!(value, expected);
            assert_eq!(physical.len(), 1, "old chunks survived inline replacement");
        }
        ValueHeader::Chunked { length, count } => {
            assert_eq!(length, expected.len());
            assert_eq!(
                physical.len(),
                count + 1,
                "old chunks survived chunked replacement"
            );
            let address = ChunkAddress::prepare(key).unwrap();
            let mut hydrated = Vec::new();
            for number in 0..count {
                hydrated
                    .extend_from_slice(physical.get(&address.chunk_key(number).unwrap()).unwrap());
            }
            assert_eq!(hydrated, expected);
        }
    }
}

#[test]
fn given_inline_primary_when_replaced_then_protected_layout_removes_only_chunk_clear_cost() {
    let key = b"primary".to_vec();
    let make_changes = || {
        vec![KvMutation::Put {
            key: key.clone(),
            value: vec![7; 32],
        }]
    };
    let generic =
        expand_mutations(make_changes(), FOUNDATIONDB_NATIVE_VALUE_BYTES, &mut []).unwrap();
    let protected = expand_mutations(
        make_changes(),
        FOUNDATIONDB_NATIVE_VALUE_BYTES,
        &mut [(key.clone(), false)],
    )
    .unwrap();
    let bytes = |changes: &[KvMutation]| {
        changes
            .iter()
            .map(|change| match change {
                KvMutation::Put { key, value } => key.len() + value.len(),
                KvMutation::Delete { key } => key.len(),
                KvMutation::DeleteRange { begin, end } => begin.len() + end.len(),
                _ => panic!("unexpanded mutation"),
            })
            .sum::<usize>()
    };
    let KvMutation::DeleteRange { begin, end } = &generic[0] else {
        panic!("generic clear missing")
    };
    assert_eq!(generic.len(), protected.len() + 1);
    assert_eq!(bytes(&generic) - bytes(&protected), begin.len() + end.len());
    let mut generic_state = std::collections::BTreeMap::new();
    let mut protected_state = std::collections::BTreeMap::new();
    store_physical_mutations(&mut generic_state, generic);
    store_physical_mutations(&mut protected_state, protected);
    assert_eq!(generic_state, protected_state);
}
