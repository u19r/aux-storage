use storage_types::{ExclusiveStartKey, ReadSequenceConsistency};

use crate::manager::storage_manager_impl_read_sequence_token::{
    READ_SEQUENCE_TOKEN_VERSION, ReadSequenceQueryContinuation, ReadSequenceToken,
    decode_read_sequence_token, encode_read_sequence_token,
};

fn token() -> ReadSequenceToken {
    ReadSequenceToken {
        version: READ_SEQUENCE_TOKEN_VERSION,
        request_digest: "request".to_string(),
        metadata_digest: "metadata".to_string(),
        consistency: ReadSequenceConsistency::Eventual,
        next_node_ordinal: 0,
        invocation_ordinal: None,
        query_cursor: None,
        query_continuations: Some(vec![
            ReadSequenceQueryContinuation {
                node_ordinal: 4,
                invocation_ordinal: 1,
                query_cursor: ExclusiveStartKey::Token("cursor-4".to_string()),
            },
            ReadSequenceQueryContinuation {
                node_ordinal: 2,
                invocation_ordinal: 0,
                query_cursor: ExclusiveStartKey::Token("cursor-2".to_string()),
            },
        ]),
        provider_continuation: None,
        completed_nodes: vec![3, 1],
        issued_at_epoch_seconds: 10,
        expires_at_epoch_seconds: i64::MAX,
        integrity: String::new(),
    }
}

#[test]
fn equivalent_frontiers_have_identical_encoded_tokens() {
    let first = encode_read_sequence_token(&token()).expect("encode first");
    let mut reordered = token();
    reordered.completed_nodes.reverse();
    reordered
        .query_continuations
        .as_mut()
        .expect("continuations")
        .reverse();
    let second = encode_read_sequence_token(&reordered).expect("encode second");
    assert_eq!(first, second);
    let decoded = decode_read_sequence_token(&first).expect("decode");
    assert_eq!(decoded.completed_nodes, vec![1, 3]);
    assert_eq!(
        decoded
            .query_continuations
            .expect("continuations")
            .into_iter()
            .map(|continuation| continuation.node_ordinal)
            .collect::<Vec<_>>(),
        vec![2, 4]
    );
}

#[test]
fn malformed_hex_and_integrity_are_rejected() {
    assert!(decode_read_sequence_token("0").is_err());
    let mut encoded = encode_read_sequence_token(&token()).expect("encode");
    encoded.replace_range(0..2, "ff");
    assert!(decode_read_sequence_token(&encoded).is_err());
}

#[test]
fn generated_frontier_permutations_have_one_canonical_encoding() {
    const CASES: usize = 4_096;
    for case in 0..CASES {
        let width = 2 + case % 7;
        let mut canonical = token();
        canonical.completed_nodes = (1..width).collect();
        canonical.query_continuations = Some(
            (1..width)
                .map(|node_ordinal| ReadSequenceQueryContinuation {
                    node_ordinal,
                    invocation_ordinal: (case as u32).wrapping_add(node_ordinal as u32),
                    query_cursor: ExclusiveStartKey::Token(format!("cursor-{case}-{node_ordinal}")),
                })
                .collect(),
        );
        let expected = encode_read_sequence_token(&canonical).expect("canonical frontier");

        let mut permuted = canonical.clone();
        let completed_rotation = (case.wrapping_mul(13)) % permuted.completed_nodes.len();
        permuted.completed_nodes.rotate_left(completed_rotation);
        let continuation_rotation =
            (case.wrapping_mul(17)) % permuted.query_continuations.as_ref().unwrap().len();
        permuted
            .query_continuations
            .as_mut()
            .expect("continuations")
            .rotate_left(continuation_rotation);
        if case & 1 == 0 {
            permuted.completed_nodes.reverse();
            permuted
                .query_continuations
                .as_mut()
                .expect("continuations")
                .reverse();
        }

        let encoded = encode_read_sequence_token(&permuted).expect("permuted frontier");
        assert_eq!(encoded, expected, "frontier case {case}");
        let decoded = decode_read_sequence_token(&encoded).expect("decode frontier");
        assert_eq!(decoded.completed_nodes, canonical.completed_nodes);
        assert_eq!(
            decoded
                .query_continuations
                .expect("decoded continuations")
                .iter()
                .map(|continuation| continuation.node_ordinal)
                .collect::<Vec<_>>(),
            canonical
                .query_continuations
                .as_ref()
                .expect("canonical continuations")
                .iter()
                .map(|continuation| continuation.node_ordinal)
                .collect::<Vec<_>>()
        );
    }
}
