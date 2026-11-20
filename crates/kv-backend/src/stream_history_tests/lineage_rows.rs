use std::num::NonZeroU64;

use storage_types::SystemStreamScope;

use crate::{constants::SYSTEM_STREAM_LINEAGE_PREFIX as LINEAGE_PREFIX, stream_history::*};

pub(super) fn sealed_row() -> LineageRow {
    LineageRow {
        scope_id: "scope".to_owned(),
        generation: 1,
        shard_id: "parent".to_owned(),
        lineage_id: "parent-lineage".to_owned(),
        parent_lineage_id: None,
        sealed: true,
        initial_sequence: NonZeroU64::MIN,
        sealed_after_offset: None,
    }
}

#[test]
fn given_native_parent_when_stamped_then_raw_header_covers_every_batch_ordinal() {
    let mut bytes = encode_native_sealed_lineage(&sealed_row()).expect("pending parent");
    assert_eq!(
        &bytes[1..11],
        &crate::constants::KV_VERSIONSTAMP_PLACEHOLDER
    );
    bytes[1..9].copy_from_slice(&41u64.to_be_bytes());
    for batch in [0u16, u16::MAX] {
        bytes[9..11].copy_from_slice(&batch.to_be_bytes());
        assert_eq!(
            decode_lineage_row(&bytes)
                .expect("sealed parent")
                .sealed_after_offset,
            Some(42)
        );
    }
}

#[test]
fn given_native_parent_when_header_or_state_is_invalid_then_decode_rejects_it() {
    let mut bytes = encode_native_sealed_lineage(&sealed_row()).expect("pending parent");
    assert!(decode_lineage_row(&bytes).is_err()); // An incomplete stamp cannot become a final end.
    assert!(decode_lineage_row(&bytes[..10]).is_err());
    bytes[0] = 2;
    assert!(decode_lineage_row(&bytes).is_err());
    let mut row = sealed_row();
    row.sealed = false;
    assert!(encode_native_sealed_lineage(&row).is_err());
    row.sealed = true;
    row.sealed_after_offset = Some(7);
    assert!(encode_native_sealed_lineage(&row).is_err());
    row.sealed_after_offset = None;
    row.initial_sequence = NonZeroU64::new(50).expect("initial");
    let mut bytes = encode_native_sealed_lineage(&row).expect("pending parent");
    bytes[1..9].copy_from_slice(&41u64.to_be_bytes());
    assert!(decode_lineage_row(&bytes).is_err());
}

#[test]
fn given_numeric_parent_when_encoded_then_numeric_end_is_preserved() {
    let mut row = sealed_row();
    row.sealed_after_offset = Some(19);
    let bytes = encode_lineage(&row).expect("numeric parent");
    assert_eq!(bytes[0], ORDINARY_LINEAGE);
    assert_eq!(
        decode_lineage_row(&bytes)
            .expect("numeric parent")
            .sealed_after_offset,
        Some(19)
    );
}

#[test]
fn given_active_lineage_when_snapshots_advance_then_old_high_stays_closed() {
    let scope = SystemStreamScope::try_new("scope", 1).expect("scope");
    let make_history = |version| {
        let mut row = sealed_row();
        row.sealed = false;
        decode_native_history(
            scope.clone(),
            vec![(
                segmented_key(LINEAGE_PREFIX, &row.lineage_id).expect("key"),
                row,
            )],
            version,
        )
        .expect("native history")
    };
    let old = make_history(41).capture_high().expect("old high");
    let new = make_history(45).capture_high().expect("new high");
    assert_eq!(
        old.frontier()[0].offset().as_str(),
        "00000000000000000041-vsffffffff"
    );
    assert_eq!(
        new.frontier()[0].offset().as_str(),
        "00000000000000000045-vsffffffff"
    );
    assert_ne!(old, new);
}

#[test]
fn given_native_lineage_start_above_snapshot_when_decoded_then_history_is_unavailable() {
    for uses_versionstamps in [false, true] {
        let scope = SystemStreamScope::try_new("scope", 1).unwrap();
        let row = LineageRow {
            sealed: false,
            initial_sequence: NonZeroU64::new(44).unwrap(),
            ..sealed_row()
        };
        let key = segmented_key(LINEAGE_PREFIX, &row.lineage_id).unwrap();
        let rows = vec![(key, row)];
        let history = if uses_versionstamps {
            decode_native_history(scope, rows, 42)
        } else {
            decode_sparse_history(scope, rows, 42)
        };
        assert!(history.is_err());
    }
}

#[test]
fn given_sealed_end_below_initial_when_decoding_then_history_is_unavailable() {
    let scope = SystemStreamScope::try_new("scope", 1).unwrap();
    let rows = || {
        let row = LineageRow {
            sealed: true,
            initial_sequence: NonZeroU64::new(5).unwrap(),
            sealed_after_offset: Some(4),
            ..sealed_row()
        };
        let key = segmented_key(LINEAGE_PREFIX, &row.lineage_id).unwrap();
        vec![(key, row)]
    };
    for uses_versionstamps in [true, false] {
        let history = if uses_versionstamps {
            decode_native_history(scope.clone(), rows(), 9)
        } else {
            decode_sparse_history(scope.clone(), rows(), 9)
        };
        let Err(error) = history else {
            panic!("a sealed end below the lineage start must be unavailable");
        };
        assert!(matches!(
            error.as_ref(),
            storage_types::StorageEnum::HistoryUnavailable
        ));
    }
}
