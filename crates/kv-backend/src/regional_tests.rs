use regional_replication::message::LineageRef;
use storage_provider::{LineageCursor, ProgressDirection};
use storage_types::{PairId, SystemStreamOffset, SystemStreamPosition};

use crate::{
    fault_injection::key_family,
    regional::{
        decode_progress, decode_progress_lineage, encode_progress, progress_key, progress_prefix,
    },
    table_key_layout::{TableSubFamily, table_data_family, table_data_range},
};

const TABLE: u32 = 0x0102_0304;

fn pair() -> PairId {
    PairId::from_bytes([7; 16])
}

fn lineage(shard: &str, lineage: &str) -> LineageRef {
    let position = SystemStreamPosition::try_new(shard, lineage, "0").unwrap();
    LineageRef {
        shard: position.shard_id().clone(),
        lineage: position.lineage_id().clone(),
    }
}

#[test]
fn given_cursor_when_encoded_then_decodes_equal() {
    for cursor in [
        LineageCursor {
            through: None,
            sealed: false,
        },
        LineageCursor {
            through: Some(SystemStreamOffset::try_new("00000000000000000007-vs00000001").unwrap()),
            sealed: true,
        },
    ] {
        assert_eq!(decode_progress(&encode_progress(&cursor)).unwrap(), cursor);
    }
}

#[test]
fn given_invalid_progress_value_then_error() {
    assert!(decode_progress(&[]).is_err());
    assert!(decode_progress(&[2]).is_err());
    assert!(decode_progress(&[0, 0xff, 0xfe]).is_err());
}

#[test]
fn given_lineage_when_keyed_then_suffix_decodes_and_keys_sort_inside_pair_prefix() {
    let first = lineage("s1", "l1");
    let receive = progress_key(TABLE, pair(), ProgressDirection::Receive, &first).unwrap();
    let send = progress_key(TABLE, pair(), ProgressDirection::Send, &first).unwrap();
    let receive_prefix = progress_prefix(TABLE, pair(), Some(ProgressDirection::Receive)).unwrap();
    let pair_prefix = progress_prefix(TABLE, pair(), None).unwrap();
    assert!(receive.starts_with(&receive_prefix));
    assert!(receive_prefix.starts_with(&pair_prefix));
    assert_eq!(
        decode_progress_lineage(&receive[receive_prefix.len()..]).unwrap(),
        first
    );
    // Every Send key of the pair sorts after every Receive key of the pair.
    let last_receive = progress_key(
        TABLE,
        pair(),
        ProgressDirection::Receive,
        &lineage("s9", "l9"),
    )
    .unwrap();
    assert!(send > last_receive);
    // A different pair never shares the prefix.
    let other = progress_key(
        TABLE,
        PairId::from_bytes([8; 16]),
        ProgressDirection::Receive,
        &first,
    )
    .unwrap();
    assert!(!other.starts_with(&pair_prefix));
    // Keys without both terminators do not decode.
    assert!(decode_progress_lineage(b"s1\0l1").is_err());
    assert!(decode_progress_lineage(b"s1").is_err());
}

#[test]
fn given_progress_key_then_family_is_regional_progress_in_data_range() {
    let key = progress_key(TABLE, pair(), ProgressDirection::Send, &lineage("s1", "l1")).unwrap();
    assert_eq!(
        table_data_family(&key),
        Some((TABLE, TableSubFamily::RegionalProgress))
    );
    assert_eq!(key_family(&key), "regional_progress");
    let (begin, end) = table_data_range(TABLE);
    assert!(begin <= key && key < end);
}

fn marker_table() -> storage_types::BoundTableInfo {
    let request: storage_types::CreateTableRequest = serde_json::from_value(serde_json::json!({
        "TableName": "orders",
        "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
        "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}]
    }))
    .unwrap();
    let info = request
        .stored_table_info_at(storage_types::TimestampMillis::from_timestamp(0))
        .unwrap();
    storage_types::BoundTableInfo {
        history_id: TABLE.into(),
        info: std::sync::Arc::new(info),
        binding: storage_types::TableBinding::default(),
    }
}

fn pk(value: &str) -> storage_types::KeyAttributes {
    storage_types::KeyAttributes::from_iter([(
        "pk".to_owned(),
        storage_types::AttributeValue::S(value.into()),
    )])
}

#[test]
fn given_marker_keys_when_listed_after_a_key_then_range_starts_above_it() {
    use crate::{regional::regional_marker_key, table_key_layout::table_prefix};

    let table = marker_table();
    let prefix = table_prefix(TABLE, TableSubFamily::RegionalMarker);
    let mut markers = ["a", "b", "ab", "c"]
        .map(|name| regional_marker_key(TABLE, &table, &pk(name)).unwrap())
        .to_vec();
    markers.sort();
    markers.dedup();
    assert_eq!(markers.len(), 4);
    for key in &markers {
        assert!(key.starts_with(&prefix));
    }
    // The `begin` the lister builds for `Some(after)` from a returned suffix.
    for (index, key) in markers.iter().enumerate() {
        let after = &key[prefix.len()..];
        let begin = [&prefix[..], after, &[0]].concat();
        assert!(begin > *key, "begin must be above the listed key");
        if let Some(next) = markers.get(index + 1) {
            assert!(begin <= *next, "begin must not skip the next marker key");
        }
    }
}
