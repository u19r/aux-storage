//! Table-first key shapes and the table data-range invariant.

use crate::table_key_layout::*;

const TABLE: u32 = 0x0102_0304;

fn table_bytes(tag: u8, rest: &[u8]) -> Vec<u8> {
    [&[b'T', 1, 2, 3, 4, tag][..], rest].concat()
}

#[test]
fn given_table_families_when_encoded_then_each_has_the_table_first_bytes() {
    assert_eq!(
        crate::item_read::item_key_from_suffix(TABLE, b"sfx").unwrap(),
        table_bytes(b'p', b"sfx")
    );
    assert_eq!(
        crate::gsi::index_entry_prefix(TABLE, 7).unwrap(),
        table_bytes(b'g', &[0, 7])
    );
    assert_eq!(crate::catalog::metadata_key(TABLE), table_bytes(b'm', &[]));
    assert_eq!(
        crate::ttl_policy::ttl_config_key(TABLE),
        table_bytes(b'c', &[])
    );
    let ttl = storage_common::TtlIndexKey::new(0, "t".to_owned().into());
    let mut expected = table_bytes(b'e', &[0, 0x80, 0, 0, 0, 0, 0, 0, 0]);
    expected.push(b't');
    assert_eq!(
        crate::expiration::key_layout::encode_index_key(TABLE, &ttl).unwrap(),
        expected
    );
}

#[test]
fn given_owner_keys_when_chunked_then_table_owners_keep_chunks_in_their_table() {
    let item = table_bytes(b'p', b"ab");
    let address = crate::value_layout::ChunkAddress::prepare(item.as_slice()).unwrap();
    assert_eq!(
        address.chunk_key(2).unwrap(),
        table_bytes(b'k', &[0, 3, b'p', b'a', b'b', 0, 2])
    );
    let global = b"\x7fp/v/x".to_vec();
    let address = crate::value_layout::ChunkAddress::prepare(global.as_slice()).unwrap();
    assert_eq!(
        address.chunk_key(1).unwrap(),
        [&b"\0chunk/"[..], &[0, 6], &global, &[0, 1]].concat()
    );
    // A versionstamped owner is known by its prefix and complete length.
    assert_eq!(
        crate::value_layout::chunk_prefix(b"\x7fs/i/", 17).unwrap(),
        [&b"\0chunk/"[..], &[0, 17], b"\x7fs/i/"].concat()
    );
}

#[test]
fn given_table_data_range_when_bounded_then_every_family_is_inside_and_reserved_tags_are_not() {
    let (begin, end) = table_data_range(TABLE);
    assert_eq!(begin, table_bytes(0, &[]));
    assert_eq!(end, table_bytes(TABLE_RESERVED_SUB_TAG, &[]));
    for family in [
        TableSubFamily::GsiNameLock,
        TableSubFamily::RegionalMarker,
        TableSubFamily::RegionalProgress,
        TableSubFamily::GsiBuild,
        TableSubFamily::TtlConfig,
        TableSubFamily::TtlIndex,
        TableSubFamily::GsiEntry,
        TableSubFamily::Chunk,
        TableSubFamily::Catalog,
        TableSubFamily::Item,
    ] {
        let key = table_prefix(TABLE, family).to_vec();
        assert!(begin <= key && key < end);
        assert_eq!(table_data_family(&key), Some((TABLE, family)));
    }
    assert_eq!(
        table_data_family(&table_bytes(TABLE_RESERVED_SUB_TAG, &[])),
        None
    );
}

fn history() -> storage_types::TableHistoryId {
    storage_types::TableHistoryId::from(TABLE)
}

#[test]
fn given_stream_families_when_encoded_then_each_is_in_the_stream_part_of_its_table() {
    let position = storage_types::SystemStreamPosition::try_new("s", "l", "5").unwrap();
    let mut entry_rest = b"s\0l\0".to_vec();
    entry_rest.extend_from_slice(&5u64.to_be_bytes());
    entry_rest.extend_from_slice(b"5");
    assert_eq!(
        crate::stream_write::key_layout::entry_key(history(), &position).unwrap(),
        table_bytes(0x80, &entry_rest)
    );
    let append = [&[0u8, 12][..], b"append-table"].concat();
    assert_eq!(
        crate::stream_write::key_layout::item_row_prefix(&history().append_stream_name()).unwrap(),
        table_bytes(0x81, &append)
    );
    assert_eq!(
        crate::stream_write::receipt_prefix(history()).unwrap(),
        table_bytes(0x82, &[])
    );
    assert_eq!(
        crate::stream_write::counter::user_stream_sequence_key(&history().append_stream_name())
            .unwrap(),
        table_bytes(0x83, &append)
    );
    let scope_id = storage_provider::table_retention_scope_id(history());
    let table_name = || storage_types::TableName::try_from("t").unwrap();
    assert_eq!(
        crate::retention::state_key(&storage_provider::StreamTrimScope::table(
            scope_id.clone(),
            table_name()
        )),
        Some(table_bytes(0x84, &[]))
    );
    let top_byte =
        storage_types::TableHistoryId::from([1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
    for scope_id in [
        "ret-missing/no-such-scope".to_owned(),
        scope_id
            .get(..scope_id.len() - 1)
            .expect("ASCII scope id")
            .to_owned(),
        format!("table:{:032X}", 0x0a0b_0c0d_u128),
        storage_provider::table_retention_scope_id(top_byte),
    ] {
        assert_eq!(
            crate::retention::state_key(&storage_provider::StreamTrimScope::table(
                scope_id,
                table_name()
            )),
            None
        );
    }
    assert_eq!(
        crate::stream_write::counter::user_stream_sequence_key(&storage_types::StreamName::new(
            b"user"
        ))
        .unwrap(),
        b"\x7fu/q/\0\x04user".to_vec()
    );
}

#[test]
fn given_stream_and_queue_owners_when_chunked_then_each_keeps_chunks_in_its_own_home() {
    let owner = table_bytes(0x80, b"ab");
    let address = crate::value_layout::ChunkAddress::prepare(owner.as_slice()).unwrap();
    let stream_chunk = address.chunk_key(2).unwrap();
    assert_eq!(
        stream_chunk,
        table_bytes(0x85, &[0, 3, 0x80, b'a', b'b', 0, 2])
    );
    assert_eq!(
        crate::value_layout::chunk_prefix(&table_bytes(0x81, b"x"), 19).unwrap(),
        table_bytes(0x85, &[0, 14, 0x81, b'x'])
    );
    let queue = [&b"\x7fq/m/"[..], &[7; 16], &[9; 12]].concat();
    let address = crate::value_layout::ChunkAddress::prepare(queue.as_slice()).unwrap();
    let queue_chunk = address.chunk_key(1).unwrap();
    assert_eq!(
        queue_chunk,
        [&b"\x7fq/k/"[..], &[7; 16], &[0, 12], &[9; 12], &[0, 1]].concat()
    );
    let global = b"\x7fp/v/x".to_vec();
    let address = crate::value_layout::ChunkAddress::prepare(global.as_slice()).unwrap();
    let global_chunk = address.chunk_key(1).unwrap();
    assert_eq!(
        global_chunk,
        [&b"\0chunk/"[..], &[0, 6], &global, &[0, 1]].concat()
    );
    let short = b"\x7fq/m/x".to_vec();
    let address = crate::value_layout::ChunkAddress::prepare(short.as_slice()).unwrap();
    assert_eq!(
        address.chunk_key(1).unwrap(),
        [&b"\0chunk/"[..], &[0, 6], &short, &[0, 1]].concat()
    );
    #[cfg(feature = "fault-injection")]
    {
        for chunk in [&stream_chunk, &queue_chunk, &global_chunk] {
            assert!(crate::is_value_chunk_key(chunk));
        }
        assert!(!crate::is_value_chunk_key(&owner));
        assert!(!crate::is_value_chunk_key(&queue));
    }
}

#[test]
fn given_table_stream_range_when_bounded_then_it_holds_every_stream_family_and_no_data_family() {
    let (begin, end) = table_stream_range(TABLE);
    assert_eq!(begin, table_bytes(0x80, &[]));
    assert_eq!(end, vec![b'T', 1, 2, 3, 5]);
    for family in [
        TableStreamFamily::Entry,
        TableStreamFamily::AppendRow,
        TableStreamFamily::AppendReceipt,
        TableStreamFamily::AppendSequence,
        TableStreamFamily::TrimState,
        TableStreamFamily::Chunk,
    ] {
        let key = table_stream_prefix(TABLE, family).to_vec();
        assert!(begin <= key && key < end);
        assert_eq!(table_stream_family(&key), Some((TABLE, family)));
        assert_eq!(table_data_family(&key), None);
    }
    for family in [
        TableSubFamily::GsiNameLock,
        TableSubFamily::RegionalMarker,
        TableSubFamily::RegionalProgress,
        TableSubFamily::GsiBuild,
        TableSubFamily::TtlConfig,
        TableSubFamily::TtlIndex,
        TableSubFamily::GsiEntry,
        TableSubFamily::Chunk,
        TableSubFamily::Catalog,
        TableSubFamily::Item,
    ] {
        let key = table_prefix(TABLE, family).to_vec();
        assert!(!(begin <= key && key < end));
        assert_eq!(table_stream_family(&key), None);
    }
    assert_eq!(table_stream_range(u32::MAX).1, b"U".to_vec());
}

#[test]
fn given_regional_marker_family_when_prefixed_then_tag_d_inside_the_table_data_range() {
    let key = table_prefix(TABLE, TableSubFamily::RegionalMarker).to_vec();
    assert_eq!(key, table_bytes(b'D', &[]));
    assert_eq!(
        TableSubFamily::from_tag(b'D'),
        Some(TableSubFamily::RegionalMarker)
    );
    assert_eq!(
        table_data_family(&key),
        Some((TABLE, TableSubFamily::RegionalMarker))
    );
    let (begin, end) = table_data_range(TABLE);
    assert!(begin <= key && key < end);
}

#[test]
fn given_regional_progress_family_when_prefixed_then_tag_p_inside_the_table_data_range() {
    let key = table_prefix(TABLE, TableSubFamily::RegionalProgress).to_vec();
    assert_eq!(key, table_bytes(b'P', &[]));
    assert_eq!(
        TableSubFamily::from_tag(b'P'),
        Some(TableSubFamily::RegionalProgress)
    );
    let (begin, end) = table_data_range(TABLE);
    assert!(begin <= key && key < end);
}
