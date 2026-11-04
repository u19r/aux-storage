//! A codec-level wide value must not turn bounded cleanup into a zero-progress
//! loop.

use storage_types::{
    IndexerDeclaration, ItemHistoryAddress, ItemHistoryChange, ItemHistoryEntry,
    ItemHistoryMetadata, ItemHistoryPublication, ItemHistoryRecord, ItemStreamVersion, MaxIndexers,
    StreamName, WireItem,
};

use super::{
    ItemHistoryMetadataPage,
    history_reclaim::{HistoryCleanupRead, admit_key_images},
};

#[test]
fn given_wide_first_history_header_when_cleanup_admits_keys_then_one_complete_value_is_admitted() {
    let history = ItemHistoryAddress {
        history_id: 1_u32.into(),
        stream_name: StreamName::new(b"table/stream-item/a"),
    };
    let version = ItemStreamVersion::new(1);
    let metadata = ItemHistoryMetadata::try_new(
        history.clone(),
        version,
        None,
        ItemHistoryPublication::PredecessorAnchor,
    )
    .unwrap();
    let payload = format!(
        "{{{}\"pk\":{{\"S\":\"a\"}}}}",
        " ".repeat(crate::constants::TABLE_HISTORY_READ_BYTES.get())
    );
    let entry = ItemHistoryEntry::try_new(
        metadata.clone(),
        ItemHistoryChange::Image {
            item: WireItem::dynamo_json(payload.into_bytes()),
            indexers: IndexerDeclaration::try_new(Vec::new(), MaxIndexers::ZERO).unwrap(),
        },
    )
    .unwrap();
    let bytes = entry.encode().unwrap();
    assert!(bytes.len() > crate::constants::TABLE_HISTORY_READ_BYTES.get());
    let header =
        ItemHistoryRecord::decode_header_prefix(&bytes, (&history, version), bytes.len()).unwrap();
    let pending = HistoryCleanupRead {
        frontier: crate::retention_test_support::frontier(7),
        work: 0,
        history,
        page: ItemHistoryMetadataPage {
            entries: vec![metadata],
            reached_tail: true,
        },
    };
    let (count, bound) = admit_key_images(&[pending], vec![Some(header)]).unwrap();
    assert_eq!(count, 1);
    assert!(bound.get() >= bytes.len());
}
