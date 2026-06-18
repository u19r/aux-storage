use crate::{
    backends::common::{KvMutation, gsi_mutation_key},
    keyspace::compact::{self, IndexStorageId, TableStorageId},
};

#[test]
fn gsi_mutation_detection_uses_compact_families_not_legacy_path_segments() {
    let table_id = TableStorageId::new(42);
    let index_id = IndexStorageId::new(3);
    let gsi_key = compact::gsi_item_key(table_id, index_id, b"gsi-suffix");
    let tombstone_key = compact::gsi_tombstone_key(table_id, index_id, b"gsi-suffix");
    let primary_key = compact::primary_item_key(table_id, b"pk/index/not-gsi");

    assert!(
        gsi_mutation_key(&KvMutation::Put {
            key: gsi_key,
            value: Vec::new()
        })
        .is_some()
    );
    assert!(gsi_mutation_key(&KvMutation::Delete { key: tombstone_key }).is_some());
    assert!(
        gsi_mutation_key(&KvMutation::Put {
            key: primary_key,
            value: Vec::new()
        })
        .is_none()
    );
    assert!(
        gsi_mutation_key(&KvMutation::Delete {
            key: b"orders/index/by_status/data/suffix".to_vec()
        })
        .is_none(),
        "legacy path bytes should not be classified as current GSI mutations"
    );
}
