use storage_types::{StorageEnum, StreamItemId};

use crate::{
    RocksDbKvStore,
    backends::rocksdb::kv_store::{
        rocksdb_durability_policy, rocksdb_options, rocksdb_stream_high_water_key,
    },
    key_template::{KeyTemplate, PlaceholderBinding},
    kv_support_tests::rocksdb_test_path,
    sorted_kv_store::{DirectWriteOperation, SortedKvStore},
};

#[test]
fn rocksdb_durability_policy_uses_sync_writes_and_fsync() {
    let policy = rocksdb_durability_policy();
    let options = rocksdb_options();

    assert!(
        policy.sync_writes,
        "rocksdb writes must use synchronous WAL writes"
    );
    assert!(
        policy.use_fsync,
        "rocksdb must use fsync for the strongest local durability policy"
    );
    assert!(
        options.get_use_fsync(),
        "rocksdb DB options must apply the fsync durability policy"
    );
}

#[test]
fn creates_parent_directory_for_rocksdb_path() {
    let target_dir = rocksdb_test_path("kv-store-parent").join("nested/rocksdb");

    assert!(
        !target_dir.parent().expect("parent").exists(),
        "nested rocksdb parent directory should not exist before initialization"
    );

    let _store = RocksDbKvStore::new(target_dir.clone()).unwrap();

    assert!(
        target_dir.parent().expect("parent").exists(),
        "rocksdb initialization should create the parent directory"
    );
}

#[test]
fn rocksdb_open_fails_closed_when_database_path_is_a_file() {
    let target = rocksdb_test_path("kv-store-file-path");
    std::fs::create_dir_all(target.parent().expect("parent")).unwrap();
    std::fs::write(&target, b"not a rocksdb directory").unwrap();

    let error = match RocksDbKvStore::new(target) {
        Ok(_) => panic!("rocksdb open should reject file path"),
        Err(error) => error,
    };

    match error.as_ref() {
        StorageEnum::InternalServerError { message } => {
            assert!(
                message.contains("open rocksdb failed"),
                "error should identify rocksdb open failure: {message}"
            );
        }
        other => panic!("expected internal rocksdb open failure, got {other:?}"),
    }
}

#[tokio::test]
async fn rocksdb_stream_id_allocation_resumes_above_durable_high_water_after_reopen() {
    let path = rocksdb_test_path("kv-stream-high-water");
    let high_water = stream_id_from_u64(9_000);
    let next = stream_id_from_u64(9_001);
    let high_water_key = rocksdb_stream_high_water_key();
    {
        let store = RocksDbKvStore::new(path.clone()).unwrap();
        store
            .put(&high_water_key, high_water.as_bytes(), None)
            .await
            .unwrap();
    }

    let store = RocksDbKvStore::new(path).unwrap();
    let template = KeyTemplate::placeholder(
        b"streams/test/".to_vec(),
        Vec::new(),
        PlaceholderBinding::unique(stream_id_from_u64(1).as_bytes().to_vec()),
    );
    store
        .transact_write_unchecked(vec![DirectWriteOperation::PutTemplate {
            template,
            value: b"payload".to_vec(),
        }])
        .await
        .unwrap();

    let written_key = [b"streams/test/".as_slice(), next.as_bytes().as_slice()].concat();
    assert_eq!(
        store.get(&written_key, true).await.unwrap(),
        Some(b"payload".to_vec())
    );
    assert_eq!(
        store.get(&high_water_key, true).await.unwrap().as_deref(),
        Some(next.as_bytes().as_slice())
    );
    assert_eq!(
        store
            .get(b"sys/streams/rocksdb/high-water", true)
            .await
            .unwrap(),
        None
    );
}

fn stream_id_from_u64(value: u64) -> StreamItemId {
    let mut bytes = [0u8; 12];
    bytes[4..].copy_from_slice(&value.to_be_bytes());
    StreamItemId::from(bytes)
}
