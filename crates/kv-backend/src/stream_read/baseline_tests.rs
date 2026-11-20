use crate::KvStore;

#[tokio::test]
async fn given_baseline_mutations_when_followed_by_ordinary_write_then_only_modify_is_published() {
    let directory = tempfile::tempdir().unwrap();
    let backend = crate::rocksdb::RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .unwrap();
    storage_provider::baseline_test_support::qualify_baseline_history(store).await;
}
