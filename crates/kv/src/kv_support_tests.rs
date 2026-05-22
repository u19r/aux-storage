use uuid::Uuid;

#[cfg(not(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend"))))]
use crate::RocksDbKvStore;
#[cfg(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend")))]
use crate::backends::fdb::{FoundationDbConfig, FoundationDbKvStore};
use crate::{sorted_kv::SortedKvDbStorageProvider, sorted_kv_store::SortedKvStore};

#[cfg(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend")))]
pub type TestStore = FoundationDbKvStore;
#[cfg(not(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend"))))]
pub type TestStore = RocksDbKvStore;

pub type TestProvider = SortedKvDbStorageProvider<TestStore>;

#[must_use]
pub fn rocksdb_test_path(label: &str) -> std::path::PathBuf {
    let crate_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = crate_dir
        .parent()
        .and_then(|path| path.parent())
        .expect("workspace root");
    workspace_root
        .join("target")
        .join("kv-test-data")
        .join(format!("{label}-{}", Uuid::now_v7()))
}

#[must_use]
pub fn create_test_store() -> TestStore {
    #[cfg(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend")))]
    {
        let prefix = format!("tests/kv/{}/", Uuid::now_v7()).into_bytes();
        let config = FoundationDbConfig {
            subspace_prefix: Some(prefix),
            ..Default::default()
        };
        TestStore::connect(config).expect("connect foundationdb for tests")
    }

    #[cfg(not(all(feature = "foundationdb-backend", not(feature = "rocksdb-backend"))))]
    {
        let path = rocksdb_test_path("kv-test");
        TestStore::new(path).expect("create rocksdb test store")
    }
}

#[must_use]
pub fn create_test_provider() -> TestProvider {
    TestProvider::new(create_test_store())
}

pub async fn cleanup_store(store: &TestStore) {
    store
        .delete_prefix(Vec::new())
        .await
        .expect("cleanup store prefix");
}
