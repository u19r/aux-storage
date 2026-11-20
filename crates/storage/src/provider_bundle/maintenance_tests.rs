//! The maintenance source binds one background store value; every native
//! attempt takes a background permit.

use std::{
    num::NonZeroU32,
    sync::{Arc, Mutex},
    time::Duration,
};

use kv_backend::{KvStore, rocksdb::RocksDbBackend};
use storage_common::native_admission::{
    AttemptAdmission, NativeAdmission, NativeAdmissionBinding, NativeAdmissionFuture,
    NativeAdmissionOutcome, NativeAdmissionPermit, NativeWork,
};
use storage_types::StorageResult;

use super::{InitializedStore, models::MaintenanceRepositories};

struct RecordingPermit;

impl NativeAdmissionPermit for RecordingPermit {
    fn complete(self: Box<Self>, _outcome: NativeAdmissionOutcome<'_>, _elapsed: Duration) {}
}

/// Records the work class of each acquisition.
#[derive(Default)]
struct RecordingAdmission {
    works: Mutex<Vec<&'static str>>,
}

impl NativeAdmission for RecordingAdmission {
    fn connection_id(&self) -> &str {
        "test"
    }

    fn require_reserved_attempts(&self, _required: std::num::NonZeroUsize) -> StorageResult<()> {
        Ok(())
    }

    fn acquire(&self, work: NativeWork) -> NativeAdmissionFuture<'_> {
        let name = match work {
            NativeWork::Recovery => "recovery",
            NativeWork::PointRead => "point_read",
            NativeWork::RangeRead => "range_read",
            NativeWork::Write => "write",
            NativeWork::Background => "background",
        };
        self.works.lock().expect("works lock").push(name);
        let permit: Box<dyn NativeAdmissionPermit> = Box::new(RecordingPermit);
        Box::pin(async move { Ok(permit) })
    }

    fn drain_native(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + '_>> {
        Box::pin(async { Ok(()) })
    }
}

async fn source(root: &tempfile::TempDir) -> InitializedStore {
    let backend = RocksDbBackend::initialize(root.path().join("store"), Default::default(), 256)
        .expect("driver");
    let store = KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE)
        .await
        .expect("initialized store");
    InitializedStore::native(Arc::new(store))
}

fn prepared(
    source: &InitializedStore,
    admission: Arc<dyn storage_common::native_admission::NativeAdmission>,
) -> MaintenanceRepositories {
    source
        .native_store()
        .expect("native fixture")
        .maintenance(AttemptAdmission::Background(Arc::new(
            NativeAdmissionBinding::bound(admission),
        )))
}

#[tokio::test]
async fn given_bound_maintenance_source_when_a_receipt_trim_page_runs_then_it_takes_background_permits()
 {
    let root = tempfile::tempdir().unwrap();
    let source = source(&root).await;
    let admission = Arc::new(RecordingAdmission::default());
    let repositories = prepared(&source, admission.clone());

    let next = repositories
        .operations
        .trim_transact_receipts_page(None, NonZeroU32::new(10).unwrap())
        .await
        .expect("trim page");

    assert_eq!(next, None);
    let recorded = admission.works.lock().unwrap().clone();
    assert!(!recorded.is_empty(), "the page takes at least one permit");
    assert!(
        recorded.iter().all(|work| *work == "background"),
        "{recorded:?}"
    );
    assert_eq!(
        recorded.len(),
        1,
        "one try without a conflict takes one permit"
    );
}

#[tokio::test]
async fn given_kv_maintenance_source_when_prepared_then_partition_reconciliation_is_present() {
    let root = tempfile::tempdir().unwrap();
    let source = source(&root).await;

    let repositories = prepared(&source, Arc::new(RecordingAdmission::default()));

    assert!(repositories.partition_reconciliation_repository.is_some());
    assert!(repositories.queue_message_cleanup_repository.is_some());
}
