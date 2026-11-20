//! Format initialisation after an unknown commit answer on an owned
//! FoundationDB fixture.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use storage_types::StorageError;

use super::driver::{FoundationDbBackend, FoundationDbOptions};
use crate::KvStore;

async fn open_backend(namespace: &str) -> FoundationDbBackend {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(namespace.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[derive(Default)]
struct FormatAnswer1021 {
    fired: AtomicBool,
    unknown_1021: AtomicUsize,
    stages_after: AtomicUsize,
}

impl crate::KvFaultProbe for FormatAnswer1021 {
    fn fail(&self, call: crate::KvFaultCall) -> Option<StorageError> {
        if call.operation == crate::KvFaultOperation::Stage
            && call.mutations > 0
            && self.fired.load(Ordering::SeqCst)
        {
            self.stages_after.fetch_add(1, Ordering::SeqCst);
        }
        None
    }

    fn replace_commit_answer(&self) -> Option<i32> {
        (!self.fired.swap(true, Ordering::SeqCst)).then_some(1021)
    }

    fn observe(&self, event: crate::KvNativeEvent) {
        if let crate::KvNativeEvent::CommitFailed {
            native_code: 1021,
            kind,
        } = event
        {
            assert_eq!(kind, Some(storage_types::AttemptKind::CommitUnknown));
            self.unknown_1021.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_answer_1021_at_format_commit_when_initialized_then_next_round_accepts_committed_rows()
 {
    let namespace = format!(
        "own-b/{}/{}/format-1021",
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    let probe = Arc::new(FormatAnswer1021::default());
    let backend = open_backend(&namespace)
        .await
        .with_fault_probe(probe.clone());
    let first =
        KvStore::initialize_format(backend, storage_types::InitialStreamLineages::ONE).await;
    assert!(first.is_ok());
    drop(first);
    assert_eq!(probe.unknown_1021.load(Ordering::SeqCst), 1);
    assert_eq!(probe.stages_after.load(Ordering::SeqCst), 0);

    let second = KvStore::initialize_format(
        open_backend(&namespace).await,
        storage_types::InitialStreamLineages::ONE,
    )
    .await;
    assert!(second.is_ok());
    println!(
        "OWN_B_FORMAT_1021 unknown={} stages_after_unknown={}",
        probe.unknown_1021.load(Ordering::SeqCst),
        probe.stages_after.load(Ordering::SeqCst)
    );
}
