//! Append receipt layout on an owned FoundationDB fixture: one unchunked value,
//! no range clears.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use foundationdb::options::TransactionOption;
use storage_provider::TableReclaimStep;
use storage_types::{
    AttemptMode, AttemptPlan, DeadlineClass, ReadProtection, StorageError, StreamItemId,
    TableHistoryId, TableHistoryTarget, TableName,
};
use stream_provider::{
    STREAM_APPEND_PROOF_BYTES, StreamAppendAllocation, StreamAppendOperationId, StreamAppendProof,
    StreamAppendReceipt, TableAppendRequest,
};

use super::driver::{FoundationDbBackend, FoundationDbOptions};
use crate::{KvBackend as _, stream_write};

#[derive(Default)]
struct MutationCounts {
    puts: AtomicUsize,
    deletes: AtomicUsize,
    range_deletes: AtomicUsize,
}

impl MutationCounts {
    fn reset(&self) {
        self.puts.store(0, Ordering::SeqCst);
        self.deletes.store(0, Ordering::SeqCst);
        self.range_deletes.store(0, Ordering::SeqCst);
    }

    fn snapshot(&self) -> (usize, usize, usize) {
        (
            self.puts.load(Ordering::SeqCst),
            self.deletes.load(Ordering::SeqCst),
            self.range_deletes.load(Ordering::SeqCst),
        )
    }
}

impl crate::KvFaultProbe for MutationCounts {
    fn fail(&self, _call: crate::KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: crate::KvNativeEvent) {
        if let crate::KvNativeEvent::Mutations {
            puts,
            deletes,
            range_deletes,
        } = event
        {
            self.puts.fetch_add(puts, Ordering::SeqCst);
            self.deletes.fetch_add(deletes, Ordering::SeqCst);
            self.range_deletes
                .fetch_add(range_deletes, Ordering::SeqCst);
        }
    }
}

async fn open_backend(label: &str) -> (FoundationDbBackend, Arc<MutationCounts>) {
    let cluster = std::env::var("TEST_FDB_CLUSTER_FILE").expect("run just test-fdb-owned");
    let namespace = format!(
        "own-b/{}/{}/{label}",
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    let counts = Arc::new(MutationCounts::default());
    let backend = FoundationDbBackend::initialize(FoundationDbOptions {
        cluster_file: Some(&cluster),
        subspace_prefix: Some(namespace.as_bytes()),
        ..Default::default()
    })
    .await
    .unwrap()
    .with_fault_probe(counts.clone());
    (backend, counts)
}

fn history() -> TableHistoryId {
    TableHistoryId::from(7u32)
}

fn receipt(response: StreamAppendAllocation) -> StreamAppendReceipt {
    let request = TableAppendRequest::new(
        StreamAppendOperationId::fresh(),
        TableHistoryTarget {
            table_name: TableName::new("t"),
            history_id: history(),
        },
        b"x",
    );
    StreamAppendReceipt::new(
        &request,
        StreamAppendProof::try_new(vec![255; STREAM_APPEND_PROOF_BYTES]).unwrap(),
        response,
    )
}

fn attempt_allocated() -> StreamAppendAllocation {
    StreamAppendAllocation::AttemptAllocated(StreamItemId::from([1; 12]))
}

fn write_plan(backend: &FoundationDbBackend) -> storage_types::AttemptRequest {
    AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
        .start(backend.attempt_deadlines())
}

async fn store(backend: &FoundationDbBackend, receipts: Vec<StreamAppendReceipt>) {
    backend
        .with_attempt(write_plan(backend), move |attempt| {
            Box::pin(async move {
                for receipt in &receipts {
                    stream_write::store_append_receipt(attempt, receipt).await?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

async fn load(
    backend: &FoundationDbBackend,
    receipt: &StreamAppendReceipt,
) -> Option<StreamAppendReceipt> {
    let key = receipt.into();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    stream_write::load_append_receipt(attempt, ReadProtection::Snapshot, key).await
                })
            },
        )
        .await
        .unwrap()
}

async fn raw_value(backend: &FoundationDbBackend, receipt: &StreamAppendReceipt) -> Vec<u8> {
    let mut key = backend.prefix.to_vec();
    key.extend_from_slice(&stream_write::receipt_prefix(receipt.history_id).unwrap());
    key.extend_from_slice(&receipt.operation.bytes());
    let transaction = backend.database.create_trx().unwrap();
    transaction
        .set_option(TransactionOption::Timeout(10_000))
        .unwrap();
    transaction
        .get(&key, false)
        .await
        .unwrap()
        .expect("receipt value is stored")
        .to_vec()
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_attempt_allocated_receipt_when_stored_on_foundationdb_then_one_unchunked_put() {
    let (backend, counts) = open_backend("receipt-put").await;
    let receipt = receipt(attempt_allocated());

    store(&backend, vec![receipt.clone()]).await;

    assert_eq!(counts.snapshot(), (1, 0, 0));
    let raw = raw_value(&backend, &receipt).await;
    crate::value_layout::decode_unchunked_value(&raw, stream_write::APPEND_RECEIPT_VALUE_BYTES)
        .unwrap();
    assert_eq!(load(&backend, &receipt).await, Some(receipt));
    println!("OWN_B_RECEIPT_PUT puts=1 range_deletes=0");
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_versionstamped_receipt_when_stored_then_one_stamped_unchunked_value() {
    let (backend, counts) = open_backend("receipt-stamped").await;
    let receipt = receipt(StreamAppendAllocation::Versionstamped([0, 1]));

    let stored = receipt.clone();
    let ((), stamp) = backend
        .with_versionstamped_attempt(write_plan(&backend), move |attempt| {
            Box::pin(async move { stream_write::store_append_receipt(attempt, &stored).await })
        })
        .await
        .unwrap();
    let stamp = stamp.expect("a write commit has a versionstamp");

    let (puts, _deletes, range_deletes) = counts.snapshot();
    assert_eq!(range_deletes, 0);
    // The driver counts the one stamped value as a put.
    assert_eq!(puts, 1);
    let raw = raw_value(&backend, &receipt).await;
    assert!(raw.starts_with(b"\0vs/"));
    assert_eq!(raw[4..14], stamp);
    crate::value_layout::decode_unchunked_value(&raw, stream_write::APPEND_RECEIPT_VALUE_BYTES)
        .unwrap();
    let loaded = load(&backend, &receipt).await.expect("receipt loads");
    let StreamAppendAllocation::AttemptAllocated(id) = loaded.response else {
        panic!("loaded receipt kept an unresolved versionstamp");
    };
    let id: [u8; 12] = *id.as_bytes();
    assert_eq!(id[..10], stamp);
    assert_eq!(id[10..], [0, 1]);
    println!("OWN_B_RECEIPT_STAMPED range_deletes=0");
}

#[tokio::test]
#[ignore = "requires just test-fdb-owned"]
async fn given_receipt_family_when_reclaimed_then_unchunked_deletes_only() {
    let (backend, counts) = open_backend("receipt-reclaim").await;
    let receipts: Vec<_> = (0..3).map(|_| receipt(attempt_allocated())).collect();
    store(&backend, receipts.clone()).await;
    counts.reset();

    let step = backend
        .with_attempt(write_plan(&backend), |attempt| {
            Box::pin(async move {
                crate::table_reclaim::delete_family_pages(
                    attempt,
                    vec![(
                        stream_write::receipt_prefix(history())?,
                        crate::table_reclaim::ReclaimValues::Unchunked,
                    )],
                )
                .await
            })
        })
        .await
        .unwrap();

    assert!(matches!(step, TableReclaimStep::Complete));
    let (_puts, deletes, range_deletes) = counts.snapshot();
    assert_eq!(deletes, 3);
    assert_eq!(range_deletes, 0);
    for receipt in &receipts {
        assert_eq!(load(&backend, receipt).await, None);
    }
    println!("OWN_B_RECEIPT_RECLAIM deletes=3 range_deletes=0");
}
