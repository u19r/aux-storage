use storage_types::{
    AttemptMode, AttemptPlan, DeadlineClass, ReadProtection, StreamItemId, TableHistoryId,
    TableHistoryTarget, TableName,
};
use stream_provider::{
    STREAM_APPEND_PROOF_BYTES, StreamAppendAllocation, StreamAppendOperationId, StreamAppendProof,
    StreamAppendReceipt, TableAppendRequest,
};

use crate::{KvBackend as _, rocksdb::RocksDbBackend};

#[tokio::test]
async fn given_widest_proof_when_receipt_is_stored_then_it_loads_unchanged() {
    let directory = tempfile::tempdir().unwrap();
    let backend = RocksDbBackend::initialize(
        directory.path().to_owned(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let history_id = TableHistoryId::from(7u32);
    let target = TableHistoryTarget {
        table_name: TableName::new("t"),
        history_id,
    };
    let request = TableAppendRequest::new(StreamAppendOperationId::fresh(), target, b"x");
    let receipt = StreamAppendReceipt::new(
        &request,
        StreamAppendProof::try_new(vec![255; STREAM_APPEND_PROOF_BYTES]).unwrap(),
        StreamAppendAllocation::AttemptAllocated(StreamItemId::from([1; 12])),
    );

    let stored = receipt.clone();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move { super::store_append_receipt(attempt, &stored).await })
            },
        )
        .await
        .unwrap();

    let key = (&receipt).into();
    let loaded = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move {
                    super::load_append_receipt(attempt, ReadProtection::Snapshot, key).await
                })
            },
        )
        .await
        .unwrap();
    assert_eq!(loaded, Some(receipt.clone()));

    let again = receipt.clone();
    let error = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            move |attempt| {
                Box::pin(async move { super::store_append_receipt(attempt, &again).await })
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.class(), storage_types::ErrorClass::Internal);
}
