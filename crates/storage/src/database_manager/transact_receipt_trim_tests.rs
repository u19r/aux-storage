//! Transact-write receipts are reclaimed five minutes after their ten-minute
//! window closes.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use storage_provider::{
    DynamoPort, DynamoTransactWriteAttempt,
    maintenance::{ExpirationPort, MaintenanceOperations, TransactReceiptRow},
};
use storage_types::{
    AttemptMode, AttemptPlan, Clock, DeadlineClass, TimestampMillis, TransactWriteItemsResponse,
};

const NOW: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[derive(serde::Serialize)]
struct Receipt {
    request_identity: Vec<u8>,
    response: Vec<u8>,
    completed_at: TimestampMillis,
}

fn receipt(age_millis: i64) -> Vec<u8> {
    storage_types::storage_serde::to_bytes(&Receipt {
        request_identity: vec![0; 4],
        response: serde_json::to_vec(&TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        })
        .unwrap(),
        completed_at: TimestampMillis::from_timestamp(NOW - age_millis),
    })
    .unwrap()
}

async fn write<S>(store: &S, token: &'static str, bytes: Vec<u8>)
where
    S: DynamoPort,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt,
{
    storage_provider::DynamoPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                attempt
                    .stage_transact_receipt(storage_provider::ReceiptSlot::Request(token), bytes)?;
                storage_provider::DynamoStreamWriteAttempt::flush_dynamo_write(attempt).await
            })
        },
    )
    .await
    .unwrap();
}

async fn stored<S>(store: &S, token: &'static str) -> Option<Vec<u8>>
where
    S: DynamoPort,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt,
{
    storage_provider::DynamoPort::with_attempt(
        store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(store),
        ),
        move |attempt| {
            Box::pin(async move {
                attempt
                    .read_transact_receipt(storage_provider::ReceiptSlot::Request(token))
                    .await
            })
        },
    )
    .await
    .unwrap()
}

async fn trim_all<S: MaintenanceOperations>(store: &S, limit: u32) -> usize {
    let limit = NonZeroU32::new(limit).unwrap();
    let mut pages = 1;
    let mut after = None;
    while let Some(next) = store
        .trim_transact_receipts_page(after, limit)
        .await
        .unwrap()
    {
        after = Some(next);
        pages += 1;
    }
    pages
}

async fn verify_trim<S>(store: S)
where
    S: DynamoPort + MaintenanceOperations,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt + ExpirationPort,
{
    let fresh = receipt(MINUTE);
    write(&store, "fresh", fresh.clone()).await;
    for token in ["old-a", "old-b", "old-c", "old-d", "old-e"] {
        write(&store, token, receipt(16 * MINUTE)).await;
    }

    assert_eq!(
        trim_all(&store, 2).await,
        4,
        "six rows span four pages of two"
    );

    for token in ["old-a", "old-b", "old-c", "old-d", "old-e"] {
        assert!(stored(&store, token).await.is_none(), "{token} survived");
    }
    assert_eq!(stored(&store, "fresh").await, Some(fresh.clone()));

    // A receipt rewritten after the scan is not the scanned receipt.
    let stale = TransactReceiptRow {
        token: "fresh".to_owned(),
        receipt: receipt(30 * MINUTE),
    };
    storage_provider::DynamoPort::with_attempt(
        &store,
        AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(
            storage_types::AttemptDeadlineSource::attempt_deadlines(&store),
        ),
        move |attempt| Box::pin(async move { attempt.delete_transact_receipts(&[stale]).await }),
    )
    .await
    .unwrap();
    assert_eq!(stored(&store, "fresh").await, Some(fresh));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_expired_receipts_when_trimmed_then_only_live_receipts_remain_on_sqlite() {
    let clock = Arc::new(TestClock(AtomicI64::new(NOW)));
    let driver = sql_backend::sqlite::SqliteBackend::initialize(
        ":memory:",
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let store = sql_backend::SqlStore::initialize_schema(
        driver,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap()
    .with_clock(clock);
    verify_trim(store).await;
}

#[cfg(feature = "rocksdb")]
#[tokio::test]
async fn given_expired_receipts_when_trimmed_then_only_live_receipts_remain_on_rocksdb() {
    let clock = Arc::new(TestClock(AtomicI64::new(NOW)));
    let root = tempfile::tempdir().unwrap();
    let backend = kv_backend::rocksdb::RocksDbBackend::initialize(
        root.path().join("store"),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    let store = kv_backend::KvStore::initialize_format_with_clock(
        backend,
        storage_types::InitialStreamLineages::ONE,
        clock,
    )
    .await
    .unwrap();
    verify_trim(store).await;
}
