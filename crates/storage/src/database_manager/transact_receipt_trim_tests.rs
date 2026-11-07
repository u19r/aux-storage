//! Transact-write receipts are reclaimed once their ten-minute window closes.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicI64, Ordering},
    },
};

use storage_provider::{
    DynamoPort, DynamoTransactWriteAttempt,
    maintenance::{ExpirationPort, ExpirationRepository, TransactReceiptRow},
};
use storage_types::{AttemptMode, Clock, TimestampMillis, TransactWriteItemsResponse};

const NOW: i64 = 1_800_000_000_000;
const MINUTE: i64 = 60_000;

struct TestClock(AtomicI64);

impl Clock for TestClock {
    fn now(&self) -> TimestampMillis {
        TimestampMillis::from_timestamp(self.0.load(Ordering::SeqCst))
    }
}

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Retention {
    JobExecution,
}

#[derive(serde::Serialize)]
struct Receipt {
    request_identity: Vec<u8>,
    response: Vec<u8>,
    completed_at: TimestampMillis,
    #[serde(skip_serializing_if = "Option::is_none")]
    retention: Option<Retention>,
}

fn receipt(age_millis: i64, retention: Option<Retention>) -> Vec<u8> {
    storage_types::storage_serde::to_bytes(&Receipt {
        request_identity: vec![0; 4],
        response: serde_json::to_vec(&TransactWriteItemsResponse {
            consumed_capacity: None,
            item_collection_metrics: None,
        })
        .unwrap(),
        completed_at: TimestampMillis::from_timestamp(NOW - age_millis),
        retention,
    })
    .unwrap()
}

async fn write<S>(store: &S, token: &'static str, bytes: Vec<u8>)
where
    S: DynamoPort,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt,
{
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { attempt.write_transact_receipt(token, bytes).await })
        })
        .await
        .unwrap();
}

async fn stored<S>(store: &S, token: &'static str) -> Option<Vec<u8>>
where
    S: DynamoPort,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt,
{
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { attempt.read_transact_receipt(token).await })
        })
        .await
        .unwrap()
}

async fn trim_all<S: ExpirationRepository>(store: &S, limit: u32) -> usize {
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
    S: DynamoPort + ExpirationRepository,
    for<'native> S::Attempt<'native>: DynamoTransactWriteAttempt + ExpirationPort,
{
    let fresh = receipt(MINUTE, None);
    write(&store, "fresh", fresh.clone()).await;
    write(
        &store,
        "job",
        receipt(60 * MINUTE, Some(Retention::JobExecution)),
    )
    .await;
    for token in ["old-a", "old-b", "old-c", "old-d", "old-e"] {
        write(&store, token, receipt(11 * MINUTE, None)).await;
    }

    assert_eq!(
        trim_all(&store, 2).await,
        4,
        "seven rows span four pages of two"
    );

    for token in ["old-a", "old-b", "old-c", "old-d", "old-e"] {
        assert!(stored(&store, token).await.is_none(), "{token} survived");
    }
    assert_eq!(stored(&store, "fresh").await, Some(fresh.clone()));
    assert!(
        stored(&store, "job").await.is_some(),
        "a scheduled-job effect receipt outlives the request window"
    );

    // A receipt rewritten after the scan is not the scanned receipt.
    let stale = TransactReceiptRow {
        token: "fresh".to_owned(),
        receipt: receipt(30 * MINUTE, None),
    };
    store
        .with_dynamo_attempt(AttemptMode::ReadWrite, move |attempt| {
            Box::pin(async move { attempt.delete_transact_receipts(&[stale]).await })
        })
        .await
        .unwrap();
    assert_eq!(stored(&store, "fresh").await, Some(fresh));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn given_expired_receipts_when_trimmed_then_only_live_receipts_remain_on_sqlite() {
    let clock = Arc::new(TestClock(AtomicI64::new(NOW)));
    let driver = sql_backend::sqlite::SqliteBackend::initialize(":memory:")
        .await
        .unwrap();
    let store = sql_backend::SqlStore::initialize_schema(driver)
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
    let backend =
        kv_backend::rocksdb::RocksDbBackend::initialize(root.path().join("store")).unwrap();
    let store = kv_backend::KvStore::initialize_format_with_clock(backend, clock)
        .await
        .unwrap();
    verify_trim(store).await;
}
