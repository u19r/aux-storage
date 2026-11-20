use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use storage_provider::DynamoOperations as _;
use storage_types::{AttemptMode, AttemptPlan, DeadlineClass};

use crate::{SqlBackend as _, SqlFaultProbe, SqlNativeEvent};

#[derive(Default)]
struct NativeCalls(AtomicUsize, AtomicBool);

impl SqlFaultProbe for NativeCalls {
    fn fail(&self, _call: crate::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        None
    }
    fn observe(&self, _event: SqlNativeEvent) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn at_commit_boundary(
        &self,
        boundary: crate::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        Box::pin(async move {
            if boundary == crate::SqlCommitBoundary::AfterDurableAcknowledgement
                && self.1.swap(false, Ordering::Relaxed)
            {
                tokio::time::pause();
                // Past the whole Write budget (10 s by default).
                tokio::time::advance(std::time::Duration::from_secs(11)).await;
                tokio::time::resume();
            }
            Ok(())
        })
    }
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_held_pool_when_deadline_expires_then_callback_never_runs() {
    let fixture = super::test_support::NativeFixture::open().await;
    eprintln!("owned_schema={}", fixture.schema);
    let held = fixture.backend.pool.get().await.unwrap();
    let backend = Arc::clone(&fixture.backend);
    tokio::time::pause();
    let deadline = backend.deadlines.start(DeadlineClass::Write);
    let result = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWriteItems, deadline.class).started(deadline),
            |_| Box::pin(async { panic!("expired acquisition must not invoke callback") }),
        )
        .await;
    let result: storage_types::StorageResult<()> = result;
    assert!(
        result
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert!(tokio::time::Instant::now() >= deadline.expires_at);
    tokio::time::resume();
    drop(held);
    assert_eq!(fixture.count_records().await, 0);
    drop(backend);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_held_pool_when_public_cold_read_and_write_expire_then_no_sql_or_mutation_runs() {
    let fixture = super::test_support::NativeFixture::open().await;
    eprintln!("owned_schema={}", fixture.schema);
    fixture
        .admin
        .batch_execute(&format!(
            "DROP TABLE {}.sys_infrastructure_format",
            fixture.schema
        ))
        .await
        .unwrap();
    let calls = Arc::new(NativeCalls::default());
    let backend = fixture
        .open_independent_backend()
        .await
        .with_fault_probe(calls.clone());
    let pool = backend.pool.clone();
    let store = crate::SqlStore::initialize_schema(
        backend,
        storage_types::InitialStreamLineages::ONE,
        true,
    )
    .await
    .unwrap();
    let name = "deadline_acquisition";
    store
        .create_table(
            serde_json::from_value(serde_json::json!({
                "TableName": name,
                "KeySchema": [{"AttributeName":"pk", "KeyType":"HASH"}],
                "AttributeDefinitions": [{"AttributeName":"pk", "AttributeType":"S"}],
                "BillingMode":"PAY_PER_REQUEST"
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    let held = pool.get().await.unwrap();
    calls.0.store(0, Ordering::Relaxed);
    tokio::time::pause();
    let read = store
        .get_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Key":{"pk":{"S":"item"}}
            }))
            .unwrap(),
        )
        .await;
    assert!(
        read.unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    let write = store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item":{"pk":{"S":"item"}}
            }))
            .unwrap(),
        )
        .await;
    assert!(
        write
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert_eq!(
        calls.0.load(Ordering::Relaxed),
        0,
        "no BEGIN, customer SQL, or callback native work"
    );
    tokio::time::resume();
    drop(held);
    let read = store
        .get_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Key":{"pk":{"S":"item"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(read.item.is_none());
    let cold_store = store
        .clone()
        .with_metadata_cache_ttl(std::time::Duration::from_secs(5));
    calls.1.store(true, Ordering::Relaxed);
    let write = cold_store
        .put_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Item":{"pk":{"S":"after-cold-preparation"}}
            }))
            .unwrap(),
        )
        .await;
    assert!(
        write
            .unwrap_err()
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
    assert!(
        !calls.1.load(Ordering::Relaxed),
        "cold preparation completed before expiry"
    );
    let read = store
        .get_item(
            serde_json::from_value(serde_json::json!({
                "TableName": name, "Key":{"pk":{"S":"after-cold-preparation"}}
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert!(
        read.item.is_none(),
        "expired parent cannot mutate after successful cold preparation"
    );
    drop(cold_store);
    drop(store);
    drop(pool);
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_pool_ready_after_expiry_when_acquisition_is_repolled_then_callback_stays_uninvoked()
{
    let fixture = super::test_support::NativeFixture::open().await;
    eprintln!("owned_schema={}", fixture.schema);
    let held = fixture.backend.pool.get().await.unwrap();
    let called = Arc::new(AtomicBool::new(false));
    let callback_called = Arc::clone(&called);
    tokio::time::pause();
    let deadline = fixture.backend.deadlines.start(DeadlineClass::Write);
    let attempt = super::driver::PostgresBackend::run_attempt(
        fixture.backend.pool.clone(),
        std::sync::Arc::new(storage_common::native_attempt_lifecycle::AttemptLifecycle::new()),
        (super::driver::PostgresTransaction::Snapshot, None),
        storage_types::AttemptDurability::Durable,
        (deadline, Arc::new(storage_types::SystemClock)),
        None,
        move |_| {
            Box::pin(async move {
                callback_called.store(true, Ordering::Relaxed);
                Ok(())
            })
        },
    );
    let mut attempt = std::pin::pin!(attempt);
    assert!(futures::poll!(attempt.as_mut()).is_pending());
    // Past the whole Write budget (10 s by default).
    tokio::time::advance(std::time::Duration::from_secs(11)).await;
    drop(held);
    let result = attempt.await;
    tokio::time::resume();
    fixture.close().await;
    assert!(
        !called.load(Ordering::Relaxed),
        "ready pool acquisition cannot outrun elapsed timer"
    );
    assert!(
        result
            .expect_err("expired acquisition")
            .attempt_kind()
            .is_some_and(storage_types::AttemptKind::is_unavailable)
    );
}
