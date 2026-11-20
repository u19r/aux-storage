//! Customer-visible KV faults through the real replacement provider stack.

use std::sync::{Arc, Mutex};

use kv_backend::{KvFaultCall, KvFaultOperation, KvFaultProbe};
use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::{GetItemRequest, PutItemRequest, StorageError};
use tracing::Level;

use super::test_support::{TestStore, assert_dynamo_error_response_exact};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaultEffect {
    Error,
    WrongCount,
    MissingMetadata,
    CorruptMetadata,
    StageError,
}

#[derive(Default)]
struct GsiPageAbortProbe(Mutex<(bool, Vec<Vec<u8>>, Vec<usize>)>);

#[derive(Default)]
struct RemovedGsiProbe(Mutex<(usize, usize, Vec<Vec<u8>>)>);

impl KvFaultProbe for RemovedGsiProbe {
    fn fail(&self, _: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        let mut state = self.0.lock().unwrap();
        match event {
            kv_backend::KvNativeEvent::Begin => state.0 += 1,
            kv_backend::KvNativeEvent::Abort => state.1 += 1,
            _ => {}
        }
    }

    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        if matches!(
            access.kind,
            kv_backend::KvAccessKind::Write
                | kv_backend::KvAccessKind::Clear
                | kv_backend::KvAccessKind::ClearRange
        ) && kv_backend::key_family(access.key) == "gsi"
        {
            self.0.lock().unwrap().2.push(access.key.to_vec());
        }
    }
}

#[tokio::test]
#[cfg_attr(feature = "foundationdb", ignore)]
async fn given_gsi_removed_then_stale_writer_retries_and_writes_no_entry() -> anyhow::Result<()> {
    use super::gsi_workflow_tests::{group_keys, put_group, table_request};
    let probe = Arc::new(RemovedGsiProbe::default());
    let fixture = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = async {
        fixture.operations.create_table(table_request("gsi_stale_removal")).await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 8).await?;
        // Two store instances share one supported native backend but own
        // independent metadata caches. A remains warm while B removes the GSI.
        let writer = fixture.independent_operations.as_ref().unwrap();
        let put = |value: &str| PutItemRequest::try_from(json!({
            "TableName": "gsi_stale_removal", "Item": {
                "pk": {"S": "writer"}, "gsi_pk": {"S": "x"}, "gsi_sk": {"S": "writer"}, "value": {"S": value}
            }
        })).map_err(anyhow::Error::msg);
        writer.put_item(put("warm")?).await?;
        let retired = probe.0.lock().unwrap().2.last().unwrap()[..8].to_vec();
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": "gsi_stale_removal", "GlobalSecondaryIndexUpdates": [{"Delete": {"IndexName": "by_group"}}]
        }))?).await?;
        for _ in 0..8 { fixture.maintenance.reclaim_table_page().await?; }
        *probe.0.lock().unwrap() = Default::default();
        writer.put_item(put("fresh")?).await?;
        {
            let state = probe.0.lock().unwrap();
            anyhow::ensure!(state.0 >= 2 && state.1 == 1, "stale cached binding did not abort once and retry: begins={}, aborts={}", state.0, state.1);
            anyhow::ensure!(state.2.is_empty(), "stale writer staged a retired GSI entry");
        }
        let item = fixture.operations.get_item(GetItemRequest::try_from(json!({
            "TableName": "gsi_stale_removal", "Key": {"pk": {"S": "writer"}}, "ConsistentRead": true
        })).map_err(anyhow::Error::msg)?).await?;
        anyhow::ensure!(item.item.as_ref().unwrap().attribute_value("value")? == Some(storage_types::AttributeValue::S("fresh".to_owned())), "fresh retry did not update primary item");
        let missing = fixture.operations.query(storage_types::QueryRequest::try_from(json!({
            "TableName": "gsi_stale_removal", "IndexName": "by_group", "KeyConditionExpression": "gsi_pk = :g",
            "ExpressionAttributeValues": {":g": {"S": "x"}}
        })).map_err(anyhow::Error::msg)?).await;
        anyhow::ensure!(missing.is_err(), "removed GSI is still publicly available");
        // Advance the fixture clock past the public index-name reservation.
        fixture.advance_time(std::time::Duration::from_secs(30));
        super::gsi_lifecycle_tests::create_index(&fixture, "gsi_stale_removal").await?;
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 8).await?;
        put_group(&fixture, "gsi_stale_removal", "replacement", "x").await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_stale_removal", "x").await? == ["replacement", "writer"]);
        anyhow::ensure!(probe.0.lock().unwrap().2.iter().all(|key| !key.starts_with(&retired)), "recreated GSI reused retired entry address");
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

#[cfg(feature = "foundationdb")]
#[derive(Default)]
struct GsiPageHoldProbe {
    commits: std::sync::atomic::AtomicUsize,
    reached: tokio::sync::Notify,
    /// The next page commit waits for this; a dropped controller fails it.
    release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

#[cfg(feature = "foundationdb")]
impl GsiPageHoldProbe {
    /// Holds the next commit until the returned controller sends or drops.
    fn arm(&self) -> tokio::sync::oneshot::Sender<()> {
        let (controller, release) = tokio::sync::oneshot::channel();
        *self.release.lock().unwrap() = Some(release);
        controller
    }
}

#[cfg(feature = "foundationdb")]
impl KvFaultProbe for GsiPageHoldProbe {
    fn fail(&self, _: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        use std::sync::atomic::Ordering::SeqCst;
        Box::pin(async move {
            if boundary == kv_backend::KvCommitBoundary::BeforeCommit {
                self.commits.fetch_add(1, SeqCst);
                let release = self.release.lock().unwrap().take();
                if let Some(release) = release {
                    self.reached.notify_one();
                    release.await.map_err(|_| {
                        StorageError::internal("GSI page hold controller was dropped")
                    })?;
                }
            }
            Ok(())
        })
    }
}

#[cfg(feature = "foundationdb")]
#[tokio::test]
#[ignore]
async fn given_absent_key_in_gsi_source_range_when_inserted_before_page_commit_then_page_retries()
-> anyhow::Result<()> {
    use std::sync::atomic::Ordering::SeqCst;

    use super::gsi_workflow_tests::{build_request, put_group, table_request};
    // RocksDB serializes write attempts on one writer, so no insert can run
    // while the page commit is held; the phantom race exists only on
    // FoundationDB.
    if TestStore::selected_backend() != "foundationdb" {
        return Ok(());
    }
    let probe = Arc::new(GsiPageHoldProbe::default());
    let fixture = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = async {
        let mut create = table_request("gsi_absent_range");
        let indexes = create.global_secondary_indexes.take().unwrap();
        fixture.operations.create_table(create).await?;
        for ordinal in 0..12 { put_group(&fixture, "gsi_absent_range", &format!("k{:02}", ordinal * 2), "x").await?; }
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": "gsi_absent_range", "GlobalSecondaryIndexUpdates": indexes.into_iter().map(|index| json!({"Create": index})).collect::<Vec<_>>()
        }))?).await?;
        probe.commits.store(0, SeqCst);
        let controller = probe.arm();
        let repository = Arc::clone(&fixture.maintenance);
        let mut request = build_request(1);
        request.page_limit = storage_backfill::GsiPageLimit::try_from(8usize)?;
        let page = tokio::spawn(async move { repository.advance_gsi_build(request).await });
        probe.reached.notified().await;
        fixture.operations.put_item(PutItemRequest::try_from(json!({
            "TableName": "gsi_absent_range", "Item": {"pk": {"S": "k01"}}
        })).map_err(anyhow::Error::msg)?).await?;
        controller.send(()).map_err(|()| anyhow::anyhow!("held page commit was abandoned"))?;
        page.await??;
        anyhow::ensure!(probe.commits.load(SeqCst) >= 3, "source phantom did not abort the page (one writer plus two page attempts required)");
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 8).await?;
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

impl KvFaultProbe for GsiPageAbortProbe {
    fn fail(&self, _: KvFaultCall) -> Option<StorageError> {
        None
    }

    fn observe_access(&self, access: kv_backend::KvAccess<'_>) {
        if access.kind == kv_backend::KvAccessKind::ReadRange
            && kv_backend::key_family(access.key) == "item"
        {
            self.0.lock().unwrap().1.push(access.key.to_vec());
        }
    }

    fn observe(&self, event: kv_backend::KvNativeEvent) {
        if let kv_backend::KvNativeEvent::RangesRead { entries, .. } = event {
            self.0.lock().unwrap().2.push(entries);
        }
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        let mut state = self.0.lock().unwrap();
        let reject = boundary == kv_backend::KvCommitBoundary::BeforeCommit && state.0;
        if reject {
            state.0 = false;
        }
        Box::pin(async move {
            if reject {
                Err(StorageError::internal("reject GSI page before submission"))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
#[cfg_attr(feature = "foundationdb", ignore)]
async fn given_gsi_page_when_commit_aborts_then_retry_halves_limit_and_preserves_cursor()
-> anyhow::Result<()> {
    use super::gsi_workflow_tests::{build_request, group_keys, put_group, table_request};
    let probe = Arc::new(GsiPageAbortProbe::default());
    let fixture = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let outcome = async {
        let mut create = table_request("gsi_page_abort");
        let indexes = create.global_secondary_indexes.take().unwrap();
        fixture.operations.create_table(create).await?;
        for ordinal in 0..12 { put_group(&fixture, "gsi_page_abort", &format!("k{ordinal:02}"), "x").await?; }
        fixture.operations.update_table(serde_json::from_value(json!({
            "TableName": "gsi_page_abort", "GlobalSecondaryIndexUpdates": indexes.into_iter().map(|index| json!({"Create": index})).collect::<Vec<_>>()
        }))?).await?;
        let mut request = build_request(1);
        request.page_limit = storage_backfill::GsiPageLimit::try_from(8usize)?;
        *probe.0.lock().unwrap() = (true, Vec::new(), Vec::new());
        fixture.maintenance.advance_gsi_build(request).await?;
        {
            let state = probe.0.lock().unwrap();
            anyhow::ensure!(!state.0, "the page did not reach its commit");
            anyhow::ensure!(state.1.len() == 2 && state.1[0] == state.1[1], "retry advanced the source cursor");
            let pages = state.2.iter().copied().filter(|entries| *entries >= 4).collect::<Vec<_>>();
            anyhow::ensure!(pages == [8, 4], "retry did not halve the source page: {:?}", state.2);
        }
        super::gsi_lifecycle_tests::finish_backfill(&fixture, 8).await?;
        anyhow::ensure!(group_keys(&fixture, "gsi_page_abort", "x").await?.len() == 12);
        Ok(())
    }.await;
    fixture.finish().await;
    outcome
}

#[derive(Default)]
struct ReadFaultProbe(Mutex<(Option<FaultEffect>, usize, usize)>);

impl ReadFaultProbe {
    fn arm(&self, effect: FaultEffect) {
        *self.0.lock().unwrap() = (Some(effect), 0, 0);
    }

    fn fired_once(&self) {
        assert_eq!(self.0.lock().unwrap().0, None);
        assert_eq!(self.0.lock().unwrap().1, 1);
    }
}

impl KvFaultProbe for ReadFaultProbe {
    fn fail(&self, call: KvFaultCall) -> Option<StorageError> {
        if call.operation != KvFaultOperation::GetBatch && call.operation != KvFaultOperation::Stage
        {
            return None;
        }
        let mut state = self.0.lock().unwrap();
        let triggered = matches!(
            (state.0, call.operation),
            (Some(FaultEffect::Error), KvFaultOperation::GetBatch)
                | (Some(FaultEffect::StageError), KvFaultOperation::Stage)
        );
        if !triggered {
            return None;
        }
        state.0 = None;
        state.1 += 1;
        Some(StorageError::internal(
            if call.operation == KvFaultOperation::Stage {
                "synthetic native KV write failure"
            } else {
                "synthetic native KV read failure"
            },
        ))
    }

    fn alter_get_batch(
        &self,
        call: KvFaultCall,
        values: Vec<Option<Vec<u8>>>,
    ) -> Vec<Option<Vec<u8>>> {
        if call.operation != KvFaultOperation::GetBatch {
            return values;
        }
        let mut state = self.0.lock().unwrap();
        state.2 += 1;
        let triggered = match state.0 {
            Some(FaultEffect::WrongCount) if state.2 == 1 => Some(Vec::new()),
            Some(FaultEffect::MissingMetadata) if state.2 == 2 => Some(vec![None]),
            Some(FaultEffect::CorruptMetadata) if state.2 == 2 => {
                Some(vec![Some(b"corrupt catalog metadata\0".to_vec())])
            }
            _ => None,
        };
        if let Some(replacement) = triggered {
            state.0 = None;
            state.1 += 1;
            replacement
        } else {
            values
        }
    }
}

#[tokio::test]
async fn given_standard_table_when_native_kv_read_fails_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(FaultEffect::Error, "synthetic native KV read failure").await;
}

#[tokio::test]
async fn given_standard_table_when_native_kv_read_has_wrong_count_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(
        FaultEffect::WrongCount,
        "native KV batch returned the wrong count",
    )
    .await;
}

#[tokio::test]
async fn given_standard_table_when_kv_metadata_is_missing_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(
        FaultEffect::MissingMetadata,
        "KV name lookup points to missing metadata",
    )
    .await;
}

#[tokio::test]
async fn given_standard_table_when_kv_metadata_is_corrupt_then_customer_error_and_warning_name_the_table()
 {
    assert_kv_fault(FaultEffect::CorruptMetadata, "KV table metadata is corrupt").await;
}

#[tokio::test]
async fn given_standard_table_when_native_kv_stage_fails_then_customer_error_is_logged_and_write_rolls_back()
 {
    let probe = Arc::new(ReadFaultProbe::default());
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let table = store.create_standard_table().await;
    probe.arm(FaultEffect::StageError);
    let request = PutItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Item": {"pk": {"S": "private-customer-key"}, "v": {"S": "private-value"}}
    }))
    .unwrap();
    let observed = observe(store.operations.put_item(request)).await;
    let warning =
        observed.only_warning_or_error_in_span("dynamo_operation", &[("operation", "PutItem")]);
    assert_eq!(warning.level, Level::WARN);
    assert_eq!(warning.fields.get("operation").unwrap(), "kv.stage");
    assert_eq!(
        warning.fields.get("error_code").unwrap(),
        "InternalServerError"
    );
    assert_eq!(
        warning.fields.get("diagnostic").unwrap(),
        "synthetic native KV write failure"
    );
    assert!(!format!("{:?}", observed.events).contains("private-customer-key"));
    assert!(!format!("{:?}", observed.events).contains("private-value"));
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
    );
    probe.fired_once();
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let readback = observe(store.operations.get_item(request)).await;
    readback.assert_no_warning_or_error();
    assert_eq!(
        serde_json::to_value(readback.result.unwrap()).unwrap(),
        json!({})
    );
    store.finish().await;
}

async fn assert_kv_fault(effect: FaultEffect, diagnostic: &str) {
    let probe = Arc::new(ReadFaultProbe::default());
    let store = TestStore::new_kv_with_fault_probe(probe.clone()).await;
    let table = store.create_standard_table().await;
    probe.arm(effect);
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let observed = observe(store.operations.get_item(request)).await;
    let warning =
        observed.only_warning_or_error_in_span("dynamo_operation", &[("operation", "GetItem")]);
    assert_eq!(warning.level, Level::WARN);
    let backend = match std::env::var("STORAGE_TEST_BACKEND").ok().as_deref() {
        Some("foundationdb") => "foundationdb",
        Some("rocksdb") => "rocksdb",
        _ if cfg!(feature = "rocksdb") => "rocksdb",
        _ => "foundationdb",
    };
    assert_eq!(warning.fields.get("backend").unwrap(), backend);
    let operation = match effect {
        FaultEffect::MissingMetadata => "catalog.read_table",
        FaultEffect::CorruptMetadata => "catalog.bind_metadata",
        _ => "kv.get_batch",
    };
    assert_eq!(warning.fields.get("operation").unwrap(), operation);
    assert_eq!(
        warning.fields.get("error_code").unwrap(),
        "InternalServerError"
    );
    assert_eq!(warning.fields.get("diagnostic").unwrap(), diagnostic);
    assert_eq!(
        warning.fields.get("message").unwrap(),
        "native storage operation failed"
    );
    assert_eq!(
        warning.fields.len(),
        5,
        "no key or item data in the warning"
    );
    assert!(!format!("{:?}", observed.events).contains("private-customer-key"));
    assert_dynamo_error_response_exact(
        observed.result.unwrap_err(),
        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        r#"{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"DynamoDB could not process your request."}"#,
    );
    probe.fired_once();
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let readback = observe(store.operations.get_item(request)).await;
    readback.assert_no_warning_or_error();
    assert_eq!(
        serde_json::to_value(readback.result.unwrap()).unwrap(),
        json!({})
    );
    store.finish().await;
}
