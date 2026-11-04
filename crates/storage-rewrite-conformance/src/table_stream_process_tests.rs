//! Real process boundaries retain catalog authority across stream transitions.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::json;
use storage_types::{GetItemRequest, PutItemRequest, TableName};

use super::{
    table_stream_eligibility_tests::{finish_initialization, set_stream},
    test_support::{TestStore, hash_table_request, unique_table_name},
};

struct PreparedWriteBarrier {
    root: PathBuf,
    phase: AtomicU8,
}

impl kv_backend::KvFaultProbe for PreparedWriteBarrier {
    fn fail(&self, call: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        if call.operation != kv_backend::KvFaultOperation::GetBatch {
            return None;
        }
        let phase = self.phase.swap(0, Ordering::SeqCst);
        if phase == 0 {
            return None;
        }
        let name = if phase == 1 { "enable" } else { "disable" };
        std::fs::write(self.root.join(format!("{name}-prepared")), []).unwrap();
        let started = Instant::now();
        while !self.root.join(name).is_file() {
            assert!(
                started.elapsed() < Duration::from_secs(120),
                "prepared write barrier timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        None
    }
}

async fn await_signal(root: &Path, name: &str) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(120), async {
        while !root.join(name).is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

async fn put_phase(store: &TestStore, table: &TableName, value: &str) -> anyhow::Result<()> {
    store
        .operations
        .put_item(
            PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "racer"}, "value": {"S": value}}
            }))
            .map_err(anyhow::Error::msg)?,
        )
        .await?;
    Ok(())
}

#[tokio::test]
#[ignore = "child process for the FoundationDB transition test"]
async fn given_process_scope_when_parent_changes_catalog_then_child_refreshes_stale_preparation()
-> anyhow::Result<()> {
    let cluster = std::env::var("STREAM_TEST_CLUSTER")?;
    let prefix = std::env::var("STREAM_TEST_PREFIX")?;
    let table = TableName::new(&std::env::var("STREAM_TEST_TABLE")?);
    let signal = std::path::PathBuf::from(std::env::var("STREAM_TEST_SIGNAL")?);
    let barrier = Arc::new(PreparedWriteBarrier {
        root: signal.clone(),
        phase: AtomicU8::new(0),
    });
    let store = TestStore::open_foundationdb_process_scope(cluster, prefix, barrier.clone()).await;
    put_phase(&store, &table, "disabled-before").await?;
    barrier.phase.store(1, Ordering::SeqCst);
    put_phase(&store, &table, "enabled").await?;
    std::fs::write(signal.join("enabled-done"), [])?;
    barrier.phase.store(2, Ordering::SeqCst);
    put_phase(&store, &table, "disabled-after").await?;
    // The parent owns namespace cleanup after this process has exited.
    drop(store);
    Ok(())
}

#[tokio::test]
#[ignore = "requires the owned FoundationDB fixture and two real processes"]
async fn given_two_processes_with_cached_catalog_when_stream_changes_then_each_write_uses_committing_configuration()
-> anyhow::Result<()> {
    for internal_gsi in [false, true] {
        qualify_transition(internal_gsi).await?;
    }
    Ok(())
}

async fn qualify_transition(internal_gsi: bool) -> anyhow::Result<()> {
    let store = TestStore::new().await;
    let table = unique_table_name();
    store
        .operations
        .create_table(hash_table_request(table.clone()))
        .await?;
    let (cluster, prefix) = store.foundationdb_process_scope();
    let signals = tempfile::tempdir()?;
    let child = tokio::process::Command::new(std::env::current_exe()?)
        .args(["--ignored", "--exact", "table_stream_process_tests::given_process_scope_when_parent_changes_catalog_then_child_refreshes_stale_preparation", "--nocapture"])
        .env("STREAM_TEST_CLUSTER", cluster).env("STREAM_TEST_PREFIX", prefix)
        .env("STREAM_TEST_TABLE", table.as_ref()).env("STREAM_TEST_SIGNAL", signals.path())
        .stdout(Stdio::piped()).stderr(Stdio::piped()).kill_on_drop(true).spawn()?;
    await_signal(signals.path(), "enable-prepared").await?;
    if internal_gsi {
        store
            .operations
            .update_table(serde_json::from_value(json!({
                "TableName": table, "GlobalSecondaryIndexUpdates": [{"Create": {
                    "IndexName": "by_pk", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"}
                }}]
            }))?)
            .await?;
    } else {
        set_stream(&store, &table, true).await?;
    }
    std::fs::write(signals.path().join("enable"), [])?;
    await_signal(signals.path(), "enabled-done").await?;
    if !internal_gsi {
        finish_initialization(&store, &table).await?;
    }
    await_signal(signals.path(), "disable-prepared").await?;
    if internal_gsi {
        super::gsi_lifecycle_tests::finish_backfill(&store, 1).await?;
        anyhow::ensure!(
            !store
                .operations
                .get_table_info(table.clone())
                .await?
                .requires_stream_entry(),
            "internal GSI completion did not stop capture"
        );
    } else {
        set_stream(&store, &table, false).await?;
    }
    std::fs::write(signals.path().join("disable"), [])?;
    let result = tokio::time::timeout(Duration::from_secs(120), child.wait_with_output()).await??;
    anyhow::ensure!(
        result.status.success(),
        "child failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    let records = super::cdc_stream_tests::system_records(&store, 100).await?;
    anyhow::ensure!(
        records.len() == 1,
        "configuration transition emitted the wrong history count"
    );
    let current = store
        .operations
        .get_item(
            GetItemRequest::try_from(json!({"TableName": table, "Key": {"pk": {"S": "racer"}}}))
                .map_err(anyhow::Error::msg)?,
        )
        .await?
        .item
        .unwrap();
    anyhow::ensure!(current.required_string_attribute("value")? == "disabled-after");
    store.finish().await;
    Ok(())
}
