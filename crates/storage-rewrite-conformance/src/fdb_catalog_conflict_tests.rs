//! FoundationDB catalog conflicts are per table: a schema change aborts only
//! the attempts that depend on the changed table's catalog
//! (`docs/plans/incomplete/fable-fdb-catalog-conflict-design.md`).

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use futures::future::BoxFuture;
use serde_json::{Value, json};
use storage_provider::DynamoOperations;
use storage_types::{StorageResult, TableName};

use crate::test_support::{TestStore, hash_table_request, unique_table_name};

/// Counts this node's write attempts and their known commit conflicts, and
/// runs one armed action before the next write commit.
#[derive(Default)]
struct ConflictProbe {
    write_attempts: AtomicUsize,
    conflicts: AtomicUsize,
    before_commit: Mutex<Option<BoxFuture<'static, ()>>>,
}

impl kv_backend::KvFaultProbe for ConflictProbe {
    fn observe(&self, event: kv_backend::KvNativeEvent) {
        match event {
            kv_backend::KvNativeEvent::AttemptRequested { mode, .. }
                if mode != storage_types::AttemptMode::ReadOnly =>
            {
                self.write_attempts.fetch_add(1, Ordering::SeqCst);
            }
            kv_backend::KvNativeEvent::CommitFailed {
                kind: Some(storage_types::AttemptKind::Conflict),
                ..
            } => {
                self.conflicts.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
    }

    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: kv_backend::KvCommitBoundary,
        mode: storage_types::AttemptMode,
    ) -> BoxFuture<'_, StorageResult<()>> {
        // Item writes only: background maintenance writes use other modes.
        let action = (boundary == kv_backend::KvCommitBoundary::BeforeCommit
            && mode == storage_types::AttemptMode::ReadWriteItems)
            .then(|| self.before_commit.lock().unwrap().take())
            .flatten();
        Box::pin(async move {
            if let Some(action) = action {
                action.await;
            }
            Ok(())
        })
    }
}

#[derive(Default)]
struct QuietProbe;

impl kv_backend::KvFaultProbe for QuietProbe {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<storage_types::StorageError> {
        None
    }
}

async fn put(operations: &Arc<dyn DynamoOperations>, table: &TableName, pk: &str) {
    let request = storage_types::PutItemRequest::try_from(
        json!({"TableName": table.as_ref(), "Item": {"pk": {"S": pk}}}),
    )
    .unwrap();
    if let Err(error) = operations.put_item(request).await {
        panic!("put failed: {:?}", error);
    }
}

async fn get(operations: &Arc<dyn DynamoOperations>, table: &TableName, pk: &str) -> Value {
    let request = storage_types::GetItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Key": {"pk": {"S": pk}}, "ConsistentRead": true
    }))
    .unwrap();
    match operations.get_item(request).await {
        Ok(response) => serde_json::to_value(response).unwrap(),
        Err(error) => panic!("get failed: {:?}", error),
    }
}

async fn create(operations: &Arc<dyn DynamoOperations>, table: &TableName) {
    if let Err(error) = operations
        .create_table(hash_table_request(table.clone()))
        .await
    {
        panic!("create failed: {:?}", error);
    }
}

async fn delete(operations: &Arc<dyn DynamoOperations>, table: &TableName) {
    let request =
        storage_types::DeleteTableRequest::try_from(json!({"TableName": table.as_ref()})).unwrap();
    if let Err(error) = operations.delete_table(request).await {
        panic!("delete failed: {:?}", error);
    }
}

/// The writer node and a second node on the same database namespace.
async fn two_nodes(probe: Arc<ConflictProbe>) -> Option<(TestStore, TestStore)> {
    if TestStore::selected_backend() != "foundationdb" {
        return None;
    }
    let writer = TestStore::new_kv_with_fault_probe(probe).await;
    let (cluster, prefix) = writer.foundationdb_process_scope();
    let other =
        TestStore::open_foundationdb_process_scope(cluster, prefix, Arc::new(QuietProbe)).await;
    Some((writer, other))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn given_ddl_on_another_table_when_writing_then_no_write_attempt_conflicts() {
    const WRITES: usize = 60;
    let probe = Arc::new(ConflictProbe::default());
    let Some((writer, other)) = two_nodes(Arc::clone(&probe)).await else {
        return;
    };
    let table = unique_table_name();
    create(&writer.operations, &table).await;
    put(&writer.operations, &table, "warm").await;

    let stop = Arc::new(AtomicBool::new(false));
    let changes = Arc::new(AtomicUsize::new(0));
    let ddl = {
        let (operations, stop, changes) = (
            Arc::clone(&other.operations),
            Arc::clone(&stop),
            Arc::clone(&changes),
        );
        tokio::spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let unrelated = unique_table_name();
                create(&operations, &unrelated).await;
                delete(&operations, &unrelated).await;
                changes.fetch_add(2, Ordering::SeqCst);
            }
        })
    };
    // Let the first catalog change land before measuring.
    while changes.load(Ordering::SeqCst) == 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let before = probe.write_attempts.load(Ordering::SeqCst);
    let conflicts_before = probe.conflicts.load(Ordering::SeqCst);
    let changes_before = changes.load(Ordering::SeqCst);
    for index in 0..WRITES {
        put(&writer.operations, &table, &format!("item-{index}")).await;
    }
    let attempts = probe.write_attempts.load(Ordering::SeqCst) - before;
    let conflicts = probe.conflicts.load(Ordering::SeqCst) - conflicts_before;
    let overlapping = changes.load(Ordering::SeqCst) - changes_before;
    stop.store(true, Ordering::SeqCst);
    ddl.await.unwrap();
    eprintln!(
        "catalog conflict census: puts={WRITES} write_attempts={attempts} conflicts={conflicts} \
         unrelated_catalog_changes={overlapping}"
    );

    assert!(
        overlapping > 0,
        "no unrelated catalog change overlapped the writes"
    );
    assert_eq!(
        conflicts, 0,
        "{attempts} write attempts for {WRITES} puts during {overlapping} unrelated catalog \
         changes"
    );
    assert_eq!(attempts, WRITES, "one write attempt per put");
    drop(other);
    writer.finish().await;
}

#[tokio::test]
async fn given_a_warm_binding_when_its_table_is_recreated_before_commit_then_the_write_never_lands_in_the_old_incarnation()
 {
    let probe = Arc::new(ConflictProbe::default());
    let Some((writer, other)) = two_nodes(Arc::clone(&probe)).await else {
        return;
    };
    let table = unique_table_name();
    create(&writer.operations, &table).await;
    // Warm the writer node's binding for this incarnation.
    put(&writer.operations, &table, "warm").await;
    let conflicts_before = probe.conflicts.load(Ordering::SeqCst);

    // The next write reads with the warm binding, then another node replaces
    // the table before that write commits.
    let (operations, name) = (Arc::clone(&other.operations), table.clone());
    *probe.before_commit.lock().unwrap() = Some(Box::pin(async move {
        delete(&operations, &name).await;
        create(&operations, &name).await;
    }));
    let request = storage_types::PutItemRequest::try_from(
        json!({"TableName": table.as_ref(), "Item": {"pk": {"S": "racing"}}}),
    )
    .unwrap();
    let result = writer.operations.put_item(request).await;

    assert!(probe.before_commit.lock().unwrap().is_none());
    assert!(
        probe.conflicts.load(Ordering::SeqCst) > conflicts_before,
        "the replaced stamp must abort the warm write"
    );
    let racing = get(&other.operations, &table, "racing").await;
    match result {
        // The retry may bind the new incarnation...
        Ok(_) => assert_eq!(
            racing["Item"]["pk"],
            json!({"S": "racing"}),
            "an acknowledged write must be in the current incarnation"
        ),
        // ...or keep the request's pinned incarnation, which is gone.
        Err(error) => {
            assert!(
                error.to_string().contains("Requested resource not found"),
                "unexpected put failure: {error}"
            );
            assert!(
                racing.get("Item").is_none(),
                "a refused write must not land"
            );
        }
    }
    let warm = get(&other.operations, &table, "warm").await;
    assert!(
        warm.get("Item").is_none(),
        "the new incarnation starts empty"
    );
    drop(other);
    writer.finish().await;
}
