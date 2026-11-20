//! R12 regression: on Postgres the stream position comes from the
//! transaction ID. Before W6F step N1 a write attempt got its ID from the
//! `FOR SHARE` stamp read, before it took the item lock, so a writer that got
//! its ID first but locked the item second was ordered before the writer that
//! changed the item first. Now the stamp read follows a shared advisory
//! catalog lock and takes no row lock.

use std::{sync::Arc, time::Duration};

use serde_json::{Value, json};
use storage_types::{GetRecordsRequest, PutItemRequest};

use super::{
    dynamodb_stream_history_tests::{
        create_stream_table, find_populated_shard, iterator, put_value,
    },
    native_census_test_support::NativeCensus,
    test_support::TestStore,
};

fn image(image: Option<&storage_types::AttributeMap>) -> Value {
    serde_json::to_value(image).expect("stream image serializes")["value"]["S"].clone()
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_writer_waiting_before_item_lock_when_other_writer_commits_then_stream_follows_write_order()
 {
    assert_eq!(
        std::env::var("STORAGE_TEST_BACKEND").as_deref(),
        Ok("postgres")
    );
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, arn) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "hot", "seed").await;

    // X stops before the wave that takes the item lock (`hashtextextended`);
    // its catalog lock and stamp read have already run.
    let held = census.hold(|wave| wave.sql_contains("hashtextextended"));
    let operations = Arc::clone(&store.operations);
    let put_x = PutItemRequest::try_from(json!({
        "TableName": table.as_ref(), "Item": {"pk": {"S": "hot"}, "value": {"S": "x"}}
    }))
    .unwrap();
    let writer_x = tokio::spawn(async move { operations.put_item(put_x).await });
    held.reached().await;

    // Y does not wait for X: shared catalog locks do not conflict.
    tokio::time::timeout(
        Duration::from_secs(20),
        put_value(&store, &table, "hot", "y"),
    )
    .await
    .expect("Y must not wait for the held writer");
    held.release();
    tokio::time::timeout(Duration::from_secs(20), writer_x)
        .await
        .expect("X completes after release")
        .expect("writer task")
        .expect("put X");

    store.wait_until_changes_are_readable().await;
    let shard = find_populated_shard(&store, &arn).await;
    let records = store
        .operations
        .get_records(GetRecordsRequest {
            shard_iterator: iterator(&store, &arn, &shard, "TRIM_HORIZON", None).await,
            limit: None,
        })
        .await
        .expect("read stream")
        .records;
    let order = records
        .iter()
        .map(|record| {
            (
                image(record.dynamodb.new_image.as_ref()),
                image(record.dynamodb.old_image.as_ref()),
            )
        })
        .collect::<Vec<_>>();
    store.finish().await;
    assert_eq!(
        order[order.len().saturating_sub(2)..],
        [(json!("y"), json!("seed")), (json!("x"), json!("y")),],
        "stream order must follow the item lock order (new, old images): {order:?}"
    );
}

/// Synthetic import exercises a restored source clock ahead of this server. It
/// preserves canonical bodies; it does not establish pg_dump/restore fidelity.
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_imported_higher_positions_when_reopened_then_native_writes_follow_unchanged_history()
{
    use storage_types::{BatchWriteItemRequest, UserStreamName};
    use stream_provider::{StreamPartitioningMode, SystemStreamPageRequest};

    let store = TestStore::new_postgres().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    for key in ["old-a", "old-b"] {
        put_value(&store, &table, key, "original").await;
    }
    let user = UserStreamName::new("imported-native-user");
    let stream = store
        .stream_operations
        .create_stream(user.clone(), None, StreamPartitioningMode::Single)
        .await
        .unwrap();
    store
        .stream_operations
        .append_item(user.clone(), b"old-user", None)
        .await
        .unwrap();
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let native: i64 = client
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .await
        .unwrap()
        .get(0);
    let imported_transaction = native + 1_000_000;
    let system_high = imported_transaction * 65_536 + 65_535;
    // Both address representations use the same imported native position.
    client
        .execute(
            "WITH positions AS (SELECT ctid, $1::bigint - row_number() OVER (ORDER BY \
             offset_order DESC) + 1 AS position FROM sys_stream_entries) UPDATE \
             sys_stream_entries AS entry SET offset_order = int8send(positions.position), \
             offset_text = lpad(positions.position::text, 20, '0') || '-ns' FROM positions WHERE \
             entry.ctid = positions.ctid",
            &[&system_high],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE sys_dynamo_stream_items SET item_id = decode('58494431', 'hex') || \
             int8send($1::bigint)",
            &[&imported_transaction],
        )
        .await
        .unwrap();
    let old_history = snapshot_imported_history(&client).await;
    let old_users = snapshot_imported_users(&client).await;
    let store = store.reopen().await;
    let offset: i64 = client
        .query_one(
            "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        offset > 0,
        "opening must raise the stored transaction offset"
    );
    assert_eq!(snapshot_imported_history(&client).await, old_history);
    assert_eq!(snapshot_imported_users(&client).await, old_users);

    let new_user = store
        .stream_operations
        .append_item(user, b"new-user", None)
        .await
        .unwrap();
    let mut imported_id = b"XID1".to_vec();
    imported_id.extend_from_slice(&imported_transaction.to_be_bytes());
    assert!(new_user.as_bytes().as_slice() > imported_id.as_slice());

    let batches = ["writer-a", "writer-b"].map(|writer| {
        let operations = Arc::clone(&store.operations);
        let request = BatchWriteItemRequest::try_from(json!({"RequestItems": {table.as_ref(): [
            {"PutRequest": {"Item": {"pk": {"S": format!("{writer}-1")}, "value": {"S": writer}}}},
            {"PutRequest": {"Item": {"pk": {"S": format!("{writer}-2")}, "value": {"S": writer}}}}
        ]}}))
        .unwrap();
        tokio::spawn(async move { operations.batch_write_item(request).await.unwrap() })
    });
    for batch in batches {
        batch.await.unwrap();
    }
    let rows = client
        .query(
            "SELECT split_part(offset_text, '-', 1)::bigint FROM sys_stream_entries ORDER BY \
             offset_order",
            &[],
        )
        .await
        .unwrap();
    let positions = rows
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 6);
    assert!(positions.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        positions[2..]
            .iter()
            .all(|position| *position > system_high)
    );
    assert_eq!(
        positions[2..]
            .iter()
            .map(|position| position / 65_536)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        2,
        "two actual multi-entry native commits"
    );
    // Reads alone wait for unrelated older transactions; do not consume another
    // native transaction ID merely to make a completed writer visible.
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let records = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let page = store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), None, 100).unwrap(),
                )
                .await
                .unwrap();
            if page.records().len() == 6 {
                break page.records().to_vec();
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("all committed history becomes visible without an extra writer");
    let mut checkpoint = None;
    let mut paged_records = Vec::new();
    let lineages = store
        .stream_operations
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap();
    for _ in 0..records.len() + lineages.len() + 1 {
        let page = store
            .stream_operations
            .get_system_stream_page(
                SystemStreamPageRequest::try_new(scope.clone(), checkpoint.clone(), 1).unwrap(),
            )
            .await
            .unwrap();
        let next = page.issued_checkpoint().token().clone();
        if !page.terminal() {
            assert_ne!(checkpoint.as_ref(), Some(&next));
        }
        paged_records.extend(page.records().iter().cloned());
        checkpoint = Some(next);
        if paged_records.len() == records.len() {
            break;
        }
    }
    let paged_keys = paged_records
        .iter()
        .map(|record| &record.keys)
        .collect::<Vec<_>>();
    assert_eq!(paged_keys.len(), records.len());
    assert!(records.iter().all(|record| {
        paged_keys
            .iter()
            .filter(|key| **key == &record.keys)
            .count()
            == 1
    }));
    // SYSTEM pages preserve each lineage's order; independent lineages have
    // no global delivery order even though every new numeric address exceeds H.
    let mut previous = std::collections::BTreeMap::new();
    for record in &paged_records {
        let position = record.system_position.as_ref().unwrap();
        let number = position
            .offset()
            .as_str()
            .split('-')
            .next()
            .unwrap()
            .parse::<i64>()
            .unwrap();
        if let Some(old) = previous.insert(position.shard_id().as_str().to_owned(), number) {
            assert!(number > old);
        }
        let key = record.keys.get("pk").unwrap().inner_str().unwrap();
        assert_eq!(number <= system_high, key.starts_with("old-"));
    }
    let page = store
        .stream_operations
        .read_forward(stream.into(), None, 10)
        .await
        .unwrap();
    assert_eq!(
        page.items
            .iter()
            .map(|item| item.data.as_slice())
            .collect::<Vec<_>>(),
        [b"old-user".as_slice(), b"new-user".as_slice()]
    );
    let preserved = snapshot_imported_history(&client).await;
    assert!(old_history.iter().all(|old| preserved.contains(old)));
    let preserved_users = snapshot_imported_users(&client).await;
    assert!(old_users.iter().all(|old| preserved_users.contains(old)));
    let store = store.reopen().await;
    let reopened_offset: i64 = client
        .query_one(
            "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(
        offset, reopened_offset,
        "ordinary reopen preserves an existing offset"
    );
    store.finish().await;
}

async fn snapshot_imported_history(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query(
            "SELECT row_to_json(entry)::text FROM sys_stream_entries AS entry ORDER BY \
             offset_order",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

async fn snapshot_imported_users(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query(
            "SELECT row_to_json(item)::text FROM sys_dynamo_stream_items AS item ORDER BY item_id",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|row| row.get(0))
        .collect()
}

/// Suppress only the acknowledgement that publishes the imported numbering.
/// Schema DDL commits must finish normally before this particular write.
#[derive(Default)]
struct LostPositionReply {
    armed: std::sync::atomic::AtomicBool,
    updates: std::sync::atomic::AtomicUsize,
    replies_lost: std::sync::atomic::AtomicUsize,
}

impl sql_backend::SqlFaultProbe for LostPositionReply {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<storage_types::StorageError> {
        use std::sync::atomic::Ordering::SeqCst;
        if call
            .sql
            .starts_with("UPDATE sys_system_stream_scope SET transaction_offset")
        {
            self.updates.fetch_add(1, SeqCst);
            self.armed.store(true, SeqCst);
        }
        None
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, storage_types::StorageResult<()>> {
        use std::sync::atomic::Ordering::SeqCst;
        let lose_reply = boundary == sql_backend::SqlCommitBoundary::AfterDurableAcknowledgement
            && self.armed.swap(false, SeqCst);
        Box::pin(async move {
            if lose_reply {
                self.replies_lost.fetch_add(1, SeqCst);
                Err(storage_types::StorageError::internal(
                    "injected lost position publication reply",
                ))
            } else {
                Ok(())
            }
        })
    }
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_imported_sealed_bound_when_open_reply_is_lost_then_reopen_preserves_numbering_and_history()
 {
    use std::sync::atomic::Ordering::SeqCst;

    use stream_provider::{
        SystemStreamLineage, SystemStreamSplitChild, SystemStreamSplitOperationId,
        SystemStreamSplitReceipt, SystemStreamSplitRequest,
    };

    let store = TestStore::new_postgres().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "before", "original").await;
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let parent = store
        .stream_operations
        .system_stream_lineage_catalog(scope.clone())
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    let children = ["import-left", "import-right"].map(|name| {
        SystemStreamSplitChild::try_new(
            SystemStreamLineage::try_new(
                name,
                name,
                Some(parent.lineage_id().as_str().to_owned()),
                false,
            )
            .unwrap(),
            1,
        )
        .unwrap()
    });
    let request = SystemStreamSplitRequest::try_new(
        scope,
        parent,
        children,
        SystemStreamSplitOperationId::try_new("import-sealed-parent").unwrap(),
    )
    .unwrap();
    store
        .stream_operations
        .split_system_stream(request.clone())
        .await
        .unwrap();
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let native: i64 = client
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .await
        .unwrap()
        .get(0);
    let sealed = (native + 1_000_000) * 65_536 + 65_535;
    // Import a coherent parent and receipt whose end exceeds the surviving
    // tail.
    let receipt =
        SystemStreamSplitReceipt::try_new(&request, u64::try_from(sealed).unwrap()).unwrap();
    let bytes = storage_types::storage_serde::to_bytes(&receipt).unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute(
            "UPDATE sys_system_stream_lineage SET sealed_after_offset = $1 WHERE lineage_id = $2",
            &[&sealed, &request.parent().lineage_id().as_str()],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE sys_system_stream_splits SET receipt_blob = $1 WHERE operation_id = $2",
            &[&bytes, &request.operation_id().as_str()],
        )
        .await
        .unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    let old_history = snapshot_imported_history(&client).await;
    let lost = Arc::new(LostPositionReply::default());
    let backend = sql_backend::postgres::PostgresBackend::initialize(
        &store.postgres_dsn(),
        4,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap()
    .with_fault_probe(lost.clone());
    let result = sql_backend::SqlStore::initialize_schema(
        backend,
        storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
        true,
    )
    .await;
    assert!(result.err().is_some_and(|error| error.is_commit_unknown()));
    assert_eq!(
        lost.updates.load(SeqCst),
        1,
        "unknown publication is not retried"
    );
    assert_eq!(lost.replies_lost.load(SeqCst), 1);
    let offset: i64 = client
        .query_one(
            "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        offset > 0,
        "the acknowledged native commit persisted its offset"
    );
    let store = store.reopen().await;
    let reopened: i64 = client
        .query_one(
            "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(reopened, offset);
    assert_eq!(snapshot_imported_history(&client).await, old_history);
    assert_eq!(
        store
            .stream_operations
            .split_system_stream(request)
            .await
            .unwrap()
            .disposition(),
        stream_provider::SystemStreamSplitDisposition::Replayed
    );
    put_value(&store, &table, "after", "later").await;
    let newest: i64 = client
        .query_one(
            "SELECT max(split_part(offset_text, '-', 1)::bigint) FROM sys_stream_entries",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        newest > sealed,
        "a sealed end alone must raise new positions"
    );
    let preserved = snapshot_imported_history(&client).await;
    assert!(old_history.iter().all(|row| preserved.contains(row)));
    store.finish().await;
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_writer_holding_scope_lock_when_open_raises_offset_then_open_waits_for_its_commit() {
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "hot", "seed").await;
    let held = census.hold(|wave| wave.sql_contains("INSERT INTO sys_stream_entries"));
    let operations = Arc::clone(&store.operations);
    let request = PutItemRequest::try_from(
        json!({"TableName": table, "Item": {"pk": {"S": "hot"}, "value": {"S": "held"}}}),
    )
    .unwrap();
    let writer = tokio::spawn(async move { operations.put_item(request).await });
    held.reached().await;
    let (admin, admin_connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = admin_connection.await;
    });
    let native: i64 = admin
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .await
        .unwrap()
        .get(0);
    let system_high = (native + 1_000_000) * 65_536 + 65_535;
    // Move the seed entry above the allocation floor so the open must raise the
    // offset.
    admin
        .execute(
            "WITH positions AS (SELECT ctid, $1::bigint - row_number() OVER (ORDER BY \
             offset_order DESC) + 1 AS position FROM sys_stream_entries) UPDATE \
             sys_stream_entries AS entry SET offset_order = int8send(positions.position), \
             offset_text = lpad(positions.position::text, 20, '0') || '-ns' FROM positions WHERE \
             entry.ctid = positions.ctid",
            &[&system_high],
        )
        .await
        .unwrap();
    let backend = sql_backend::postgres::PostgresBackend::initialize(
        &store.postgres_dsn(),
        4,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let opening = tokio::spawn(async move {
        sql_backend::SqlStore::initialize_schema(
            backend,
            storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
            true,
        )
        .await
    });
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let waited = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let waiting: bool = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE relation = \
                     'sys_system_stream_scope'::regclass AND mode = 'ShareRowExclusiveLock' AND \
                     NOT granted)",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            if waiting {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        !opening.is_finished(),
        "an open that must raise the offset waits for the live writer"
    );
    held.release();
    writer.await.unwrap().unwrap();
    let opened = tokio::time::timeout(Duration::from_secs(20), opening)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        waited.is_ok(),
        "opening must drain the compatible live writer before scanning positions"
    );
    drop(opened);
    put_value(&store, &table, "hot", "after").await;
    store.wait_until_changes_are_readable().await;
    let rows = client
        .query(
            "SELECT split_part(offset_text, '-', 1)::bigint FROM sys_stream_entries ORDER BY \
             offset_order",
            &[],
        )
        .await
        .unwrap();
    let positions = rows
        .iter()
        .map(|row| row.get::<_, i64>(0))
        .collect::<Vec<_>>();
    assert_eq!(positions.len(), 3);
    store.finish().await;
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_unchanged_offset_when_another_store_opens_then_open_does_not_wait_for_a_live_writer()
{
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "hot", "seed").await;
    let held = census.hold(|wave| wave.sql_contains("INSERT INTO sys_stream_entries"));
    let operations = Arc::clone(&store.operations);
    let request = PutItemRequest::try_from(
        json!({"TableName": table, "Item": {"pk": {"S": "hot"}, "value": {"S": "held"}}}),
    )
    .unwrap();
    let writer = tokio::spawn(async move { operations.put_item(request).await });
    held.reached().await;
    let backend = sql_backend::postgres::PostgresBackend::initialize(
        &store.postgres_dsn(),
        4,
        false,
        storage_types::AttemptDeadlines::default(),
    )
    .await
    .unwrap();
    let opening = tokio::spawn(async move {
        sql_backend::SqlStore::initialize_schema(
            backend,
            storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
            true,
        )
        .await
    });
    let opened = tokio::time::timeout(Duration::from_secs(10), opening)
        .await
        .expect("open must not wait for a live writer when the offset is unchanged")
        .unwrap()
        .unwrap();
    drop(opened);
    held.release();
    writer.await.unwrap().unwrap();
    store.finish().await;
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_imported_retired_bound_above_tail_when_reopened_then_new_position_exceeds_retired_history()
 {
    let store = TestStore::new_postgres().await;
    let (table, _) = create_stream_table(&store, "KEYS_ONLY").await;
    put_value(&store, &table, "before", "original").await;
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let native: i64 = client
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .await
        .unwrap()
        .get(0);
    let retired = (native + 1_000_000) * 65_536 + 65_535;
    let old_history = snapshot_imported_history(&client).await;
    client
        .execute(
            "UPDATE sys_system_stream_lineage SET retired_offset = $1 WHERE scope_key = 'system'",
            &[&retired],
        )
        .await
        .unwrap();
    client
        .execute(
            "UPDATE sys_system_stream_scope SET transaction_offset = 17 WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap();
    let store = store.reopen().await;
    let offset: i64 = client
        .query_one(
            "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(offset > 17);
    assert_eq!(snapshot_imported_history(&client).await, old_history);
    put_value(&store, &table, "after", "later").await;
    let position: i64 = client
        .query_one(
            "SELECT max(split_part(offset_text, '-', 1)::bigint) FROM sys_stream_entries",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        position > retired,
        "retained tail alone is insufficient after retirement"
    );
    let preserved = snapshot_imported_history(&client).await;
    assert!(old_history.iter().all(|row| preserved.contains(row)));
    store.finish().await;
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_older_unrelated_transaction_when_written_parent_splits_then_parent_and_child_history_survive()
 {
    use std::panic::AssertUnwindSafe;

    use futures::FutureExt as _;
    use stream_provider::{
        DynamoMutationRoute, SystemStreamLineage, SystemStreamLineageCatalog,
        SystemStreamPageRequest, SystemStreamSplitChild, SystemStreamSplitOperationId,
        SystemStreamSplitRequest,
    };

    let store = TestStore::new_postgres().await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let scope = store.stream_operations.system_stream_scope().await.unwrap();
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Initialization must finish first: opening intentionally waits for old
    // transactions.
    client.batch_execute("BEGIN").await.unwrap();
    let held_xid: i64 = client
        .query_one("SELECT pg_current_xact_id()::text::bigint", &[])
        .await
        .unwrap()
        .get(0);
    let outcome = AssertUnwindSafe(async {
        put_value(&store, &table, "hot", "parent-one").await;
        put_value(&store, &table, "hot", "parent-two").await;
        let catalog = SystemStreamLineageCatalog::try_new(
            scope.clone(),
            store
                .stream_operations
                .system_stream_lineage_catalog(scope.clone())
                .await
                .unwrap(),
        )
        .unwrap();
        let key: storage_types::KeyAttributes =
            serde_json::from_value(json!({"pk": {"S": "hot"}})).unwrap();
        let schema = super::test_support::hash_table_request(table.clone());
        let route = DynamoMutationRoute::try_new(&table, &schema.key_schema, &key).unwrap();
        let parent = catalog.writer_lineage(route.routing_key()).unwrap().clone();
        let row = client
            .query_one(
                "SELECT max(split_part(offset_text, '-', 1)::bigint) FROM sys_stream_entries \
                 WHERE lineage_id = $1",
                &[&parent.lineage_id().as_str()],
            )
            .await
            .unwrap();
        let tail: i64 = row.get(0);
        let offset: i64 = client
            .query_one(
                "SELECT transaction_offset FROM sys_system_stream_scope WHERE scope_key = 'system'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        let xmin: i64 = client
            .query_one(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert!(xmin <= held_xid && tail / 65_536 - offset > held_xid);
        let bounds = client
            .query_one(
                "SELECT retired_offset, initial_sequence FROM sys_system_stream_lineage WHERE \
                 lineage_id = $1",
                &[&parent.lineage_id().as_str()],
            )
            .await
            .unwrap();
        let retired: i64 = bounds.get(0);
        let initial: i64 = bounds.get(1);
        let expected_bound = ((xmin + offset) * 65_536)
            .max(retired + 1)
            .max(tail + 1)
            .max(initial);
        let children = ["held-left", "held-right"].map(|id| {
            SystemStreamSplitChild::try_new(
                SystemStreamLineage::try_new(
                    id,
                    id,
                    Some(parent.lineage_id().as_str().to_owned()),
                    false,
                )
                .unwrap(),
                1,
            )
            .unwrap()
        });
        let split = SystemStreamSplitRequest::try_new(
            scope.clone(),
            parent.clone(),
            children,
            SystemStreamSplitOperationId::try_new("held-parent-split").unwrap(),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            store.stream_operations.split_system_stream(split.clone()),
        )
        .await
        .expect("split must not wait for the unrelated transaction")
        .unwrap();
        let sealed: i64 = client
            .query_one(
                "SELECT sealed_after_offset FROM sys_system_stream_lineage WHERE lineage_id = $1",
                &[&parent.lineage_id().as_str()],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(sealed, expected_bound);
        put_value(&store, &table, "hot", "child").await;
        let child_catalog = SystemStreamLineageCatalog::try_new(
            scope.clone(),
            store
                .stream_operations
                .system_stream_lineage_catalog(scope.clone())
                .await
                .unwrap(),
        )
        .unwrap();
        let child = child_catalog.writer_lineage(route.routing_key()).unwrap();
        assert_eq!(child.parent_lineage_id(), Some(parent.lineage_id()));
        let mut checkpoint = None;
        let mut images = Vec::new();
        let mut positions = Vec::new();
        for _ in 0..8 {
            let page = store
                .stream_operations
                .get_system_stream_page(
                    SystemStreamPageRequest::try_new(scope.clone(), checkpoint, 1).unwrap(),
                )
                .await
                .unwrap();
            for record in page.records() {
                assert_eq!(record.keys, key);
                images.push(image(record.item.as_ref()));
                positions.push(record.system_position.clone().unwrap());
            }
            checkpoint = Some(page.issued_checkpoint().token().clone());
            if page.terminal() {
                break;
            }
        }
        assert_eq!(images, [json!("parent-one"), json!("parent-two")]);
        assert!(
            positions
                .iter()
                .all(|position| position.lineage_id() == parent.lineage_id())
        );
        // Releasing the old transaction alone must publish the child; no extra
        // writer.
        client.batch_execute("ROLLBACK").await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let page = store
                    .stream_operations
                    .get_system_stream_page(
                        SystemStreamPageRequest::try_new(scope.clone(), checkpoint.clone(), 1)
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                for record in page.records() {
                    assert_eq!(record.keys, key);
                    images.push(image(record.item.as_ref()));
                    positions.push(record.system_position.clone().unwrap());
                }
                checkpoint = Some(page.issued_checkpoint().token().clone());
                if images.len() == 3 && page.terminal() {
                    break;
                }
                assert!(images.len() <= 3);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("child becomes visible without another write");
        assert_eq!(
            images,
            [json!("parent-one"), json!("parent-two"), json!("child")]
        );
        assert_eq!(positions[2].lineage_id(), child.lineage_id());
        assert!(positions[0].offset() < positions[1].offset());
        let count: i64 = client
            .query_one("SELECT count(*) FROM sys_system_stream_sequences", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(count, 0);
        let retained: i64 = client
            .query_one(
                "SELECT retired_offset FROM sys_system_stream_lineage WHERE lineage_id = $1",
                &[&parent.lineage_id().as_str()],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(retained, retired);
        (split, sealed)
    })
    .catch_unwind()
    .await;
    client.batch_execute("ROLLBACK").await.unwrap();
    let (split, sealed) = match outcome {
        Ok(value) => value,
        Err(error) => {
            store.finish().await;
            std::panic::resume_unwind(error);
        }
    };
    let store = store.reopen().await;
    let replay = store
        .stream_operations
        .split_system_stream(split)
        .await
        .unwrap();
    assert_eq!(
        replay.disposition(),
        stream_provider::SystemStreamSplitDisposition::Replayed
    );
    let reopened: i64 = client
        .query_one(
            "SELECT sealed_after_offset FROM sys_system_stream_lineage WHERE lineage_id = $1",
            &[&replay.sealed_parent().lineage_id().as_str()],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(reopened, sealed);
    store.finish().await;
}
