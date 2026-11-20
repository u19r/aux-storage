//! W6F N1: the Postgres catalog advisory lock (W6 A8).
//!
//! Item writers hold the lock of each table shared and read the stamp without
//! a row lock; catalog changes hold it exclusive. A write attempt gets no
//! transaction ID before its item lock. The facts come from `pg_locks` and
//! `pg_stat_activity`, not from timing.

use std::{future::Future, sync::Arc, time::Duration};

use serde_json::{Value, json};
use storage_types::{
    BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest, DeleteTableRequest,
    PutItemRequest, TableName, TimeToLiveSpecification, TransactWriteItemsRequest,
    UpdateItemRequest, UpdateTableRequest, UpdateTimeToLiveRequest,
};

use super::{
    dynamodb_stream_history_tests::{create_stream_table, put_value},
    native_census_test_support::{CensusWave, NativeCensus},
    test_support::{TestStore, hash_table_request, unique_table_name},
};

/// `POSTGRES_CATALOG_LOCK_CLASS` of `sql-backend` (`'AUXC'` as `int4`).
const CATALOG_LOCK_CLASS: i64 = 0x4155_5843;
const ITEM_LOCK: &str = "hashtextextended";
const EXCLUSIVE_CATALOG_LOCK: &str = "pg_advisory_xact_lock(1096112195";
const SHARED_CATALOG_LOCK: &str = "pg_advisory_xact_lock_shared(1096112195";
const TIMEOUT: Duration = Duration::from_secs(20);

fn request<T>(value: Value) -> T
where
    T: TryFrom<Value>,
    T::Error: std::fmt::Debug,
{
    T::try_from(value).expect("valid request")
}

fn require_postgres() {
    assert_eq!(
        std::env::var("STORAGE_TEST_BACKEND").as_deref(),
        Ok("postgres")
    );
}

async fn connect(store: &TestStore) -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
            .await
            .expect("connect to Postgres");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Catalog lock requests of `table` in `pg_locks`: (granted, mode, pid).
async fn catalog_locks(
    client: &tokio_postgres::Client,
    table: &TableName,
) -> Vec<(bool, String, i32)> {
    client
        .query(
            "SELECT granted, mode, pid FROM pg_locks WHERE locktype = 'advisory' AND \
             classid::bigint = $1 AND objid::bigint = (hashtext(current_schema() || '.' || \
             $2)::bigint & 4294967295) AND objsubid = 2",
            &[&CATALOG_LOCK_CLASS, &table.as_ref()],
        )
        .await
        .expect("read pg_locks")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2)))
        .collect()
}

async fn wait_for_catalog_waiters(
    client: &tokio_postgres::Client,
    table: &TableName,
    count: usize,
) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let waiting = catalog_locks(client, table)
                .await
                .iter()
                .filter(|(granted, _, _)| !granted)
                .count();
            if waiting >= count {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("catalog lock waiters did not appear");
}

fn spawn_put(
    store: &TestStore,
    table: &TableName,
    key: &str,
    value: &str,
) -> tokio::task::JoinHandle<storage_types::StorageResult<storage_types::PutItemResponse>> {
    let operations = Arc::clone(&store.operations);
    let put = request::<PutItemRequest>(json!({
        "TableName": table.as_ref(), "Item": {"pk": {"S": key}, "value": {"S": value}}
    }));
    tokio::spawn(async move { operations.put_item(put).await })
}

fn stream_enable(table: &TableName) -> UpdateTableRequest {
    request(json!({
        "TableName": table.as_ref(),
        "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
    }))
}

/// Ordinary conditions reject before any physical writes. Job conditions and
/// SERIALIZABLE TTL operations have separate native locking requirements.
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_existing_and_absent_items_when_conditions_fail_then_no_xid_or_writes() {
    require_postgres();
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "existing", "seed").await;
    put_value(&store, &table, "delete_target", "seed").await;
    let client = connect(&store).await;
    let before_items = read_table_items(&store, &table).await;
    let before_stream = read_stream_rows(&client).await;

    for (operation, key) in [
        ("put", "existing"),
        ("put", "absent"),
        ("update", "existing"),
        ("update", "absent"),
        ("delete", "existing"),
        ("delete", "absent"),
        ("transaction", "existing"),
    ] {
        census.reset();
        let held = census.hold(|wave| {
            matches!(wave,
                CensusWave::Sql(statements) if statements.iter().any(|(sql, _)| *sql == "COMMIT")
            )
        });
        let operations = Arc::clone(&store.operations);
        let name = table.clone();
        let writer = tokio::spawn(async move {
            reject_condition(operations.as_ref(), &name, operation, key).await
        });
        held.reached().await;
        // Capture evidence before releasing the empty COMMIT, but assert only
        // after the request can finish so an observation failure cannot strand
        // its task.
        let rows = client
            .query(
                "SELECT a.backend_xid IS NULL FROM pg_locks AS l JOIN pg_stat_activity AS a ON \
                 a.pid = l.pid WHERE l.locktype = 'advisory' AND l.classid::bigint = $1 AND \
                 l.objid::bigint = (hashtext(current_schema() || '.' || $2)::bigint & 4294967295) \
                 AND l.objsubid = 2 AND l.granted AND l.mode = 'ShareLock'",
                &[&CATALOG_LOCK_CLASS, &table.as_ref()],
            )
            .await;
        held.release();
        let error = tokio::time::timeout(TIMEOUT, writer)
            .await
            .expect("rejected request completes")
            .expect("request task")
            .expect_err("condition must reject");
        let counts = census.snapshot();
        let rows = rows.expect("observe transaction before empty commit");
        assert_eq!(rows.len(), 1, "one held request: {operation}/{key}");
        assert!(
            rows[0].get::<_, bool>(0),
            "assigned transaction ID: {operation}/{key}"
        );
        match error.as_ref() {
            storage_types::StorageEnum::ConditionalCheckFailed if operation != "transaction" => {}
            storage_types::StorageEnum::TransactionCanceled { reasons }
                if operation == "transaction" =>
            {
                assert_eq!(reasons.len(), 4);
                assert!(
                    reasons[..3]
                        .iter()
                        .all(|reason| *reason == storage_types::CancellationReason::none())
                );
                assert!(reasons[3].is_conditional_check_failed());
            }
            other => panic!("unexpected rejection for {operation}/{key}: {other:?}"),
        }
        assert_eq!(
            counts.rows_written, 0,
            "changed rows: {operation}/{key}: {counts}"
        );
        assert!(
            counts.touched_written.is_empty(),
            "write statements: {operation}/{key}: {counts}"
        );
        assert_eq!(counts.rollbacks, 0, "rollback: {operation}/{key}: {counts}");
        assert_eq!(counts.commits, 1, "commit: {operation}/{key}: {counts}");
        assert_eq!(read_table_items(&store, &table).await, before_items);
        assert_eq!(read_stream_rows(&client).await, before_stream);
    }
    store.finish().await;
}

async fn read_table_items(store: &TestStore, table: &TableName) -> Vec<String> {
    let page = store
        .operations
        .scan(storage_types::ScanRequest::new(table.clone()))
        .await
        .expect("read primary items");
    assert!(
        page.last_evaluated_key.is_none(),
        "small fixture fits one page"
    );
    let mut items = page
        .items
        .unwrap_or_default()
        .into_iter()
        .map(|item| serde_json::to_string(&item).expect("serialize item"))
        .collect::<Vec<_>>();
    items.sort();
    items
}

async fn read_stream_rows(client: &tokio_postgres::Client) -> Vec<String> {
    client
        .query(
            "SELECT row_to_json(s)::text FROM sys_stream_entries AS s ORDER BY \
             row_to_json(s)::text",
            &[],
        )
        .await
        .expect("read stored stream events")
        .into_iter()
        .map(|row| row.get(0))
        .collect()
}

async fn reject_condition(
    operations: &dyn storage_provider::DynamoOperations,
    table: &TableName,
    operation: &str,
    key: &str,
) -> storage_types::StorageResult<()> {
    let condition = if key == "existing" {
        "attribute_not_exists(pk)"
    } else {
        "attribute_exists(pk)"
    };
    let mut body = json!({"TableName": table.as_ref(), "Key": {"pk": {"S": key}},
        "ConditionExpression": condition});
    match operation {
        "put" => {
            body.as_object_mut().unwrap().remove("Key");
            body["Item"] = json!({"pk": {"S": key}, "value": {"S": "rejected"}});
            operations.put_item(request::<PutItemRequest>(body)).await.map(|_| ())
        }
        "update" => {
            body["UpdateExpression"] = json!("SET #v = :v");
            body["ExpressionAttributeNames"] = json!({"#v": "value"});
            body["ExpressionAttributeValues"] = json!({":v": {"S": "rejected"}});
            operations.update_item(request::<UpdateItemRequest>(body)).await.map(|_| ())
        }
        "delete" => operations.delete_item(request::<DeleteItemRequest>(body)).await.map(|_| ()),
        "transaction" => operations.transact_write_items(request::<TransactWriteItemsRequest>(json!({
            "TransactItems": [
                {"Put": {"TableName": table.as_ref(), "Item": {"pk": {"S": "new"}, "value": {"S": "rejected"}}}},
                {"Update": {"TableName": table.as_ref(), "Key": {"pk": {"S": "existing"}},
                    "UpdateExpression": "SET #v = :v", "ExpressionAttributeNames": {"#v": "value"},
                    "ExpressionAttributeValues": {":v": {"S": "rejected"}}}},
                {"Delete": {"TableName": table.as_ref(), "Key": {"pk": {"S": "delete_target"}}}},
                {"ConditionCheck": {"TableName": table.as_ref(), "Key": {"pk": {"S": "absent"}},
                    "ConditionExpression": "attribute_exists(pk)"}}
            ]
        }))).await.map(|_| ()),
        _ => unreachable!("known test operation"),
    }
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_write_attempt_then_no_xid_before_item_lock() {
    require_postgres();
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    put_value(&store, &table, "hot", "seed").await;
    let client = connect(&store).await;

    // The held writer holds the shared catalog lock after the observation wave.
    let held = census.hold(|wave| {
        matches!(wave,
            CensusWave::Sql(statements) if statements.iter().any(|(sql, _)| *sql == "COMMIT")
        )
    });
    let writer = spawn_put(&store, &table, "hot", "x");
    held.reached().await;
    let rows = client
        .query(
            "SELECT a.backend_xid IS NULL FROM pg_locks AS l JOIN pg_stat_activity AS a ON a.pid \
             = l.pid WHERE l.locktype = 'advisory' AND l.classid::bigint = $1 AND l.objid::bigint \
             = (hashtext(current_schema() || '.' || $2)::bigint & 4294967295) AND l.objsubid = 2 \
             AND l.granted AND l.mode = 'ShareLock'",
            &[&CATALOG_LOCK_CLASS, &table.as_ref()],
        )
        .await
        .expect("read held writer");
    held.release();
    tokio::time::timeout(TIMEOUT, writer)
        .await
        .expect("held writer completes")
        .expect("writer task")
        .expect("held put");
    assert_eq!(rows.len(), 1, "one writer holds the shared catalog lock");
    assert!(
        rows[0].get::<_, bool>(0),
        "the writer has a transaction ID before its item lock"
    );

    // Each item writer on a stream table passes the transaction ID guard.
    let name = table.as_ref();
    store
        .operations
        .update_item(request::<UpdateItemRequest>(json!({
            "TableName": name, "Key": {"pk": {"S": "hot"}},
            "UpdateExpression": "SET #v = :v", "ExpressionAttributeNames": {"#v": "value"},
            "ExpressionAttributeValues": {":v": {"S": "updated"}}
        })))
        .await
        .expect("update");
    store
        .operations
        .batch_write_item(request::<BatchWriteItemRequest>(
            json!({"RequestItems": {name: [
                {"PutRequest": {"Item": {"pk": {"S": "b1"}, "value": {"S": "b"}}}},
                {"DeleteRequest": {"Key": {"pk": {"S": "seedless"}}}}
            ]}}),
        ))
        .await
        .expect("batch write");
    store
        .operations
        .transact_write_items(request::<TransactWriteItemsRequest>(
            json!({"TransactItems": [
                {"Put": {"TableName": name, "Item": {"pk": {"S": "t1"}, "value": {"S": "t"}}}},
                {"ConditionCheck": {"TableName": name, "Key": {"pk": {"S": "hot"}},
                    "ConditionExpression": "attribute_exists(pk)"}}
            ]}),
        ))
        .await
        .expect("transact write");
    store
        .operations
        .delete_item(request::<DeleteItemRequest>(
            json!({"TableName": name, "Key": {"pk": {"S": "b1"}}}),
        ))
        .await
        .expect("delete");
    store.finish().await;
}

#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_put_held_after_wave1_when_update_table_starts_then_transition_waits_and_next_put_sees_new_stamp()
 {
    require_postgres();
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let (table, _) = create_stream_table(&store, "NEW_AND_OLD_IMAGES").await;
    let client = connect(&store).await;
    let before = record(&census, put_value(&store, &table, "hot", "seed")).await;

    // X holds the shared catalog lock after the observation wave.
    let held = census.hold(|wave| {
        matches!(wave,
            CensusWave::Sql(statements) if statements.iter().any(|(sql, _)| *sql == "COMMIT")
        )
    });
    let writer = spawn_put(&store, &table, "hot", "x");
    held.reached().await;
    let operations = Arc::clone(&store.operations);
    let update = request::<UpdateTableRequest>(json!({
        "TableName": table.as_ref(), "StreamSpecification": {"StreamEnabled": false}
    }));
    let transition = tokio::spawn(async move { operations.update_table(update).await });
    wait_for_catalog_waiters(&client, &table, 1).await;
    let locks = catalog_locks(&client, &table).await;
    let finished = transition.is_finished();
    held.release();
    tokio::time::timeout(TIMEOUT, writer)
        .await
        .expect("held writer completes")
        .expect("writer task")
        .expect("put X");
    tokio::time::timeout(TIMEOUT, transition)
        .await
        .expect("transition completes")
        .expect("transition task")
        .expect("update table");

    // The next writer binds the new stamp: it writes no stream entry.
    let after = record(&census, put_value(&store, &table, "hot", "y")).await;
    store.finish().await;
    assert!(!finished, "the transition must wait for the held writer");
    assert!(
        locks
            .iter()
            .any(|(granted, mode, _)| *granted && mode == "ShareLock")
            && locks
                .iter()
                .any(|(granted, mode, _)| !granted && mode == "ExclusiveLock"),
        "the transition waits on the exclusive catalog lock: {locks:?}"
    );
    let writes_stream = |statements: &[String]| {
        statements
            .iter()
            .any(|sql| sql.contains("INSERT INTO sys_stream_entries ("))
    };
    assert!(
        writes_stream(&before),
        "a stream table write has a stream entry"
    );
    assert!(
        !writes_stream(&after),
        "the put after the transition used the old binding: {after:#?}"
    );
}

/// Statements of one operation, in send order.
async fn record<F: Future<Output = ()>>(census: &NativeCensus, operation: F) -> Vec<String> {
    census.record();
    operation.await;
    census.recorded()
}

fn is_catalog_change(sql: &str) -> bool {
    let words = sql.split_whitespace().collect::<Vec<_>>();
    match words.as_slice() {
        ["UPDATE", relation, "SET", ..] => relation.trim_matches('"') == "tables",
        ["INSERT", "INTO", relation, ..] => relation.trim_matches('"') == "tables",
        _ => false,
    }
}

/// Every catalog change follows an exclusive catalog lock; every item lock
/// follows a catalog lock, and the first ordering lock depends on its saved
/// transaction ID check.
fn assert_lock_order(operation: &str, statements: &[String], changes_catalog: bool) {
    let exclusive = statements
        .iter()
        .position(|sql| sql.contains(EXCLUSIVE_CATALOG_LOCK));
    let any_catalog = statements
        .iter()
        .position(|sql| sql.contains(EXCLUSIVE_CATALOG_LOCK) || sql.contains(SHARED_CATALOG_LOCK));
    let change = statements.iter().position(|sql| is_catalog_change(sql));
    let item = statements.iter().position(|sql| sql.contains(ITEM_LOCK));
    assert_eq!(
        change.is_some(),
        changes_catalog,
        "{operation}: catalog change expected = {changes_catalog}: {statements:#?}"
    );
    if let Some(change) = change {
        assert!(
            exclusive.is_some_and(|lock| lock < change),
            "{operation}: catalog change before the exclusive catalog lock: {statements:#?}"
        );
    }
    if let Some(item) = item {
        assert!(
            any_catalog.is_some_and(|lock| lock < item),
            "{operation}: item lock before the catalog lock: {statements:#?}"
        );
    }
    let ordering = [exclusive, item].into_iter().flatten().min();
    if let Some(ordering) = ordering {
        assert!(
            statements[ordering].contains("WITH xid_before_lock AS MATERIALIZED")
                && statements[ordering].contains("pg_current_xact_id_if_assigned")
                && statements[ordering].contains("OFFSET xid_before_lock.assigned * 0"),
            "{operation}: no saved transaction ID dependency on the first ordering lock: \
             {statements:#?}"
        );
    }
}

/// The recording of W6F section 4.2: the catalog mutators and the item-lock
/// owners of the Dynamo API and the TTL sweep.
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_catalog_mutators_and_item_lock_owners_when_recorded_then_locks_precede_effects() {
    require_postgres();
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = unique_table_name();
    let name = table.as_ref();
    let operations = &store.operations;
    let mut cases = Vec::new();

    cases.push((
        "CreateTable",
        true,
        record(&census, async {
            operations
                .create_table(request::<CreateTableRequest>(json!({
                    "TableName": name, "BillingMode": "PAY_PER_REQUEST",
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "AuxStreamDurationHours": 48,
                    "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES"}
                })))
                .await
                .expect("create table");
        })
        .await,
    ));
    cases.push((
        "UpdateTimeToLive",
        true,
        record(&census, async {
            operations
                .update_time_to_live(UpdateTimeToLiveRequest {
                    table_name: table.clone(),
                    time_to_live_specification: TimeToLiveSpecification {
                        attribute_name: "ttl".into(),
                        enabled: true,
                    },
                })
                .await
                .expect("update TTL");
        })
        .await,
    ));
    cases.push((
        "PutItem",
        false,
        record(&census, async {
            operations
                .put_item(request::<PutItemRequest>(json!({
                    "TableName": name, "Item": {"pk": {"S": "a"}, "ttl": {"N": "1"}}
                })))
                .await
                .expect("put");
        })
        .await,
    ));
    cases.push((
        "UpdateItem",
        false,
        record(&census, async {
            operations
                .update_item(request::<UpdateItemRequest>(json!({
                    "TableName": name, "Key": {"pk": {"S": "b"}},
                    "UpdateExpression": "SET v = :v",
                    "ExpressionAttributeValues": {":v": {"S": "b"}}
                })))
                .await
                .expect("update");
        })
        .await,
    ));
    cases.push((
        "DeleteItem",
        false,
        record(&census, async {
            operations
                .delete_item(request::<DeleteItemRequest>(
                    json!({"TableName": name, "Key": {"pk": {"S": "b"}}}),
                ))
                .await
                .expect("delete");
        })
        .await,
    ));
    cases.push((
        "BatchWriteItem",
        false,
        record(&census, async {
            operations
                .batch_write_item(request::<BatchWriteItemRequest>(
                    json!({"RequestItems": {name: [
                        {"PutRequest": {"Item": {"pk": {"S": "c"}}}},
                        {"DeleteRequest": {"Key": {"pk": {"S": "d"}}}}
                    ]}}),
                ))
                .await
                .expect("batch write");
        })
        .await,
    ));
    cases.push((
        "TransactWriteItems",
        false,
        record(&census, async {
            operations
                .transact_write_items(request::<TransactWriteItemsRequest>(
                    json!({"TransactItems": [
                        {"Put": {"TableName": name, "Item": {"pk": {"S": "e"}}}},
                        {"ConditionCheck": {"TableName": name, "Key": {"pk": {"S": "a"}},
                            "ConditionExpression": "attribute_exists(pk)"}}
                    ]}),
                ))
                .await
                .expect("transact write");
        })
        .await,
    ));
    cases.push((
        "TtlSweepPage",
        false,
        record(&census, async {
            let policy = store
                .maintenance
                .read_ttl_sweep_policy(&table)
                .await
                .expect("read TTL policy")
                .expect("TTL enabled");
            let now = store.now();
            let input = storage_provider::TtlSweepPageInput::try_new("ttl", now, 100, None)
                .expect("sweep input");
            let command = storage_provider::TtlSweepPageCommand::try_new(policy, input, now)
                .expect("sweep command");
            let swept = store
                .maintenance
                .execute_ttl_sweep_page(command)
                .await
                .expect("sweep page");
            assert_eq!(swept.deleted_items, 1);
        })
        .await,
    ));
    cases.push((
        "UpdateTable",
        true,
        record(&census, async {
            operations
                .update_table(request::<UpdateTableRequest>(
                    json!({"TableName": name, "AuxStreamDurationHours": 24}),
                ))
                .await
                .expect("update table");
        })
        .await,
    ));
    cases.push((
        "DeleteTable",
        true,
        record(&census, async {
            operations
                .delete_table(request::<DeleteTableRequest>(json!({"TableName": name})))
                .await
                .expect("delete table");
        })
        .await,
    ));
    store.finish().await;

    for (operation, changes_catalog, statements) in &cases {
        assert_lock_order(operation, statements, *changes_catalog);
        if *operation == "TtlSweepPage" {
            assert!(
                statements
                    .iter()
                    .any(|sql| sql.contains(SHARED_CATALOG_LOCK))
            );
            assert!(
                !statements
                    .iter()
                    .any(|sql| sql.contains(EXCLUSIVE_CATALOG_LOCK))
            );
        }
    }
    for owner in [
        "PutItem",
        "UpdateItem",
        "DeleteItem",
        "BatchWriteItem",
        "TransactWriteItems",
        "TtlSweepPage",
    ] {
        let (_, _, statements) = cases
            .iter()
            .find(|(operation, _, _)| *operation == owner)
            .expect("recorded owner");
        assert!(
            statements.iter().any(|sql| sql.contains(ITEM_LOCK)),
            "{owner} takes an item lock"
        );
    }
}

/// W6F experiment (a): VACUUM, an index build, a catalog transition, and a
/// new writer next to a held writer. Writers and the transition wait only on
/// the advisory catalog lock, never on a relation lock.
#[tokio::test]
#[ignore = "requires STORAGE_TEST_BACKEND=postgres and TEST_POSTGRES_DSN"]
async fn given_held_writer_when_vacuum_index_build_and_transition_run_then_only_advisory_waits() {
    require_postgres();
    let census = NativeCensus::new();
    let store = TestStore::new_with_census(Arc::clone(&census)).await;
    let table = unique_table_name();
    store
        .operations
        .create_table(hash_table_request(table.clone()))
        .await
        .expect("create table");
    put_value(&store, &table, "seed", "seed").await;
    let client = connect(&store).await;
    let id: String = client
        .query_one(
            "SELECT id FROM tables WHERE table_name = $1 AND table_status <> 'DELETING'",
            &[&table.as_ref()],
        )
        .await
        .expect("catalog row")
        .get(0);
    let relation = format!("table_{}", id.replace('-', ""));
    let column: String = client
        .query_one(
            "SELECT column_name::text FROM information_schema.columns WHERE table_schema = \
             current_schema() AND table_name = $1 ORDER BY ordinal_position LIMIT 1",
            &[&relation],
        )
        .await
        .expect("item column")
        .get(0);

    // The held writer holds the shared catalog lock after the observation wave.
    let held = census.hold(|wave| {
        matches!(wave,
            CensusWave::Sql(statements) if statements.iter().any(|(sql, _)| *sql == "COMMIT")
        )
    });
    let writer = spawn_put(&store, &table, "seed", "x");
    held.reached().await;

    // VACUUM completes while the writer is held.
    tokio::time::timeout(
        TIMEOUT,
        client.batch_execute(&format!("VACUUM \"{relation}\"")),
    )
    .await
    .expect("VACUUM does not wait for the held writer")
    .expect("VACUUM");
    let index_client = connect(&store).await;
    let index = format!(
        "CREATE INDEX CONCURRENTLY \"{relation}_experiment\" ON \"{relation}\" (\"{column}\")"
    );
    let index_build = tokio::spawn(async move { index_client.batch_execute(&index).await });
    let operations = Arc::clone(&store.operations);
    let update = stream_enable(&table);
    let transition = tokio::spawn(async move { operations.update_table(update).await });
    wait_for_catalog_waiters(&client, &table, 1).await;
    let second = spawn_put(&store, &table, "other", "y");
    wait_for_catalog_waiters(&client, &table, 2).await;

    let relation_waits: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'relation' AND NOT granted AND \
             relation IN (to_regclass($1), to_regclass('tables'))",
            &[&format!("\"{relation}\"")],
        )
        .await
        .expect("relation waits")
        .get(0);
    let locks = catalog_locks(&client, &table).await;
    held.release();
    for put in [writer, second] {
        tokio::time::timeout(TIMEOUT, put)
            .await
            .expect("writer completes")
            .expect("writer task")
            .expect("put");
    }
    tokio::time::timeout(TIMEOUT, transition)
        .await
        .expect("transition completes")
        .expect("transition task")
        .expect("update table");
    tokio::time::timeout(TIMEOUT, index_build)
        .await
        .expect("index build completes")
        .expect("index task")
        .expect("index build");
    store.finish().await;
    assert_eq!(relation_waits, 0, "no wait on a relation lock");
    let waiting = locks
        .iter()
        .filter(|(granted, _, _)| !granted)
        .map(|(_, mode, _)| mode.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        waiting.len(),
        2,
        "the transition and the new writer wait on the catalog lock: {locks:?}"
    );
    assert!(waiting.contains(&"ExclusiveLock") && waiting.contains(&"ShareLock"));
}
