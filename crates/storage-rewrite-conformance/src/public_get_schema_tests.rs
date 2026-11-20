//! Real PostgreSQL DDL races at the public Get boundary.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use futures::FutureExt as _;
use serde_json::json;
use storage_types::{StorageError, StorageResult};
use tokio::sync::Notify;

use super::test_support::TestStore;

#[derive(Default)]
struct SchemaRace {
    armed: AtomicBool,
    arrived: Notify,
    released: Notify,
    queries: AtomicUsize,
    begins: AtomicUsize,
    token_only_waves: AtomicUsize,
    suppress_acknowledgement: AtomicBool,
    query_failure: Mutex<Option<(String, StorageError)>>,
}

impl sql_backend::SqlFaultProbe for SchemaRace {
    fn fail(&self, call: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        let mut failure = self.query_failure.lock().unwrap();
        if call.operation == sql_backend::SqlFaultOperation::Query
            && call.sql.contains("JOIN LATERAL")
            && failure
                .as_ref()
                .is_some_and(|(target, _)| call.sql.contains(target))
        {
            return failure.take().map(|(_, error)| error);
        }
        None
    }

    fn observe(&self, event: sql_backend::SqlNativeEvent) {
        if matches!(event, sql_backend::SqlNativeEvent::Begin) {
            self.begins.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(
            event,
            sql_backend::SqlNativeEvent::Statement {
                operation: sql_backend::SqlFaultOperation::Query,
                ..
            }
        ) {
            self.queries.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn at_commit_boundary(
        &self,
        boundary: sql_backend::SqlCommitBoundary,
        _mode: storage_types::AttemptMode,
    ) -> futures::future::BoxFuture<'_, StorageResult<()>> {
        Box::pin(async move {
            if boundary == sql_backend::SqlCommitBoundary::AfterDurableAcknowledgement
                && self.suppress_acknowledgement.swap(false, Ordering::SeqCst)
            {
                return Err(StorageError::internal(
                    "withheld successful-read acknowledgement",
                ));
            }
            Ok(())
        })
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [sql_backend::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            if statements
                .iter()
                .any(|statement| statement.sql.contains("SELECT receipt_blob"))
                && !statements
                    .iter()
                    .any(|statement| statement.sql.contains("item_value"))
            {
                self.token_only_waves.fetch_add(1, Ordering::SeqCst);
            }
            if statements
                .iter()
                .any(|statement| statement.sql.contains("JOIN LATERAL"))
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.arrived.notify_one();
                tokio::time::timeout(
                    std::time::Duration::from_secs(600),
                    self.released.notified(),
                )
                .await
                .expect("DDL releases combined Get");
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for SchemaRace {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }
}

async fn get(
    store: &TestStore,
    table: &storage_types::TableName,
) -> StorageResult<serde_json::Value> {
    serde_json::to_value(
        store
            .operations
            .get_item(
                serde_json::from_value(json!({
                    "TableName": table, "Key": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await?,
    )
    .map_err(|error| StorageError::internal(&error))
}

async fn read_points(
    store: &TestStore,
    table: &storage_types::TableName,
    operation: &str,
) -> StorageResult<serde_json::Value> {
    read_points_from_operations(store.operations.as_ref(), table, operation).await
}

async fn read_points_from_operations(
    operations: &dyn storage_provider::DynamoOperations,
    table: &storage_types::TableName,
    operation: &str,
) -> StorageResult<serde_json::Value> {
    match operation {
        "query" => serde_json::to_value(operations.query(storage_types::QueryRequest::try_from(json!({
            "TableName": table, "KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": {"S": "customer-1"}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "scan" => serde_json::to_value(operations.scan(storage_types::ScanRequest::try_from(json!({
            "TableName": table
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "put" => serde_json::to_value(operations.put_item(storage_types::PutItemRequest::try_from(json!({
            "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "written"}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "update" => serde_json::to_value(operations.update_item(storage_types::UpdateItemRequest::try_from(json!({
            "TableName": table, "Key": {"pk": {"S": "customer-1"}}, "UpdateExpression": "SET #v = :v",
            "ExpressionAttributeNames": {"#v": "value"}, "ExpressionAttributeValues": {":v": {"S": "written"}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "delete" => serde_json::to_value(operations.delete_item(storage_types::DeleteItemRequest::try_from(json!({
            "TableName": table, "Key": {"pk": {"S": "customer-1"}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "batch_write" => serde_json::to_value(operations.batch_write_item(serde_json::from_value(json!({
            "RequestItems": {table.as_ref(): [{"PutRequest": {"Item": {"pk": {"S": "customer-1"}, "value": {"S": "written"}}}}]}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "transact_write" => serde_json::to_value(operations.transact_write_items(serde_json::from_value(json!({
            "TransactItems": [{"Put": {"TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "written"}}}}]
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "batch" => serde_json::to_value(operations.batch_get_item(serde_json::from_value(json!({
            "RequestItems": {table.as_ref(): {"Keys": [{"pk": {"S": "customer-1"}}, {"pk": {"S": "missing"}}]}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        "transact" => serde_json::to_value(operations.transact_get_items(serde_json::from_value(json!({
            "TransactItems": [
                {"Get": {"TableName": table, "Key": {"pk": {"S": "customer-1"}}}},
                {"Get": {"TableName": table, "Key": {"pk": {"S": "missing"}}}}
            ]
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
        _ => serde_json::to_value(operations.get_item(serde_json::from_value(json!({
            "TableName": table, "Key": {"pk": {"S": "customer-1"}}
        })).unwrap()).await?).map_err(|error| StorageError::internal(&error)),
    }
}

#[derive(Clone, Copy)]
enum SchemaRaceCase {
    MissingTable,
    MissingColumn,
    RecreatedTable,
    RecreatedAfterConflict,
    ExpiredRecovery,
}

async fn run_race(case: SchemaRaceCase) {
    run_point_race(case, "get").await;
}

async fn run_point_race(case: SchemaRaceCase, operation: &str) {
    let expired = matches!(case, SchemaRaceCase::ExpiredRecovery);
    let missing_column = matches!(
        case,
        SchemaRaceCase::MissingColumn | SchemaRaceCase::ExpiredRecovery
    );
    let recreate = matches!(
        case,
        SchemaRaceCase::RecreatedTable | SchemaRaceCase::RecreatedAfterConflict
    );
    let recreate_after_conflict = matches!(case, SchemaRaceCase::RecreatedAfterConflict);
    let is_write = matches!(
        operation,
        "put" | "update" | "delete" | "batch_write" | "transact_write"
    );
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(SchemaRace::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        // Cold resolution publishes the candidate, but never prepares combined SQL.
        read_points(&store, &table, if matches!(operation, "put" | "update" | "delete" | "batch_write" | "transact_write") { "get" } else { operation }).await.unwrap();
        let (admin, connection) = tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap(); });
        let physical: String = admin.query_one("SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'item_value'", &[]).await.unwrap().get(0);
        probe.queries.store(0, Ordering::SeqCst);
        probe.begins.store(0, Ordering::SeqCst);
        probe.armed.store(true, Ordering::SeqCst);
        let retry_pause = recreate_after_conflict.then(|| store.pause_next_retry());
        let (result, ()) = tokio::join!(read_points(&store, &table, operation), async {
            tokio::time::timeout(std::time::Duration::from_secs(10), probe.arrived.notified()).await.unwrap();
            assert_eq!(probe.queries.load(Ordering::SeqCst), 0, "candidate supplied no actual observation");
            if missing_column {
                admin.batch_execute(&format!("ALTER TABLE \"{physical}\" RENAME COLUMN item_value TO retired_item_value")).await.unwrap();
            } else if recreate_after_conflict {
                // The wave must parse, report the replacement's changed stamp and
                // fail at execution: the old name becomes a view after the recreation.
                admin.batch_execute(&format!("ALTER TABLE \"{physical}\" RENAME TO retired_race_item")).await.unwrap();
            } else {
                admin.batch_execute(&format!("DROP TABLE \"{physical}\"")).await.unwrap();
            }
            if recreate {
                let writer = store.independent_operations.as_ref().unwrap();
                writer.delete_table(storage_types::DeleteTableRequest { table_name: table.clone() }).await.unwrap();
                writer.create_table(serde_json::from_value(json!({
                    "TableName": table,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST"
                })).unwrap()).await.unwrap();
                writer.put_item(storage_types::PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "replacement"}}
                })).unwrap()).await.unwrap();
            }
            if recreate_after_conflict {
                admin.batch_execute(&format!("CREATE FUNCTION native_race_failure() RETURNS bytea LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION USING ERRCODE = ''42703''; END'; CREATE VIEW \"{physical}\" AS SELECT pk, native_race_failure() AS item_value, __aux_provenance, __aux_ttl_epoch, __aux_evaluated_bytes FROM retired_race_item")).await.unwrap();
            }
            if expired {
                tokio::time::pause();
                tokio::time::advance(std::time::Duration::from_secs(121)).await;
                tokio::time::resume();
            }
            probe.released.notify_one();
            if let Some(pause) = retry_pause {
                tokio::time::timeout(std::time::Duration::from_secs(10), pause.arrived.notified()).await.unwrap();
                let writer = store.independent_operations.as_ref().unwrap();
                writer.delete_table(storage_types::DeleteTableRequest { table_name: table.clone() }).await.unwrap();
                store.create_hash_table(table.clone()).await;
                pause.released.notify_one();
            }
        });
        if expired {
            if operation == "batch_write" {
                let response = result.unwrap();
                assert_eq!(response["UnprocessedItems"][table.as_ref()].as_array().unwrap().len(), 1);
            } else {
                let error = result.expect_err(&format!("expired schema recovery, operation {operation}: expected an error"));
                assert!(error.attempt_kind().is_some_and(storage_types::AttemptKind::is_unavailable), "expired schema recovery, operation {operation}: error {error:?} begins {} queries {}", probe.begins.load(Ordering::SeqCst), probe.queries.load(Ordering::SeqCst));
            }
            assert!(probe.begins.load(Ordering::SeqCst) <= 1, "expired schema recovery starts no Fresh attempt");
            assert_eq!(probe.queries.load(Ordering::SeqCst), 0, "no completed candidate or Fresh authority read");
            let rows: i64 = admin.query_one(&format!("SELECT count(*) FROM \"{physical}\""), &[]).await.unwrap().get(0);
            assert_eq!(rows, 0, "expired public mutation leaves the table empty");
        } else if recreate_after_conflict {
            let error = result.expect_err(&format!("operation {operation}: expected an error"));
            assert!(matches!(error.as_ref(), storage_types::StorageEnum::TableNotFound { .. } | storage_types::StorageEnum::ResourceNotFound { .. }), "first Fresh actual observation pins the replacement incarnation: {operation} {error:?}");
            if is_write {
                let persisted = read_points_from_operations(store.independent_operations.as_ref().unwrap().as_ref(), &table, "get").await.unwrap();
                assert_eq!(persisted, json!({}), "a rejected retargeted write leaves the newest table empty: {operation}");
            }
        } else if recreate {
            let expected_item = json!({"pk": {"S": "customer-1"}, "value": {"S": "replacement"}});
            let expected = match operation {
                "put" | "update" | "delete" | "transact_write" => json!({}),
                "batch_write" => json!({"UnprocessedItems": {}}),
                "batch" => json!({"Responses": {table.as_ref(): [expected_item]}, "UnprocessedKeys": {}}),
                "transact" => json!({"Responses": [{"Item": expected_item}, {}]}),
                "query" | "scan" => json!({"Items": [expected_item], "Count": 1, "ScannedCount": 1}),
                _ => json!({"Item": expected_item}),
            };
            assert_eq!(result.unwrap_or_else(|error| panic!("operation {operation}: {error:?}")), expected);
            if is_write {
                let persisted = read_points_from_operations(store.independent_operations.as_ref().unwrap().as_ref(), &table, "get").await.unwrap();
                let expected = if operation == "delete" { json!({}) } else { json!({"Item": {"pk": {"S": "customer-1"}, "value": {"S": "written"}}}) };
                assert_eq!(persisted, expected, "recovered mutation must persist its complete result: {operation}");
            }

            assert!(probe.queries.load(Ordering::SeqCst) >= 2, "the fresh try reads the catalog and the item");
        } else {
            let error = result.expect_err(&format!("operation {operation}: expected an error"));
            let expected = if missing_column { "42703" } else { "42P01" };
            match error.as_ref() {
                // The fresh try of a write misses again and is an invariant failure.
                storage_types::StorageEnum::InternalServerError { message } if is_write => assert_eq!(message, "fresh attempt requested another fresh validation", "operation {operation}"),
                // The fresh try of a read is cold and maps the native error.
                storage_types::StorageEnum::InternalServerError { message } => assert_eq!(message, &format!("Postgres native operation failed (SQLSTATE {expected})")),
                other => panic!("original mapped native error expected, received {other:?}"),
            }
            assert!(probe.queries.load(Ordering::SeqCst) >= 1, "the fresh try resolves the table again");
        }
        drop(admin);
        connection.await.unwrap();
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_preparse_missing_table_when_authority_changes_then_public_get_retries_current_incarnation()
 {
    run_race(SchemaRaceCase::RecreatedTable).await;
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_preparse_missing_table_when_authority_is_unchanged_then_public_get_preserves_native_error()
 {
    run_race(SchemaRaceCase::MissingTable).await;
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_preparse_missing_column_when_authority_is_unchanged_then_public_get_preserves_native_error()
 {
    run_race(SchemaRaceCase::MissingColumn).await;
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_cold_get_when_commit_answer_is_suppressed_then_failure_is_known_and_never_publishes_metadata()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(SchemaRace::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        probe.queries.store(0, Ordering::SeqCst);
        probe.suppress_acknowledgement.store(true, Ordering::SeqCst);
        let error = get(&store, &table).await.unwrap_err();
        assert!(
            !error.is_commit_unknown(),
            "read failures are known: {error:?}"
        );
        let first_queries = probe.queries.swap(0, Ordering::SeqCst);
        assert!(first_queries > 1, "cold metadata and item were read");
        assert_eq!(get(&store, &table).await.unwrap(), json!({}));
        assert_eq!(
            probe.queries.load(Ordering::SeqCst),
            first_queries,
            "failed attempt does not publish cold metadata"
        );
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_stamp_change_with_execution_schema_failure_when_table_is_recreated_again_then_public_get_rejects_retargeting()
 {
    run_race(SchemaRaceCase::RecreatedAfterConflict).await;
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_warm_multi_reads_when_candidate_schema_fails_then_fresh_recovery_preserves_error_and_identity()
 {
    for operation in ["batch", "transact"] {
        for case in [
            SchemaRaceCase::MissingTable,
            SchemaRaceCase::MissingColumn,
            SchemaRaceCase::RecreatedTable,
            SchemaRaceCase::RecreatedAfterConflict,
        ] {
            run_point_race(case, operation).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_corrupt_primary_image_when_warm_batch_or_transaction_finishes_then_public_operation_fails()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(SchemaRace::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store
            .operations
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        read_points(&store, &table, "batch").await.unwrap();
        let (admin, connection) =
            tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                .await
                .unwrap();
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let physical: String = admin
            .query_one(
                "SELECT table_name FROM information_schema.columns WHERE table_schema = \
                 current_schema() AND column_name = 'item_value'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        admin
            .batch_execute(&format!(
                "UPDATE \"{physical}\" SET item_value = 'corrupt'::bytea"
            ))
            .await
            .unwrap();
        for operation in ["batch", "transact"] {
            // This independent cache stays cold after each rejected operation,
            // exercising the unchanged ordinary resolver and sticky physical
            // failure.
            probe.begins.store(0, Ordering::SeqCst);
            probe.queries.store(0, Ordering::SeqCst);
            let ordinary = read_points_from_operations(
                store.independent_operations.as_ref().unwrap().as_ref(),
                &table,
                operation,
            )
            .await
            .unwrap_err();
            assert_eq!(
                probe.begins.load(Ordering::SeqCst),
                1,
                "permanent ordinary error never retries"
            );
            assert_eq!(
                probe.queries.load(Ordering::SeqCst),
                4,
                "failed cold metadata is not published"
            );
            probe.begins.store(0, Ordering::SeqCst);
            probe.queries.store(0, Ordering::SeqCst);
            let terminal = read_points(&store, &table, operation).await.unwrap_err();
            assert_eq!(
                probe.begins.load(Ordering::SeqCst),
                1,
                "permanent terminal error never retries"
            );
            assert_eq!(probe.queries.load(Ordering::SeqCst), 3);
            assert_eq!(
                format!("{:?}", ordinary.as_ref()),
                format!("{:?}", terminal.as_ref()),
                "ordinary and warm {operation} error"
            );
            assert!(
                matches!(
                    terminal.as_ref(),
                    storage_types::StorageEnum::InternalServerError { .. }
                ),
                "{operation}: {terminal:?}"
            );
            println!(
                "N7_MULTI_CORRUPT operation={operation} ordinary={ordinary:?} \
                 terminal={terminal:?}"
            );
        }
        drop(admin);
        connection.await.unwrap();
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_candidate_execution_schema_failure_and_later_stronger_rejection_when_public_multi_read_runs_then_no_fresh_recovery_occurs()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(SchemaRace::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        for name in ["aa_schema", "zz_capacity"] {
            let table = storage_types::TableName::new(name);
            store.create_hash_table(table.clone()).await;
            store.operations.put_item(storage_types::PutItemRequest::try_from(json!({
                "TableName": table, "Item": {"pk": {"S": "customer-1"}}
            })).unwrap()).await.unwrap();
            get(&store, &table).await.unwrap();
        }
        let (admin, connection) = tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls).await.unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap(); });
        let physical: String = admin.query_one("SELECT 'table_' || replace(id, '-', '') FROM tables WHERE table_name = 'aa_schema'", &[]).await.unwrap().get(0);
        let healthy: String = admin.query_one("SELECT 'table_' || replace(id, '-', '') FROM tables WHERE table_name = 'zz_capacity'", &[]).await.unwrap().get(0);
        // A view keeps preparation valid, then raises a genuine candidate
        // SQLSTATE after the authority prefix has actually executed.
        admin.batch_execute(&format!("CREATE FUNCTION native_public_schema_failure() RETURNS bytea LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION USING ERRCODE = ''42703'', MESSAGE = ''test missing column''; END'; ALTER TABLE \"{physical}\" RENAME TO retired_schema_item; CREATE VIEW \"{physical}\" AS SELECT pk, native_public_schema_failure() AS item_value FROM retired_schema_item")).await.unwrap();
        for (expected, error) in [
            (&[storage_types::AttemptKind::Capacity, storage_types::AttemptKind::TransactionBytes][..], StorageError::from(storage_types::AttemptFailure::CAPACITY)),
            (&[storage_types::AttemptKind::Unavailable, storage_types::AttemptKind::Deadline][..], StorageError::from(storage_types::AttemptFailure::Unavailable(storage_types::NativeUnavailable::WriterQueueFull))),
        ] {
        for operation in ["batch", "transact"] {
            probe.queries.store(0, Ordering::SeqCst);
            probe.begins.store(0, Ordering::SeqCst);
            *probe.query_failure.lock().unwrap() = Some((format!("FROM \"{healthy}\""), error.clone()));
            let result = if operation == "batch" {
                store.operations.batch_get_item(serde_json::from_value(json!({"RequestItems": {
                    "aa_schema": {"Keys": [{"pk": {"S": "customer-1"}}]},
                    "zz_capacity": {"Keys": [{"pk": {"S": "customer-1"}}]}
                }})).unwrap()).await.map(|_| ())
            } else {
                store.operations.transact_get_items(serde_json::from_value(json!({"TransactItems": [
                    {"Get": {"TableName": "aa_schema", "Key": {"pk": {"S": "customer-1"}}}},
                    {"Get": {"TableName": "zz_capacity", "Key": {"pk": {"S": "customer-1"}}}}
                ]})).unwrap()).await.map(|_| ())
            };
            assert!(result.unwrap_err().attempt_kind().is_some_and(|kind| expected.contains(&kind)));
            assert!(probe.query_failure.lock().unwrap().is_none(), "later independent query reached its capacity fault");
            assert_eq!(probe.begins.load(Ordering::SeqCst), 1, "no retry or Fresh attempt");
            assert_eq!(probe.queries.load(Ordering::SeqCst), 1, "only actual prefix completes; no Fresh stamp read");
        }
        }
        drop(admin);
        connection.await.unwrap();
    }).catch_unwind().await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_warm_point_writes_when_candidate_schema_fails_then_recovery_preserves_native_errors_and_unknown_settlement()
 {
    for operation in ["put", "update", "delete"] {
        for case in [
            SchemaRaceCase::MissingTable,
            SchemaRaceCase::MissingColumn,
            SchemaRaceCase::RecreatedTable,
            SchemaRaceCase::RecreatedAfterConflict,
        ] {
            run_point_race(case, operation).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_warm_bulk_writes_when_candidate_schema_fails_then_recovery_preserves_native_errors_and_unknown_settlement()
 {
    for operation in ["batch_write", "transact_write"] {
        for case in [
            SchemaRaceCase::MissingTable,
            SchemaRaceCase::MissingColumn,
            SchemaRaceCase::RecreatedTable,
            SchemaRaceCase::RecreatedAfterConflict,
        ] {
            run_point_race(case, operation).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_token_receipt_when_relation_is_broken_behind_catalog_then_second_fresh_miss_is_internal()
 {
    use storage_provider::DynamoOperations as _;
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    for (execution_failure, mismatch, expired) in [
        (false, false, false),
        (false, true, false),
        (true, false, false),
        (true, true, false),
        (false, false, true),
        (true, false, true),
    ] {
        let probe = Arc::new(SchemaRace::default());
        let store = TestStore::new_with_native_probes(
            probe.clone(),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            probe.clone(),
        )
        .await;
        let outcome = std::panic::AssertUnwindSafe(async {
            let table = store.create_standard_table().await;
            let token = uuid::Uuid::new_v4().to_string();
            let mut request = json!({"ClientRequestToken": token, "TransactItems": [{"Put": {
                "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "original"}}
            }}]});
            store
                .independent_operations
                .as_ref()
                .unwrap()
                .transact_write_items(serde_json::from_value(request.clone()).unwrap())
                .await
                .unwrap();
            // A separate native pool keeps the replay statement cold;
            // cloning SqlStore would share its prepared-statement cache.
            let backend = sql_backend::postgres::PostgresBackend::initialize(
                &store.postgres_dsn(),
                4,
                false,
                storage_types::AttemptDeadlines::default(),
            )
            .await
            .unwrap()
            .with_fault_probe(probe.clone());
            let replay_operations = sql_backend::SqlStore::initialize_schema(
                backend,
                storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
                true,
            )
            .await
            .unwrap();
            read_points_from_operations(&replay_operations, &table, "get")
                .await
                .unwrap();
            if mismatch {
                request["TransactItems"][0]["Put"]["Item"]["value"]["S"] = json!("changed");
            }
            let (admin, connection) =
                tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                    .await
                    .unwrap();
            let connection = tokio::spawn(async move {
                connection.await.unwrap();
            });
            let physical: String = admin
                .query_one(
                    "SELECT table_name FROM information_schema.columns WHERE table_schema = \
                     current_schema() AND column_name = 'item_value'",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            let original_item: Vec<u8> = admin
                .query_one(&format!("SELECT item_value FROM \"{physical}\""), &[])
                .await
                .unwrap()
                .get(0);
            probe.queries.store(0, Ordering::SeqCst);
            probe.begins.store(0, Ordering::SeqCst);
            probe.armed.store(true, Ordering::SeqCst);
            let (result, ()) = tokio::join!(
                replay_operations.transact_write_items(serde_json::from_value(request).unwrap()),
                async {
                    tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        probe.arrived.notified(),
                    )
                    .await
                    .unwrap();
                    if execution_failure {
                        admin
                            .batch_execute(&format!(
                                "CREATE FUNCTION native_bulk_schema_failure() RETURNS bytea \
                                 LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION USING ERRCODE = \
                                 ''42703'', MESSAGE = ''test missing column''; END'; ALTER TABLE \
                                 \"{physical}\" RENAME TO retired_bulk_receipt_item; CREATE VIEW \
                                 \"{physical}\" AS SELECT pk, native_bulk_schema_failure() AS \
                                 item_value, __aux_provenance, __aux_ttl_epoch FROM \
                                 retired_bulk_receipt_item"
                            ))
                            .await
                            .unwrap();
                    } else {
                        admin
                            .batch_execute(&format!("DROP TABLE \"{physical}\""))
                            .await
                            .unwrap();
                    }
                    if expired {
                        tokio::time::pause();
                        tokio::time::advance(std::time::Duration::from_secs(16)).await;
                        tokio::time::resume();
                    }
                    probe.released.notify_one();
                }
            );
            let case = format!(
                "token receipt execution_failure={execution_failure} mismatch={mismatch} \
                 expired={expired}"
            );
            if expired {
                assert!(
                    result
                        .expect_err(&case)
                        .attempt_kind()
                        .is_some_and(storage_types::AttemptKind::is_unavailable),
                    "{case}"
                );
                assert_eq!(
                    probe.begins.load(Ordering::SeqCst),
                    0,
                    "unexecuted schema wave has no BEGIN or locked receipt recovery"
                );
            } else {
                // The receipt of an aborted snapshot is never lent; the
                // relation stays broken, so the fresh try misses again.
                assert!(
                    matches!(
                        result.expect_err(&case).as_ref(),
                        storage_types::StorageEnum::InternalServerError { message }
                            if message == "fresh attempt requested another fresh validation"
                    ),
                    "{case}"
                );
            }
            if execution_failure {
                let item: Vec<u8> = admin
                    .query_one("SELECT item_value FROM retired_bulk_receipt_item", &[])
                    .await
                    .unwrap()
                    .get(0);
                assert_eq!(
                    item, original_item,
                    "receipt recovery must preserve the original stored image"
                );
            }
            assert_eq!(
                probe.token_only_waves.load(Ordering::SeqCst),
                0,
                "schema recovery reads catalog stamps only, never the receipt alone"
            );
            println!(
                "N9_RECEIPT_SCHEMA execution_failure={execution_failure} mismatch={mismatch} \
                 expired={expired} queries={} begins={}",
                probe.queries.load(Ordering::SeqCst),
                probe.begins.load(Ordering::SeqCst)
            );
            drop(admin);
            connection.await.unwrap();
        })
        .catch_unwind()
        .await;
        store.finish().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_recreated_table_with_completed_token_when_relation_is_dropped_then_fresh_snapshot_receipt_answers()
 {
    use storage_provider::DynamoOperations as _;
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(SchemaRace::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        let request = json!({"ClientRequestToken": uuid::Uuid::new_v4().to_string(), "TransactItems": [{"Put": {
            "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "original"}}
        }}]});
        store
            .independent_operations
            .as_ref()
            .unwrap()
            .transact_write_items(serde_json::from_value(request.clone()).unwrap())
            .await
            .unwrap();
        // A separate native pool keeps the replay statement cold.
        let backend = sql_backend::postgres::PostgresBackend::initialize(
            &store.postgres_dsn(),
            4,
            false,
            storage_types::AttemptDeadlines::default(),
        )
        .await
        .unwrap()
        .with_fault_probe(probe.clone());
        let replay_operations = sql_backend::SqlStore::initialize_schema(
            backend,
            storage_provider::StorageBackend::Postgres.default_initial_stream_lineages(),
            true,
        )
        .await
        .unwrap();
        read_points_from_operations(&replay_operations, &table, "get")
            .await
            .unwrap();
        let (admin, connection) =
            tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                .await
                .unwrap();
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let physical: String = admin
            .query_one(
                "SELECT table_name FROM information_schema.columns WHERE table_schema = \
                 current_schema() AND column_name = 'item_value'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        probe.queries.store(0, Ordering::SeqCst);
        probe.begins.store(0, Ordering::SeqCst);
        probe.armed.store(true, Ordering::SeqCst);
        let (result, ()) = tokio::join!(
            replay_operations.transact_write_items(serde_json::from_value(request).unwrap()),
            async {
                tokio::time::timeout(std::time::Duration::from_secs(10), probe.arrived.notified())
                    .await
                    .unwrap();
                admin
                    .batch_execute(&format!("DROP TABLE \"{physical}\""))
                    .await
                    .unwrap();
                let writer = store.independent_operations.as_ref().unwrap();
                writer
                    .delete_table(storage_types::DeleteTableRequest {
                        table_name: table.clone(),
                    })
                    .await
                    .unwrap();
                writer
                    .create_table(
                        serde_json::from_value(json!({
                            "TableName": table,
                            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                            "BillingMode": "PAY_PER_REQUEST"
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                writer
                    .put_item(
                        storage_types::PutItemRequest::try_from(json!({
                            "TableName": table,
                            "Item": {"pk": {"S": "customer-1"}, "value": {"S": "replacement"}}
                        }))
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                probe.released.notify_one();
            }
        );
        // The retry reads the receipt in a fresh snapshot; the receipt arm
        // answers before the identity confirmation.
        let response = result.unwrap_or_else(|error| panic!("receipt answer expected: {error:?}"));
        assert_eq!(serde_json::to_value(response).unwrap(), json!({}));
        let persisted = read_points_from_operations(
            store.independent_operations.as_ref().unwrap().as_ref(),
            &table,
            "get",
        )
        .await
        .unwrap();
        assert_eq!(
            persisted,
            json!({"Item": {"pk": {"S": "customer-1"}, "value": {"S": "replacement"}}}),
            "the receipt answered; the transaction did not apply again"
        );
        drop(admin);
        connection.await.unwrap();
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_completed_receipt_when_observation_has_permanent_or_capacity_failure_then_receipt_cannot_mask_it()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    for failure in [
        StorageError::internal("permanent grouped item read failure"),
        storage_types::AttemptFailure::CAPACITY.into(),
    ] {
        let probe = Arc::new(SchemaRace::default());
        let store = TestStore::new_with_native_probes(
            probe.clone(),
            #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
            probe.clone(),
        )
        .await;
        let outcome = std::panic::AssertUnwindSafe(async {
            let table = store.create_standard_table().await;
            let request = json!({"ClientRequestToken": uuid::Uuid::new_v4().to_string(), "TransactItems": [{"Put": {
                "TableName": table, "Item": {"pk": {"S": "customer-1"}, "value": {"S": "original"}}
            }}]});
            store.operations.transact_write_items(serde_json::from_value(request.clone()).unwrap()).await.unwrap();
            let (admin, connection) = tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls).await.unwrap();
            let connection = tokio::spawn(async move { connection.await.unwrap(); });
            let physical: String = admin.query_one("SELECT table_name FROM information_schema.columns WHERE table_schema = current_schema() AND column_name = 'item_value'", &[]).await.unwrap().get(0);
            *probe.query_failure.lock().unwrap() = Some((physical, failure.clone()));
            let error = store.operations.transact_write_items(serde_json::from_value(request).unwrap()).await.unwrap_err();
            if matches!(failure.attempt_kind(), Some(storage_types::AttemptKind::Capacity | storage_types::AttemptKind::TransactionBytes)) {
                assert!(matches!(error.attempt_kind(), Some(storage_types::AttemptKind::Capacity | storage_types::AttemptKind::TransactionBytes)));
            } else {
                assert!(std::ptr::eq(error.as_ref(), failure.as_ref()), "the original retained permanent cause must win over a completed matching receipt: {error:?}");
            }
            assert!(probe.query_failure.lock().unwrap().is_none());
            let persisted = get(&store, &table).await.unwrap();
            assert_eq!(persisted, json!({"Item": {"pk": {"S": "customer-1"}, "value": {"S": "original"}}}));
            drop(admin);
            connection.await.unwrap();
        }).catch_unwind().await;
        store.finish().await;
        if let Err(panic) = outcome {
            std::panic::resume_unwind(panic);
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_warm_ranges_when_candidate_schema_fails_then_existing_fresh_recovery_preserves_error_and_identity()
 {
    for operation in ["query", "scan"] {
        for case in [
            SchemaRaceCase::MissingTable,
            SchemaRaceCase::MissingColumn,
            SchemaRaceCase::RecreatedTable,
            SchemaRaceCase::RecreatedAfterConflict,
        ] {
            run_point_race(case, operation).await;
        }
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_corrupt_scan_image_when_filter_is_malformed_then_stored_error_has_priority_in_cold_and_warm_reads()
 {
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let store = TestStore::new().await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = store.create_standard_table().await;
        store
            .operations
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        read_points(&store, &table, "scan").await.unwrap();
        let (admin, connection) =
            tokio_postgres::connect(&store.postgres_dsn(), tokio_postgres::NoTls)
                .await
                .unwrap();
        let connection = tokio::spawn(async move {
            connection.await.unwrap();
        });
        let physical: String = admin
            .query_one(
                "SELECT table_name FROM information_schema.columns WHERE table_schema = \
                 current_schema() AND column_name = 'item_value'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        admin
            .batch_execute(&format!(
                "UPDATE \"{physical}\" SET item_value = 'corrupt'::bytea"
            ))
            .await
            .unwrap();
        let mut errors = Vec::new();
        for operations in [
            store.operations.as_ref(),
            store.independent_operations.as_ref().unwrap().as_ref(),
        ] {
            for filtered in [false, true] {
                let scan = if filtered {
                    json!({
                        "TableName": table,
                        "FilterExpression": "pk = :v",
                        "ExpressionAttributeValues": {":v": {"S": "customer-1"}}
                    })
                } else {
                    json!({"TableName": table})
                };
                let request = storage_types::ScanRequest::try_from(scan).unwrap();
                storage_types::DynamoRequestValidate::validate_for_dynamodb(&request).unwrap();
                let error = operations.scan(request).await.unwrap_err();
                assert!(
                    matches!(
                        error.as_ref(),
                        storage_types::StorageEnum::InternalServerError { .. }
                    ),
                    "valid scan reads the corrupt image: {error:?}"
                );
                errors.push(format!("{:?}", error.as_ref()));
            }
        }
        assert!(errors.iter().all(|error| *error == errors[0]));
        // Validation rejects reversed BETWEEN bounds before any read, so the
        // request never reaches the corrupt image and no stored error appears.
        let message = storage_types::ScanRequest::try_from(json!({
            "TableName": table,
            "FilterExpression": "pk BETWEEN :upper AND :lower",
            "ExpressionAttributeValues": {":upper": {"N": "5"}, ":lower": {"N": "4"}}
        }))
        .unwrap_err();
        assert!(
            message
                .to_string()
                .contains("The BETWEEN operator requires upper bound"),
            "reversed bounds are rejected by validation: {message}"
        );
        drop(admin);
        connection.await.unwrap();
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_expired_parent_when_candidate_schema_aborts_then_no_fresh_recovery_or_mutation_occurs()
 {
    for operation in ["put", "update", "delete", "batch_write", "transact_write"] {
        for case in [SchemaRaceCase::ExpiredRecovery] {
            run_point_race(case, operation).await;
        }
    }
}

/// Records the BEGIN text that rides as the first statement of a first wave.
#[derive(Default)]
struct BeginTexts {
    texts: Mutex<Vec<String>>,
}

impl sql_backend::SqlFaultProbe for BeginTexts {
    fn fail(&self, _: sql_backend::SqlFaultCall<'_>) -> Option<StorageError> {
        None
    }

    fn before_wave<'a>(
        &'a self,
        statements: &'a [sql_backend::SqlWaveStatement<'a>],
    ) -> futures::future::BoxFuture<'a, StorageResult<()>> {
        Box::pin(async move {
            let mut texts = self.texts.lock().unwrap();
            for statement in statements {
                if statement.sql.starts_with("START TRANSACTION") {
                    texts.push(statement.sql.to_owned());
                }
            }
            Ok(())
        })
    }
}

#[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
impl kv_backend::KvFaultProbe for BeginTexts {
    fn fail(&self, _: kv_backend::KvFaultCall) -> Option<StorageError> {
        None
    }
}

/// The recorded texts since the last call contain `expected`; every other
/// one is the `Snapshot` text (a cold ReadOnly prepare or catalog attempt may
/// precede the caller's own attempt).
fn assert_begin_texts(probe: &BeginTexts, row: &str, expected: &str) {
    const SNAPSHOT: &str = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY";
    let texts = std::mem::take(&mut *probe.texts.lock().unwrap());
    assert!(
        texts.iter().any(|text| text == expected),
        "{row}: expected BEGIN text {expected:?} among {texts:?}"
    );
    for text in &texts {
        assert!(
            text == expected || text == SNAPSHOT,
            "{row}: unexpected BEGIN text {text:?} (expected {expected:?}) among {texts:?}"
        );
    }
}

#[tokio::test]
#[ignore = "requires owned native fixture"]
async fn given_each_attempt_scope_when_public_operations_run_then_postgres_begin_text_is_unchanged()
{
    const SERIALIZABLE: &str = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    const SERIALIZABLE_DATA: &str = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE; LOCK TABLE \
                                     sys_system_stream_scope IN ROW EXCLUSIVE MODE";
    const DATA_WRITE: &str = "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                              sys_system_stream_scope IN ROW EXCLUSIVE MODE";
    const TOPOLOGY: &str = "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                            sys_system_stream_scope IN SHARE ROW EXCLUSIVE MODE";
    if TestStore::selected_backend() != "postgres" {
        return;
    }
    let probe = Arc::new(BeginTexts::default());
    let store = TestStore::new_with_native_probes(
        probe.clone(),
        #[cfg(any(feature = "rocksdb", feature = "foundationdb"))]
        probe.clone(),
    )
    .await;
    let outcome = std::panic::AssertUnwindSafe(async {
        let table = storage_types::TableName::new("begin_text_table");
        let clear = || probe.texts.lock().unwrap().clear();

        clear();
        store
            .operations
            .create_table(
                serde_json::from_value(json!({
                    "TableName": table,
                    "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                    "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                    "BillingMode": "PAY_PER_REQUEST"
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !probe
                .texts
                .lock()
                .unwrap()
                .iter()
                .any(|text| text.contains("LOCK TABLE")),
            "CreateTable takes no topology fence"
        );
        assert_begin_texts(&probe, "CreateTable", SERIALIZABLE);

        clear();
        store
            .operations
            .put_item(
                storage_types::PutItemRequest::try_from(json!({
                    "TableName": table, "Item": {"pk": {"S": "customer-1"}}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_begin_texts(&probe, "PutItem", DATA_WRITE);

        clear();
        store
            .operations
            .update_time_to_live(
                serde_json::from_value(json!({
                    "TableName": table,
                    "TimeToLiveSpecification": {"AttributeName": "ttl", "Enabled": true}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        assert_begin_texts(&probe, "UpdateTimeToLive", SERIALIZABLE_DATA);

        clear();
        store
            .stream_operations
            .create_stream(
                storage_types::UserStreamName::new("begin-text-stream"),
                None,
                stream_provider::StreamPartitioningMode::Single,
            )
            .await
            .unwrap();
        assert_begin_texts(&probe, "CreateStream", SERIALIZABLE_DATA);

        let scope = store.stream_operations.system_stream_scope().await.unwrap();
        let parent = store
            .stream_operations
            .system_stream_lineage_catalog(scope.clone())
            .await
            .unwrap()
            .into_iter()
            .find(|lineage| !lineage.sealed() && lineage.parent_lineage_id().is_none())
            .expect("active root lineage");
        let child = |name: &str| {
            stream_provider::SystemStreamSplitChild::try_new(
                stream_provider::SystemStreamLineage::try_new(
                    name,
                    format!("{name}-lineage"),
                    Some(parent.lineage_id().as_str().to_owned()),
                    false,
                )
                .unwrap(),
                1,
            )
            .unwrap()
        };
        let request = stream_provider::SystemStreamSplitRequest::try_new(
            scope,
            parent.clone(),
            [child("begin-left"), child("begin-right")],
            stream_provider::SystemStreamSplitOperationId::try_new("begin-text-split").unwrap(),
        )
        .unwrap();
        clear();
        store
            .stream_operations
            .split_system_stream(request)
            .await
            .unwrap();
        assert_begin_texts(&probe, "split_system_stream", TOPOLOGY);

        clear();
        store
            .operations
            .delete_table(storage_types::DeleteTableRequest {
                table_name: table.clone(),
            })
            .await
            .unwrap();
        assert!(
            !probe
                .texts
                .lock()
                .unwrap()
                .iter()
                .any(|text| text.contains("LOCK TABLE")),
            "DeleteTable takes no topology fence"
        );
        assert_begin_texts(&probe, "DeleteTable", SERIALIZABLE);
    })
    .catch_unwind()
    .await;
    store.finish().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}
