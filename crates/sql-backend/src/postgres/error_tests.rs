use storage_common::diagnostics::NativeOperation;
use storage_types::{StorageEnum, StorageError, context::WrappedError};

use super::error::{PostgresCallError, collect_pipeline, map_driver_error};

#[test]
fn given_successful_pipeline_when_collected_then_preserve_statement_order() {
    assert_eq!(
        collect_pipeline(vec![Ok(3), Ok(1), Ok(2)], NativeOperation::SqlExecute).unwrap(),
        vec![3, 1, 2]
    );
    assert!(
        collect_pipeline::<u64>(Vec::new(), NativeOperation::SqlExecute)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn given_local_failure_when_collected_then_preserve_error() {
    let error = collect_pipeline::<u64>(
        vec![
            Ok(1),
            Err(PostgresCallError::Local(StorageError::internal(
                "decode failed",
            ))),
        ],
        NativeOperation::SqlExecute,
    )
    .unwrap_err();
    assert!(
        matches!(error.to_enum(), StorageEnum::InternalServerError { message } if message == "decode failed")
    );
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_aborted_pipeline_when_secondary_error_arrives_first_then_preserve_origin() {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    for (code, retryable) in [("40001", true), ("40P01", true), ("23505", false)] {
        client.batch_execute("BEGIN").await.unwrap();
        let origin = client
            .batch_execute(&format!(
                "DO $$ BEGIN RAISE EXCEPTION 'test failure' USING ERRCODE = '{code}'; END $$"
            ))
            .await
            .unwrap_err();
        let secondary = client.batch_execute("SELECT 1").await.unwrap_err();
        assert_eq!(secondary.code().unwrap().code(), "25P02");
        client.batch_execute("ROLLBACK").await.unwrap();
        let error = collect_pipeline::<u64>(
            vec![Err(secondary.into()), Err(origin.into())],
            NativeOperation::SqlExecute,
        )
        .unwrap_err();
        assert_eq!(error.is_retryable_write(), retryable);
        match error.to_enum() {
            StorageEnum::TransactionConflict { message }
            | StorageEnum::InternalServerError { message } => assert!(message.contains(code)),
            other => panic!("unexpected error: {other:?}"),
        }
    }
    client.batch_execute("BEGIN").await.unwrap();
    let _ = client.batch_execute("SELECT 1 / 0").await.unwrap_err();
    let secondary = client.batch_execute("SELECT 1").await.unwrap_err();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        !collect_pipeline::<u64>(vec![Err(secondary.into())], NativeOperation::SqlExecute)
            .unwrap_err()
            .is_retryable_write()
    );
    drop(client);
    connection.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_concurrent_schema_creation_when_catalog_uniqueness_aborts_then_only_catalog_conflict_is_retryable()
 {
    let fixture = super::test_support::NativeFixture::open().await;
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    let connection = tokio::spawn(connection);
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    let create = format!(
        "CREATE TABLE IF NOT EXISTS {}.ddl_conflict_probe (value bigint)",
        fixture.schema
    );
    fixture.admin.batch_execute("BEGIN").await.unwrap();
    fixture.admin.batch_execute(&create).await.unwrap();
    let contender = tokio::spawn(async move { client.batch_execute(&create).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            fixture
                .admin
                .simple_query("SELECT pg_stat_clear_snapshot()")
                .await
                .unwrap();
            let blocked: bool = fixture
                .admin
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND \
                     wait_event_type = 'Lock')",
                    &[&pid],
                )
                .await
                .unwrap()
                .get(0);
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture.admin.batch_execute("COMMIT").await.unwrap();
    let catalog_error = contender.await.unwrap().unwrap_err();
    let db_error = catalog_error.as_db_error().unwrap();
    assert_eq!(db_error.schema(), Some("pg_catalog"));
    assert_eq!(db_error.table(), Some("pg_type"));
    assert_eq!(db_error.constraint(), Some("pg_type_typname_nsp_index"));
    let catalog_error = map_driver_error(catalog_error);

    let customer_error = fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {}.customer_constraint_probe (value bigint CONSTRAINT \
             pg_type_typname_nsp_index UNIQUE); INSERT INTO {}.customer_constraint_probe VALUES \
             (1), (1)",
            fixture.schema, fixture.schema,
        ))
        .await
        .unwrap_err();
    let customer_error = map_driver_error(customer_error);
    fixture.close().await;
    connection.await.unwrap().unwrap();
    assert!(
        catalog_error.is_retryable_write(),
        "catalog DDL race must permit a fresh attempt"
    );
    assert!(
        !customer_error.is_retryable_write(),
        "customer uniqueness must remain terminal even with a catalog-like constraint name"
    );
}
