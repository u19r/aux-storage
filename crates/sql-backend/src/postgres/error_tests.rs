use storage_common::{
    diagnostics::NativeOperation,
    native_outcome::{NativeCommitError, classify_commit},
};
use storage_types::{StorageEnum, StorageError, context::WrappedError};

use super::error::{PostgresCallError, collect_pipeline, commit_error, map_driver_error};

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
        assert_eq!(error.class().is_retryable(), retryable);
        assert!(error.to_string().contains(code), "{error}");
    }
    client.batch_execute("BEGIN").await.unwrap();
    let _ = client.batch_execute("SELECT 1 / 0").await.unwrap_err();
    let secondary = client.batch_execute("SELECT 1").await.unwrap_err();
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        !collect_pipeline::<u64>(vec![Err(secondary.into())], NativeOperation::SqlExecute)
            .unwrap_err()
            .class()
            .is_retryable()
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
    let catalog_error = map_driver_error(&catalog_error);

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
    let customer_error = map_driver_error(&customer_error);
    fixture.close().await;
    connection.await.unwrap().unwrap();
    assert!(
        catalog_error.class().is_retryable(),
        "catalog DDL race must permit a fresh attempt"
    );
    assert!(
        !customer_error.class().is_retryable(),
        "customer uniqueness must remain terminal even with a catalog-like constraint name"
    );
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_serialization_failure_at_commit_when_classified_then_known_conflict() {
    let fixture = super::test_support::NativeFixture::open().await;
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let mut sessions = Vec::new();
    for _ in 0..2 {
        let (client, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        sessions.push((client, tokio::spawn(connection)));
    }
    let table = format!("{}.ssi_probe", fixture.schema);
    fixture
        .admin
        .batch_execute(&format!(
            "CREATE TABLE {table} (id bigint PRIMARY KEY, value bigint NOT NULL); INSERT INTO \
             {table} VALUES (1, 0), (2, 0)"
        ))
        .await
        .unwrap();
    // Write skew: each session reads both rows and updates a different one.
    for (index, (client, _)) in sessions.iter().enumerate() {
        client
            .batch_execute("BEGIN ISOLATION LEVEL SERIALIZABLE")
            .await
            .unwrap();
        client
            .query(&format!("SELECT sum(value) FROM {table}"), &[])
            .await
            .unwrap();
        client
            .batch_execute(&format!(
                "UPDATE {table} SET value = 1 WHERE id = {}",
                index + 1
            ))
            .await
            .unwrap();
    }
    sessions[0].0.batch_execute("COMMIT").await.unwrap();
    let error = sessions[1]
        .0
        .batch_execute("COMMIT")
        .await
        .expect_err("the second committer is the serialization victim");
    assert_eq!(error.code().unwrap().code(), "40001");
    let NativeCommitError::Conflict(cause) = commit_error(&error) else {
        panic!("a serialization failure at COMMIT is a known conflict");
    };
    assert_eq!(cause.phase, storage_types::NativeCallPhase::Commit);
    let classified = classify_commit(true, commit_error(&error));
    assert!(!classified.is_commit_unknown(), "{classified}");
    assert_eq!(
        classified.attempt_kind(),
        Some(storage_types::AttemptKind::Conflict)
    );
    drop(sessions);
    fixture.close().await;
}
