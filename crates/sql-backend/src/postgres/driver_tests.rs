use std::time::Duration;

use storage_types::{AttemptDeadlines, AttemptMode, AttemptPlan, AttemptScope, DeadlineClass};

use super::driver::PostgresTransaction;
use crate::{ReadProtection, SqlAttempt, SqlBackend, SqlStatement, SqlValue};

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_short_session_timeout_when_concurrent_index_build_is_longer_then_index_is_valid() {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
    let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    let schema = format!("ddl_session_{}", uuid::Uuid::new_v4().simple());
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; SET search_path TO {schema}; CREATE TABLE slow_probe (value \
             bigint NOT NULL); INSERT INTO slow_probe SELECT generate_series(1, 16); CREATE \
             FUNCTION slow_key(value bigint) RETURNS bigint IMMUTABLE LANGUAGE plpgsql AS $$ \
             BEGIN PERFORM pg_sleep(0.25); RETURN value; END $$"
        ))
        .await
        .unwrap();
    let separator = if dsn.contains('?') { '&' } else { '?' };
    let scoped = format!("{dsn}{separator}options=-csearch_path%3D{schema}");
    // Session statement timeout 2001 ms; the build sleeps 16 x 250 ms.
    let deadlines = AttemptDeadlines::try_new(
        [Duration::from_secs(1); DeadlineClass::COUNT],
        Duration::from_secs(1),
    )
    .unwrap();
    let backend = super::PostgresBackend::initialize(&scoped, 2, false, deadlines)
        .await
        .unwrap();
    backend
        .execute_autocommit_ddl(crate::SqlDdl {
            object: "slow_probe_key".to_owned(),
            guard: SqlStatement::new("SELECT true", vec![]),
            statement: SqlStatement::new(
                "CREATE INDEX CONCURRENTLY slow_probe_key ON slow_probe (slow_key(value))",
                vec![],
            ),
            lock_timeout: None,
        })
        .await
        .unwrap();
    let valid: bool = admin
        .query_one(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('slow_probe_key')",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(valid, "the long concurrent build must complete");
    let pooled = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let page = attempt
                        .query(
                            SqlStatement::new(
                                "SELECT (extract(epoch FROM \
                                 current_setting('statement_timeout')::interval) * 1000)::bigint",
                                vec![],
                            ),
                            ReadProtection::Snapshot,
                            storage_provider::CATALOG_READ_BYTES,
                        )
                        .await?;
                    match page.rows[0].value(crate::ColumnIndex::new(0))? {
                        SqlValue::Integer(value) => Ok(*value),
                        _ => Err(storage_types::StorageError::internal(
                            "statement timeout type",
                        )),
                    }
                })
            },
        )
        .await
        .unwrap();
    // Each BEGIN sets the remaining attempt budget (1 s class) over the session
    // value.
    assert!(
        pooled > 0 && pooled <= 1000,
        "the attempt carries its own statement deadline: {pooled}"
    );
    drop(backend);
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}

#[test]
fn given_each_mode_and_scope_then_postgres_transaction_matches_former_mapping() {
    const SNAPSHOT: &str = "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY";
    const SERIALIZABLE: &str = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE";
    const READ_COMMITTED: &str = "START TRANSACTION ISOLATION LEVEL READ COMMITTED";
    const SERIALIZABLE_DATA: &str = "START TRANSACTION ISOLATION LEVEL SERIALIZABLE; LOCK TABLE \
                                     sys_system_stream_scope IN ROW EXCLUSIVE MODE";
    const DATA_WRITE: &str = "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                              sys_system_stream_scope IN ROW EXCLUSIVE MODE";
    const TOPOLOGY: &str = "START TRANSACTION ISOLATION LEVEL READ COMMITTED; LOCK TABLE \
                            sys_system_stream_scope IN SHARE ROW EXCLUSIVE MODE";
    assert_eq!(PostgresTransaction::Snapshot.begin_sql(), SNAPSHOT);
    assert_eq!(PostgresTransaction::Serializable.begin_sql(), SERIALIZABLE);
    assert_eq!(
        PostgresTransaction::ReadCommitted.begin_sql(),
        READ_COMMITTED
    );
    assert_eq!(
        PostgresTransaction::SerializableData.begin_sql(),
        SERIALIZABLE_DATA
    );
    assert_eq!(PostgresTransaction::DataWrite.begin_sql(), DATA_WRITE);
    assert_eq!(PostgresTransaction::Topology.begin_sql(), TOPOLOGY);
    for mode in [
        AttemptMode::ReadOnly,
        AttemptMode::ReadWrite,
        AttemptMode::ReadWriteItems,
    ] {
        for scope in [
            AttemptScope::Default,
            AttemptScope::Schema,
            AttemptScope::Data,
            AttemptScope::ItemWrite,
            AttemptScope::Topology,
        ] {
            let expected = match (mode, scope) {
                (AttemptMode::ReadOnly, _) => PostgresTransaction::Snapshot,
                (AttemptMode::ReadWrite, AttemptScope::Default | AttemptScope::Schema) => {
                    PostgresTransaction::Serializable
                }
                (AttemptMode::ReadWriteItems, AttemptScope::Default | AttemptScope::Schema) => {
                    PostgresTransaction::ReadCommitted
                }
                (AttemptMode::ReadWrite, AttemptScope::Data | AttemptScope::ItemWrite) => {
                    PostgresTransaction::SerializableData
                }
                (AttemptMode::ReadWriteItems, AttemptScope::Data | AttemptScope::ItemWrite) => {
                    PostgresTransaction::DataWrite
                }
                (_, AttemptScope::Topology) => PostgresTransaction::Topology,
            };
            assert_eq!(
                PostgresTransaction::of(mode, scope).begin_sql(),
                expected.begin_sql(),
                "{mode:?} {scope:?}"
            );
        }
    }
}
