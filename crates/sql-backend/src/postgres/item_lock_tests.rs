use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use storage_types::{AttributeValue, KeyAttributes};
use tokio::sync::oneshot;

use crate::{
    AttemptMode, ColumnIndex, ReadProtection, SqlAttempt, SqlBackend, SqlDataAccess, SqlStatement,
    SqlStore, SqlValue, item_lock::item_lock_statement,
};

struct LockFixture {
    backend: Arc<super::PostgresBackend>,
    admin: tokio_postgres::Client,
    schema: String,
}

impl LockFixture {
    async fn load() -> Self {
        let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap();
        let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .unwrap();
        tokio::spawn(async move { connection.await.unwrap() });
        let schema = format!("item_lock_test_{}", uuid::Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .unwrap();
        let separator = if dsn.contains('?') { '&' } else { '?' };
        let dsn = format!("{dsn}{separator}options=-csearch_path%3D{schema}");
        let backend = super::PostgresBackend::initialize(&dsn, 4, false)
            .await
            .unwrap();
        let store = SqlStore::initialize_schema(backend).await.unwrap();
        let backend = Arc::clone(&store.backend);
        backend
            .with_attempt(AttemptMode::ReadWrite, |attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "CREATE TABLE item_lock_probe (value bigint NOT NULL)",
                            vec![],
                        ))
                        .await?;
                    attempt
                        .execute(SqlStatement::new(
                            "INSERT INTO item_lock_probe VALUES (0)",
                            vec![],
                        ))
                        .await
                })
            })
            .await
            .unwrap();
        Self {
            backend,
            admin,
            schema,
        }
    }

    async fn close(self) {
        drop(self.backend);
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .unwrap();
    }
}

fn lock_target() -> SqlStatement {
    let table = crate::test_support::table();
    let mut key = KeyAttributes::new();
    key.insert("pk", AttributeValue::S("absent-key".to_owned()));
    item_lock_statement(std::iter::once((&table, &key))).unwrap()
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_blocked_item_and_cold_cache_when_pipeline_runs_then_read_observes_lock_holder_commit()
 {
    let fixture = LockFixture::load().await;
    let (locked, acquired) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let backend = Arc::clone(&fixture.backend);
    let holder = tokio::spawn(async move {
        backend
            .with_data_attempt(
                AttemptMode::ReadWriteItems,
                SqlDataAccess::Write,
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                lock_target(),
                                ReadProtection::Snapshot,
                                NonZeroUsize::new(4096).unwrap(),
                            )
                            .await?;
                        locked.send(()).unwrap();
                        released.await.unwrap();
                        attempt
                            .execute(SqlStatement::new(
                                "UPDATE item_lock_probe SET value = 1",
                                vec![],
                            ))
                            .await
                    })
                },
            )
            .await
            .unwrap();
    });
    acquired.await.unwrap();
    let backend = Arc::clone(&fixture.backend);
    let mut reader = tokio::spawn(async move {
        backend
            .with_data_attempt(
                AttemptMode::ReadWriteItems,
                SqlDataAccess::Write,
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query_batch_protected(
                                vec![
                                    (lock_target(), ReadProtection::Snapshot),
                                    (
                                        SqlStatement::new(
                                            "SELECT value, \
                                             current_setting('transaction_isolation') FROM \
                                             item_lock_probe",
                                            vec![],
                                        ),
                                        ReadProtection::Snapshot,
                                    ),
                                ],
                                NonZeroUsize::new(4096).unwrap(),
                            )
                            .await
                    })
                },
            )
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut reader)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    holder.await.unwrap();
    let pages = reader.await.unwrap();
    assert!(matches!(
        pages[1].rows[0].value(ColumnIndex::new(0)).unwrap(),
        SqlValue::Integer(1)
    ));
    assert!(
        matches!(pages[1].rows[0].value(ColumnIndex::new(1)).unwrap(), SqlValue::Text(value) if value == "read committed")
    );
    fixture.close().await;
}

#[tokio::test]
#[ignore = "requires TEST_POSTGRES_DSN"]
async fn given_active_data_writer_when_split_starts_then_writers_coexist_and_split_drains_them() {
    let fixture = LockFixture::load().await;
    let (locked, acquired) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let backend = Arc::clone(&fixture.backend);
    let holder = tokio::spawn(async move {
        backend
            .with_data_attempt(
                AttemptMode::ReadWriteItems,
                SqlDataAccess::Write,
                |attempt| {
                    Box::pin(async move {
                        locked.send(()).unwrap();
                        released.await.unwrap();
                        attempt
                            .execute(SqlStatement::new(
                                "UPDATE item_lock_probe SET value = 1",
                                vec![],
                            ))
                            .await
                    })
                },
            )
            .await
            .unwrap();
    });
    acquired.await.unwrap();
    // A second ordinary writer must not wait for the first writer's fence.
    tokio::time::timeout(
        Duration::from_secs(2),
        fixture.backend.with_data_attempt(
            AttemptMode::ReadWriteItems,
            SqlDataAccess::Write,
            |attempt| {
                Box::pin(async move {
                    attempt
                        .query(
                            SqlStatement::new("SELECT 1::bigint", vec![]),
                            ReadProtection::Snapshot,
                            NonZeroUsize::new(4096).unwrap(),
                        )
                        .await
                })
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let backend = Arc::clone(&fixture.backend);
    let mut split = tokio::spawn(async move {
        backend
            .with_data_attempt(
                AttemptMode::ReadWrite,
                SqlDataAccess::Exclusive,
                |attempt| {
                    Box::pin(async move {
                        attempt
                            .query(
                                SqlStatement::new("SELECT value FROM item_lock_probe", vec![]),
                                ReadProtection::Snapshot,
                                NonZeroUsize::new(4096).unwrap(),
                            )
                            .await
                    })
                },
            )
            .await
            .unwrap()
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut split)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    holder.await.unwrap();
    let page = split.await.unwrap();
    assert!(matches!(
        page.rows[0].value(ColumnIndex::new(0)).unwrap(),
        SqlValue::Integer(1)
    ));
    fixture.close().await;
}
