use std::sync::Arc;

use super::PostgresBackend;
use crate::{SqlAttempt, SqlBackend, SqlStatement};

pub(crate) struct NativeFixture {
    pub(super) backend: Arc<PostgresBackend>,
    pub(super) admin: tokio_postgres::Client,
    pub(super) schema: String,
    #[cfg(feature = "fault-injection")]
    scoped_dsn: String,
}

impl NativeFixture {
    pub(crate) async fn open() -> Self {
        let dsn = std::env::var("TEST_POSTGRES_DSN").expect("TEST_POSTGRES_DSN");
        let (admin, connection) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
            .await
            .expect("connect native fixture");
        tokio::spawn(async move { connection.await.expect("native fixture connection") });
        let schema = format!("native_attempt_{}", uuid::Uuid::new_v4().simple());
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("owned schema");
        let (base, query) = dsn.split_once('?').unwrap_or((&dsn, ""));
        let retained = query
            .split('&')
            .filter(|pair| !pair.is_empty() && pair.split('=').next() != Some("options"))
            .collect::<Vec<_>>()
            .join("&");
        let mut scoped_dsn = format!("{base}?options=-csearch_path%3D{schema}");
        if !retained.is_empty() {
            scoped_dsn.push('&');
            scoped_dsn.push_str(&retained);
        }
        let backend = Arc::new(
            PostgresBackend::initialize(&scoped_dsn, 1, false)
                .await
                .expect("native backend"),
        );
        backend
            .with_schema_attempt(|attempt| {
                Box::pin(async move {
                    attempt
                        .execute(SqlStatement::new(
                            "CREATE TABLE sys_infrastructure_format (slot bigint NOT NULL)",
                            vec![],
                        ))
                        .await?;
                    attempt
                        .execute(SqlStatement::new(
                            "CREATE TABLE native_commit_probe (value bigint NOT NULL)",
                            vec![],
                        ))
                        .await
                })
            })
            .await
            .expect("native fixture tables");
        Self {
            backend,
            admin,
            schema,
            #[cfg(feature = "fault-injection")]
            scoped_dsn,
        }
    }

    #[cfg(feature = "fault-injection")]
    pub(crate) async fn open_independent_backend(&self) -> PostgresBackend {
        PostgresBackend::initialize(&self.scoped_dsn, 1, false)
            .await
            .expect("independent backend")
    }

    pub(super) async fn count_records(&self) -> i64 {
        self.admin
            .query_one(
                &format!("SELECT count(*) FROM {}.native_commit_probe", self.schema),
                &[],
            )
            .await
            .expect("count committed rows")
            .get(0)
    }

    pub(crate) async fn close(self) {
        drop(self.backend);
        self.admin
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await
            .expect("drop owned schema");
    }
}
