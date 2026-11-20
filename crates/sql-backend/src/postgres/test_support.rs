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
        eprintln!("native_fixture_schema={schema} phase=creating");
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await
            .expect("owned schema");
        eprintln!("native_fixture_schema={schema} phase=created");
        let (base, query) = dsn.split_once('?').unwrap_or((&dsn, ""));
        let mut pairs = query
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if let Some(options) = pairs.iter_mut().find(|pair| pair.starts_with("options=")) {
            options.push_str(&format!("%20-csearch_path%3D{schema}"));
        } else {
            pairs.push(format!("options=-csearch_path%3D{schema}"));
        }
        let scoped_dsn = format!("{base}?{}", pairs.join("&"));
        let backend = Arc::new(
            PostgresBackend::initialize(
                &scoped_dsn,
                1,
                false,
                storage_types::AttemptDeadlines::default(),
            )
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
        PostgresBackend::initialize(
            &self.scoped_dsn,
            1,
            false,
            storage_types::AttemptDeadlines::default(),
        )
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
