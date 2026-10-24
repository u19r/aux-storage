//! Actual native constructors, with cleanup limited to fixture-owned targets.

use std::num::NonZeroUsize;
#[cfg(any(feature = "native-sqlite", feature = "native-postgres"))]
use std::sync::Arc;

use futures::future::BoxFuture;
use infrastructure_provider::{
    limits::InfrastructureLimits, physical_attempt::PhysicalRecordAttempt,
    physical_store::PhysicalRecordStore,
};
#[cfg(any(feature = "native-sqlite", feature = "native-postgres"))]
use sql_backend::SqlStore;
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult};

#[cfg(any(feature = "native-sqlite", feature = "native-postgres"))]
use crate::HarnessResult;

/// Recovery contracts bound execution, not provider latency. Keep the budget
/// explicit and shared by the parent and subprocess scheduler constructors;
/// fault probes still interrupt the actual native settlement boundaries.
pub fn scheduled_job_config() -> bg_job_store::ScheduledJobStoreConfig {
    std::time::Duration::from_secs(5).into()
}

pub fn limits() -> StorageResult<InfrastructureLimits> {
    let nonzero = |value| {
        NonZeroUsize::new(value).ok_or_else(|| StorageError::internal("invalid fixture limit"))
    };
    InfrastructureLimits::try_new(
        nonzero(1024)?,
        nonzero(4096)?,
        nonzero(16384)?,
        nonzero(64)?,
    )
}

/// A complete native callback using the ordinary catalog and operation
/// boundary.
pub async fn with_records<P, R, F>(store: &P, mode: AttemptMode, operation: F) -> StorageResult<R>
where
    P: DynamoPort + 'static,
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
    R: Send + 'static,
    F: for<'native, 'borrow> FnOnce(
            &'borrow mut P::Attempt<'native>,
        ) -> BoxFuture<'borrow, StorageResult<R>>
        + Send
        + 'static,
{
    store.with_dynamo_attempt(mode, operation).await
}

#[cfg(feature = "native-sqlite")]
pub async fn memory_sqlite() -> HarnessResult<SqlStore<sql_backend::sqlite::SqliteBackend>> {
    let backend = sql_backend::sqlite::SqliteBackend::initialize(":memory:").await?;
    let store = SqlStore::initialize_schema(backend).await?;
    store.initialize_records(limits()?).await?;
    Ok(store)
}

#[cfg(feature = "native-sqlite")]
pub struct FileSqlite {
    pub store: SqlStore<sql_backend::sqlite::SqliteBackend>,
    pub path: std::path::PathBuf,
    _directory: tempfile::TempDir,
}

#[cfg(feature = "native-sqlite")]
impl FileSqlite {
    pub async fn try_new() -> HarnessResult<Self> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("physical.sqlite");
        let store = Self::open_store(&path).await?;
        Ok(Self {
            store,
            path,
            _directory: directory,
        })
    }

    pub async fn open_store(
        path: &std::path::Path,
    ) -> HarnessResult<SqlStore<sql_backend::sqlite::SqliteBackend>> {
        Self::open_with_probe(path, None).await
    }

    pub async fn open_store_with_fault_probe(
        path: &std::path::Path,
        probe: Arc<dyn sql_backend::SqlFaultProbe>,
    ) -> HarnessResult<SqlStore<sql_backend::sqlite::SqliteBackend>> {
        Self::open_with_probe(path, Some(probe)).await
    }

    async fn open_with_probe(
        path: &std::path::Path,
        probe: Option<Arc<dyn sql_backend::SqlFaultProbe>>,
    ) -> HarnessResult<SqlStore<sql_backend::sqlite::SqliteBackend>> {
        let backend = sql_backend::sqlite::SqliteBackend::initialize(path).await?;
        let backend = if let Some(probe) = probe {
            backend.with_fault_probe(probe)
        } else {
            backend
        };
        let store = SqlStore::initialize_schema(backend).await?;
        store.initialize_records(limits()?).await?;
        Ok(store)
    }
}

#[cfg(feature = "native-postgres")]
pub struct Postgres {
    pub store: SqlStore<sql_backend::postgres::PostgresBackend>,
    dsn: String,
    schema: String,
}

#[cfg(feature = "native-postgres")]
impl Postgres {
    pub async fn try_new() -> HarnessResult<Self> {
        let dsn = std::env::var("TEST_POSTGRES_DSN")
            .map_err(|_| "selected Postgres contracts require TEST_POSTGRES_DSN")?;
        let mut scoped = url::Url::parse(&dsn)?;
        let schema = format!("replication_p03a_{}", uuid::Uuid::new_v4().simple());
        let admin = connect_postgres(&dsn).await?;
        admin
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .await?;
        let mut query = scoped
            .query()
            .into_iter()
            .flat_map(|query| query.split('&'))
            .filter(|pair| !matches!(pair.split('=').next(), Some("options" | "application_name")))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        // tokio-postgres decodes percent escapes, not form-encoded '+' spaces.
        query.push(format!(
            "options=-csearch_path%3D{schema}%20-csynchronous_commit%3Don"
        ));
        query.push(format!("application_name={schema}"));
        scoped.set_query(Some(&query.join("&")));
        let dsn = String::from(scoped);
        let result = Self::open_store(&dsn, &schema, None).await;
        match result {
            Ok(store) => Ok(Self { store, dsn, schema }),
            Err(error) => {
                admin
                    .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                    .await?;
                Err(error)
            }
        }
    }

    pub(super) async fn open_store(
        dsn: &str,
        expected_schema: &str,
        probe: Option<Arc<dyn sql_backend::SqlFaultProbe>>,
    ) -> HarnessResult<SqlStore<sql_backend::postgres::PostgresBackend>> {
        let connection = connect_postgres(dsn).await?;
        let scope = connection.query_one("SELECT current_schema()", &[]).await?;
        let schema: Option<String> = scope.get(0);
        if schema.as_deref() != Some(expected_schema) {
            return Err(
                "Postgres fixture search_path did not resolve its exact owned schema".into(),
            );
        }
        let policy: String = connection
            .query_one("SHOW synchronous_commit", &[])
            .await?
            .get(0);
        if policy != "on" {
            return Err("selected Postgres contracts require synchronous_commit=on".into());
        }
        let backend = sql_backend::postgres::PostgresBackend::initialize(dsn, 4, false).await?;
        let backend = if let Some(probe) = probe {
            backend.with_fault_probe(probe)
        } else {
            backend
        };
        let store = sql_backend::SqlStore::initialize_schema(backend).await?;
        store.initialize_records(limits()?).await?;
        Ok(store)
    }

    pub async fn connect(&self) -> HarnessResult<tokio_postgres::Client> {
        connect_postgres(&self.dsn).await
    }

    pub async fn open_independent_store(
        &self,
    ) -> HarnessResult<SqlStore<sql_backend::postgres::PostgresBackend>> {
        Self::open_store(&self.dsn, &self.schema, None).await
    }

    pub async fn open_independent_store_with_fault_probe(
        &self,
        probe: Arc<dyn sql_backend::SqlFaultProbe>,
    ) -> HarnessResult<SqlStore<sql_backend::postgres::PostgresBackend>> {
        Self::open_store(&self.dsn, &self.schema, Some(probe)).await
    }

    pub async fn finish(self) -> HarnessResult<()> {
        drop(self.store);
        connect_postgres(&self.dsn)
            .await?
            .batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .await?;
        Ok(())
    }
}

#[cfg(feature = "native-postgres")]
async fn connect_postgres(dsn: &str) -> HarnessResult<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

/// Execute the existing shared PutItem stages on the caller's borrowed catalog.
pub async fn put_item_in_catalog<A: storage_provider::UpsertAttempt>(
    catalog: &mut A,
    cache: &storage_provider::TableMetadataCache,
    request: storage_types::PutItemRequest,
    captured_at: storage_types::TimestampMillis,
) -> storage_types::StorageResult<()> {
    use storage_provider::{PutItemShape, prepare_accepted_upsert, resolve_table_in_attempt};

    let shape = PutItemShape::try_from_request(request)?;
    let (table, _) = resolve_table_in_attempt(catalog, cache, &shape.table_name).await?;
    let ttl = catalog.read_ttl_config(&table).await?;
    let bound = shape.bind_table(table, ttl.as_ref())?;
    let (observed, lifecycle) = catalog.observe_for_write(&bound.table, &bound.key).await?;
    let (accepted, _) = bound.accept_old(observed, lifecycle)?.into_result()?;
    prepare_accepted_upsert(
        catalog,
        accepted,
        captured_at,
        storage_types::ItemProvenance::default(),
    )
    .await?;
    catalog.flush_dynamo_write().await
}

/// Execute the existing shared UpdateItem stages on the caller's borrowed
/// catalog.
pub async fn update_item_in_catalog<A: storage_provider::UpsertAttempt>(
    catalog: &mut A,
    cache: &storage_provider::TableMetadataCache,
    request: storage_types::UpdateItemRequest,
    captured_at: storage_types::TimestampMillis,
) -> StorageResult<storage_types::WireItem> {
    use storage_provider::{UpdateItemShape, prepare_accepted_upsert, resolve_table_in_attempt};

    let shape = UpdateItemShape::try_from_request(request)?;
    let (table, _) = resolve_table_in_attempt(catalog, cache, &shape.table_name).await?;
    let bound = shape.bind_table(table)?;
    let (observed, lifecycle) = catalog.observe_for_write(&bound.table, &bound.key).await?;
    let (accepted, _) = bound.accept_old(observed, lifecycle)?.into_result()?;
    let images = prepare_accepted_upsert(
        catalog,
        accepted,
        captured_at,
        storage_types::ItemProvenance::default(),
    )
    .await?;
    catalog.flush_dynamo_write().await?;
    Ok(images.new_item)
}
