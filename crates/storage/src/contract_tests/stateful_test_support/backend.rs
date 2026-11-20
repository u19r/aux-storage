use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use storage_types::{
    AttributeDefinition, AttributeValue, BillingMode, CreateGlobalSecondaryIndex,
    CreateTableRequest, GlobalSecondaryIndexUpdate, IndexName, KeyAttributeType, KeyAttributes,
    KeySchemaElement, KeyType, Projection, ProjectionType, StreamSpecification, StreamViewType,
    TableName, TimeToLiveSpecification, UpdateTableRequest, UpdateTimeToLiveRequest,
    context::WrappedError as _,
};

use super::{
    maintenance::MaintenanceHarness,
    models::{
        ExpirationFixtureTarget, GsiFixtureTarget, QueueReclamationFixtureTarget,
        StreamReclamationFixtureTarget,
    },
};
use crate::{
    DatabaseManager, DatabaseManagerRuntimeOptions, DeleteItemInput, QueryIndexInput,
    database_manager::construction_tests::database_manager_with_components,
    provider::{SqliteSettings, StorageBackend, StorageConfig},
};

static PATH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub struct StatefulTestBackend {
    pub(super) database: Option<DatabaseManager>,
    pub(super) maintenance: Option<MaintenanceHarness>,
    pub(super) config: StorageConfig,
    pub(super) path: Option<PathBuf>,
    pub(super) target: Mutex<Option<ExpirationFixtureTarget>>,
    pub(super) gsi_target: Mutex<Option<GsiFixtureTarget>>,
    pub(super) stream_reclamation_target: Mutex<Option<StreamReclamationFixtureTarget>>,
    #[cfg_attr(
        not(any(feature = "foundationdb", feature = "rocksdb")),
        expect(dead_code, reason = "queue reclamation contracts run on KV providers")
    )]
    pub(super) queue_reclamation_target: Mutex<Option<QueueReclamationFixtureTarget>>,
    pub(super) generation: AtomicU64,
}

impl StatefulTestBackend {
    pub async fn sqlite() -> Result<Self, storage::StorageError> {
        let path = unique_database_path();
        let config = sqlite_config(&path);
        Self::try_new(config, Some(path)).await
    }

    #[cfg(feature = "turso")]
    pub async fn turso() -> Result<Self, storage::StorageError> {
        let path = unique_database_path();
        let config = local_config(StorageBackend::Turso, &path);
        Self::try_new(config, Some(path)).await
    }

    #[cfg(feature = "rocksdb")]
    pub async fn rocksdb() -> Result<Self, storage::StorageError> {
        let path = unique_database_path();
        let config = local_config(StorageBackend::RocksDB, &path);
        Self::try_new(config, Some(path)).await
    }

    #[cfg(feature = "postgres")]
    pub async fn postgres() -> Result<Option<Self>, storage::StorageError> {
        let Some(base_dsn) = std::env::var("TEST_POSTGRES_DSN").ok() else {
            return Ok(None);
        };
        let dsn = isolated_postgres_dsn(&base_dsn).await?;
        let config = StorageConfig {
            initial_stream_lineages: None,
            backend_type: StorageBackend::Postgres,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: Some(crate::provider::PostgresSettings {
                dsn,
                tls: false,
                ..crate::provider::PostgresSettings::default()
            }),
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        };
        Self::try_new(config, None).await.map(Some)
    }

    #[cfg(feature = "foundationdb")]
    pub async fn foundationdb() -> Result<Option<Self>, storage::StorageError> {
        let Some(cluster_file) = std::env::var("TEST_FDB_CLUSTER_FILE").ok() else {
            return Ok(None);
        };
        let config = StorageConfig {
            initial_stream_lineages: None,
            backend_type: StorageBackend::FoundationDb,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: Some(crate::provider::FoundationDbSettings {
                cluster_file: Some(cluster_file),
                subspace_prefix: Some(unique_external_scope("gsi-contract")),
                ..crate::provider::FoundationDbSettings::default()
            }),
            remote: None,
        };
        Self::try_new(config, None).await.map(Some)
    }

    async fn try_new(
        config: StorageConfig,
        path: Option<PathBuf>,
    ) -> Result<Self, storage::StorageError> {
        let (database, maintenance) = Self::open(&config).await?;
        Ok(Self {
            database: Some(database),
            maintenance: Some(maintenance),
            config,
            path,
            target: Mutex::new(None),
            gsi_target: Mutex::new(None),
            stream_reclamation_target: Mutex::new(None),
            queue_reclamation_target: Mutex::new(None),
            generation: AtomicU64::new(0),
        })
    }

    async fn open(
        config: &StorageConfig,
    ) -> Result<(DatabaseManager, MaintenanceHarness), storage::StorageError> {
        let runtime_options = DatabaseManagerRuntimeOptions::builder()
            .enable_database_jobs(false)
            .scheduled_job_store_config(bg_job_store::ScheduledJobStoreConfig::from(
                std::time::Duration::from_secs(5),
            ))
            .build();
        let (database, maintenance_repositories, store) =
            database_manager_with_components(config.clone(), runtime_options).await?;
        let store = store.ok_or_else(|| {
            storage::StorageError::internal("scheduled job store was not selected")
        })?;
        let maintenance_repositories = maintenance_repositories.ok_or_else(|| {
            storage::StorageError::internal("maintenance repositories were not selected")
        })?;
        let maintenance = MaintenanceHarness::try_new(store, maintenance_repositories).await?;
        Ok((database, maintenance))
    }

    pub async fn create_table(&self, table_name: &str) -> Result<(), storage::StorageError> {
        self.create_table_with_stream(table_name, false)
            .await
            .map(|_| ())
    }

    pub async fn create_expiration_target(
        &self,
        table_name: &str,
    ) -> Result<(), storage::StorageError> {
        let table_name = self.create_table_with_stream(table_name, true).await?;
        self.database
            .as_ref()
            .expect("fixture database is open")
            .update_time_to_live(UpdateTimeToLiveRequest {
                table_name: table_name.clone(),
                time_to_live_specification: TimeToLiveSpecification {
                    attribute_name: "ttl".to_owned(),
                    enabled: true,
                },
            })
            .await?;
        // Establish the durable expiration task before injecting a commit
        // fault. Finish the empty bounded sweep so the next run starts at
        // the catalog beginning and can observe the item inserted below.
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .prime_expiration_task()
            .await?;
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("A".to_owned())),
            ("ttl".to_owned(), AttributeValue::N("1".to_owned())),
        ]);
        self.database
            .as_ref()
            .expect("fixture database is open")
            .put_item_encode(table_name.clone(), &item, None, None, None, None)
            .await?;
        *self.target.lock().expect("stateful target lock") = Some(ExpirationFixtureTarget {
            table_name,
            key: KeyAttributes::from(HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("A".to_owned()),
            )])),
        });
        Ok(())
    }

    pub(crate) async fn create_table_with_stream(
        &self,
        table_name: &str,
        stream_enabled: bool,
    ) -> Result<TableName, storage::StorageError> {
        let table_name = TableName::try_from(table_name.to_owned())
            .map_err(|error| storage::StorageError::validation(error.to_string()))?;
        let mut request = CreateTableRequest::new(
            table_name.clone(),
            vec![AttributeDefinition {
                attribute_name: "pk".to_owned(),
                attribute_type: KeyAttributeType::S,
            }],
            vec![KeySchemaElement {
                attribute_name: "pk".to_owned(),
                key_type: KeyType::Hash,
            }],
            BillingMode::PayPerRequest,
        );
        if stream_enabled {
            request.stream_specification = Some(StreamSpecification {
                stream_enabled: true,
                stream_view_type: Some(StreamViewType::NewAndOldImages),
            });
        }
        self.database
            .as_ref()
            .expect("fixture database is open")
            .create_table(&request)
            .await?;
        self.generation.fetch_add(1, Ordering::Relaxed);
        *self.target.lock().expect("stateful target lock") = Some(ExpirationFixtureTarget {
            table_name: table_name.clone(),
            key: KeyAttributes::from(HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("A".to_owned()),
            )])),
        });
        Ok(table_name)
    }

    pub async fn create_index(&self) -> Result<(), storage::StorageError> {
        let index_name = IndexName::new("by_group");
        let table_name = self.create_index_named(index_name.clone()).await?;
        *self.gsi_target.lock().expect("GSI fixture target lock") = Some(GsiFixtureTarget {
            table_name,
            index_name,
        });
        Ok(())
    }

    async fn create_index_named(
        &self,
        index_name: IndexName,
    ) -> Result<TableName, storage::StorageError> {
        let table_name = self
            .target
            .lock()
            .expect("stateful target lock")
            .as_ref()
            .expect("fixture target is configured")
            .table_name
            .clone();
        self.database
            .as_ref()
            .expect("fixture database is open")
            .update_table(UpdateTableRequest {
                attribute_definitions: Some(vec![
                    AttributeDefinition {
                        attribute_name: "gsi_pk".to_owned(),
                        attribute_type: KeyAttributeType::S,
                    },
                    AttributeDefinition {
                        attribute_name: "gsi_sk".to_owned(),
                        attribute_type: KeyAttributeType::S,
                    },
                ]),
                global_secondary_index_updates: Some(vec![GlobalSecondaryIndexUpdate {
                    create: Some(CreateGlobalSecondaryIndex {
                        index_name: index_name.clone(),
                        key_schema: vec![
                            KeySchemaElement {
                                attribute_name: "gsi_pk".to_owned(),
                                key_type: KeyType::Hash,
                            },
                            KeySchemaElement {
                                attribute_name: "gsi_sk".to_owned(),
                                key_type: KeyType::Range,
                            },
                        ],
                        projection: Projection {
                            projection_type: Some(ProjectionType::All),
                            non_key_attributes: None,
                        },
                        provisioned_throughput: None,
                    }),
                    update: None,
                    delete: None,
                }]),
                ..UpdateTableRequest::new(table_name.clone())
            })
            .await?;
        Ok(table_name)
    }

    pub async fn put(&self) -> Result<(), storage::StorageError> {
        let target = self
            .target
            .lock()
            .expect("stateful target lock")
            .clone()
            .expect("fixture target is configured");
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S("A".to_owned())),
            ("ttl".to_owned(), AttributeValue::N("4000000000".to_owned())),
        ]);
        self.database
            .as_ref()
            .expect("fixture database is open")
            .put_item_encode(target.table_name, &item, None, None, None, None)
            .await
            .map(|_| ())
    }

    pub async fn put_initial_gsi_items(&self) -> Result<(), storage::StorageError> {
        self.put_gsi_item("A", "x").await?;
        self.put_gsi_item("B", "x").await
    }

    pub async fn move_gsi_item(&self) -> Result<(), storage::StorageError> {
        self.put_gsi_item("A", "y").await
    }

    pub async fn delete_gsi_item(&self) -> Result<(), storage::StorageError> {
        let target = self
            .gsi_target
            .lock()
            .expect("GSI fixture target lock")
            .clone()
            .expect("GSI fixture target is configured");
        self.database
            .as_ref()
            .expect("fixture database is open")
            .delete_item(
                DeleteItemInput::builder()
                    .table_name(target.table_name)
                    .key(KeyAttributes::from(HashMap::from([(
                        "pk".to_owned(),
                        AttributeValue::S("B".to_owned()),
                    )])))
                    .build(),
            )
            .await
            .map_err(|error| {
                storage::StorageError::internal(&format!(
                    "delete GSI fixture item failed: {:?}",
                    error.to_enum()
                ))
            })?;
        Ok(())
    }

    pub async fn delete(&self) -> Result<(), storage::StorageError> {
        let table_name = self
            .target
            .lock()
            .expect("stateful target lock")
            .as_ref()
            .expect("fixture target is configured")
            .table_name
            .clone();
        let database = self.database.as_ref().expect("fixture database is open");
        database.delete_table(&table_name).await?;
        database.clear_all_tables().await?;
        Ok(())
    }

    pub async fn query_index(&self) -> Result<Vec<String>, storage::StorageError> {
        let target = self
            .gsi_target
            .lock()
            .expect("GSI fixture target lock")
            .clone()
            .expect("GSI fixture target is configured");
        let mut visible = Vec::new();
        for group in ["x", "y"] {
            let page = self
                .database
                .as_ref()
                .expect("fixture database is open")
                .query_index_map(
                    QueryIndexInput::builder()
                        .table_name(target.table_name.clone())
                        .index_name(target.index_name.clone())
                        .key_condition_expression("gsi_pk = :group".to_owned())
                        .expression_attribute_values(HashMap::from([(
                            ":group".to_owned(),
                            AttributeValue::S(group.to_owned()),
                        )]))
                        .build(),
                )
                .await?;
            for item in page.items {
                let Some(AttributeValue::S(key)) = item.get("pk") else {
                    return Err(storage::StorageError::internal(
                        "GSI fixture row has no string primary key",
                    ));
                };
                visible.push(format!("{key}:{group}"));
            }
        }
        visible.sort_unstable();
        Ok(visible)
    }

    pub async fn drive_ready_work(&self) -> Result<(), storage::StorageError> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .drive_ready_work()
            .await
    }

    pub async fn drive_gsi_until_idle(&self) -> Result<(), storage::StorageError> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .drive_gsi_until_idle()
            .await
    }

    pub async fn shutdown_before_next_expiration_page(&self) -> Result<(), storage::StorageError> {
        self.maintenance
            .as_ref()
            .expect("fixture maintenance runtime is open")
            .shutdown_before_next_page()
            .await
    }

    pub async fn restart(&mut self) -> Result<(), storage::StorageError> {
        self.database.take();
        self.maintenance
            .as_mut()
            .expect("fixture maintenance runtime is open")
            .restart()?;
        self.database = Some(
            DatabaseManager::new_with_config_and_runtime_options(
                self.config.clone(),
                DatabaseManagerRuntimeOptions::builder()
                    .enable_database_jobs(false)
                    .scheduled_job_store_config(bg_job_store::ScheduledJobStoreConfig::from(
                        std::time::Duration::from_secs(5),
                    ))
                    .build(),
            )
            .await?,
        );
        Ok(())
    }

    async fn put_gsi_item(&self, key: &str, group: &str) -> Result<(), storage::StorageError> {
        let table_name = self
            .gsi_target
            .lock()
            .expect("GSI fixture target lock")
            .clone()
            .map(|target| target.table_name)
            .unwrap_or_else(|| {
                self.target
                    .lock()
                    .expect("stateful target lock")
                    .as_ref()
                    .expect("fixture target is configured")
                    .table_name
                    .clone()
            });
        let item = HashMap::from([
            ("pk".to_owned(), AttributeValue::S(key.to_owned())),
            ("gsi_pk".to_owned(), AttributeValue::S(group.to_owned())),
            ("gsi_sk".to_owned(), AttributeValue::S(key.to_owned())),
        ]);
        self.database
            .as_ref()
            .expect("fixture database is open")
            .put_item_encode(table_name, &item, None, None, None, None)
            .await?;
        Ok(())
    }
}

impl Drop for StatefulTestBackend {
    fn drop(&mut self) {
        self.database.take();
        self.maintenance.take();
        if let Some(path) = self.path.as_deref() {
            remove_local_artifacts(path);
        }
    }
}

fn unique_database_path() -> PathBuf {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    let sequence = PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "aux-storage-stateful-contract-{}-{timestamp}-{sequence}.db",
        std::process::id()
    ))
}

#[cfg(any(feature = "postgres", feature = "foundationdb"))]
fn unique_external_scope(label: &str) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after Unix epoch")
        .as_nanos();
    let sequence = PATH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "tests/{label}/{}/{timestamp}/{sequence}/",
        std::process::id()
    )
}

#[cfg(feature = "postgres")]
pub(crate) async fn isolated_postgres_dsn(base_dsn: &str) -> Result<String, storage::StorageError> {
    let schema = unique_external_scope("gsi_contract")
        .replace(['/', '-'], "_")
        .trim_matches('_')
        .to_owned();
    let (client, connection) = tokio_postgres::connect(base_dsn, tokio_postgres::NoTls)
        .await
        .map_err(|error| storage::StorageError::internal(&error))?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute(&format!("CREATE SCHEMA \"{schema}\""))
        .await
        .map_err(|error| storage::StorageError::internal(&error))?;
    if base_dsn.contains("://") {
        let separator = if base_dsn.contains('?') { '&' } else { '?' };
        Ok(format!(
            "{base_dsn}{separator}options=-csearch_path%3D{schema}"
        ))
    } else {
        Ok(format!("{base_dsn} options='-csearch_path={schema}'"))
    }
}

fn sqlite_config(path: &Path) -> StorageConfig {
    let mut config = local_config(StorageBackend::SQLite, path);
    config.sqlite = Some(SqliteSettings::default());
    config
}

fn local_config(backend_type: StorageBackend, path: &Path) -> StorageConfig {
    StorageConfig {
        initial_stream_lineages: None,
        backend_type,
        connection_string: Some(path.to_string_lossy().into_owned()),
        file_path: None,
        sqlite: None,
        postgres: None,
        turso: None,
        rocksdb: None,
        foundationdb: None,
        remote: None,
    }
}

fn remove_local_artifacts(path: &Path) {
    if path.is_dir() {
        let _ = fs::remove_dir_all(path);
        return;
    }
    for artifact in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = fs::remove_file(artifact);
    }
}
