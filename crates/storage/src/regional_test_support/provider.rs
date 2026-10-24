use std::path::Path;

use anyhow::{Context, Result};
use serde_json::json;

pub struct ProviderFixture {
    pub backends: [config::Backends; 2],
    cleanup: Cleanup,
}

enum Cleanup {
    Files,
    Postgres {
        dsn: String,
        schemas: [String; 2],
    },
    #[cfg(all(feature = "foundationdb", feature = "test-support"))]
    FoundationDb(Box<[crate::FoundationDbTestScope; 2]>),
}

impl ProviderFixture {
    pub async fn start(root: &Path) -> Result<Self> {
        let selected = std::env::var("REGIONAL_TEST_PROVIDER").unwrap_or_else(|_| "sqlite".into());
        match selected.as_str() {
            "sqlite" | "turso" | "rocksdb" => {
                let backend = |index| {
                    serde_json::from_value(json!({selected.clone(): {
                        "db_path": root.join(format!("region-{index}.{selected}")).to_string_lossy(),
                        "immediate_gsi_consistency": true,
                    }}))
                };
                Ok(Self {
                    backends: [backend(1)?, backend(2)?],
                    cleanup: Cleanup::Files,
                })
            }
            "postgres" => Self::postgres().await,
            #[cfg(all(feature = "foundationdb", feature = "test-support"))]
            "foundationdb" => Self::foundationdb().await,
            _ => anyhow::bail!(
                "selected regional provider is unknown or not compiled with test support"
            ),
        }
    }

    async fn postgres() -> Result<Self> {
        let dsn = std::env::var("TEST_POSTGRES_DSN")
            .context("Postgres fixture requires TEST_POSTGRES_DSN")?;
        let client = postgres_client(&dsn).await?;
        let schema_prefix = format!("regional_slice_{}", uuid::Uuid::new_v4().simple());
        let schemas = [format!("{schema_prefix}_1"), format!("{schema_prefix}_2")];
        client
            .batch_execute(&format!(
                "BEGIN; CREATE SCHEMA {}; CREATE SCHEMA {}; COMMIT",
                schemas[0], schemas[1]
            ))
            .await?;
        let prepared = async {
            let mut backends = Vec::new();
            for schema in &schemas {
                let mut scoped = url::Url::parse(&dsn)?;
                let mut query = scoped
                    .query()
                    .into_iter()
                    .flat_map(|query| query.split('&'))
                    .filter(|pair| {
                        !matches!(pair.split('=').next(), Some("options" | "application_name"))
                    })
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                query.push(format!(
                    "options=-csearch_path%3D{schema}%20-csynchronous_commit%3Don"
                ));
                query.push(format!("application_name={schema}"));
                scoped.set_query(Some(&query.join("&")));
                let scoped = String::from(scoped);
                let observed: Option<String> = postgres_client(&scoped)
                    .await?
                    .query_one("SELECT current_schema()", &[])
                    .await?
                    .get(0);
                anyhow::ensure!(
                    observed.as_deref() == Some(schema),
                    "Postgres fixture is outside its owned schema"
                );
                backends.push(serde_json::from_value(json!({"postgres": {
                    "dsn": scoped, "max_pool_size": 12, "background_max_pool_size": 4,
                    "tls": false, "immediate_gsi_consistency": true,
                }}))?);
            }
            backends
                .try_into()
                .map_err(|_| anyhow::anyhow!("missing regional stores"))
        }
        .await;
        let cleanup = Cleanup::Postgres { dsn, schemas };
        match prepared {
            Ok(backends) => Ok(Self { backends, cleanup }),
            Err(error) => {
                cleanup.finish().await?;
                Err(error)
            }
        }
    }

    #[cfg(all(feature = "foundationdb", feature = "test-support"))]
    async fn foundationdb() -> Result<Self> {
        let cluster_file = std::env::var("TEST_FDB_CLUSTER_FILE")
            .unwrap_or_else(|_| "/usr/local/etc/foundationdb/fdb.cluster".into());
        let first = crate::FoundationDbTestScope::try_new(cluster_file.clone()).await?;
        let second = match crate::FoundationDbTestScope::try_new(cluster_file).await {
            Ok(scope) => scope,
            Err(error) => {
                first.cleanup().await?;
                return Err(error.into());
            }
        };
        let scopes = [first, second];
        let backend = |scope: &crate::FoundationDbTestScope| {
            serde_json::from_value(json!({"foundationdb": {
                "cluster_file": scope.settings().cluster_file, "subspace_prefix": scope.settings().subspace_prefix,
                "cache_read_version_ms": 0, "immediate_gsi_consistency": true,
            }}))
        };
        let prepared = (|| Ok([backend(&scopes[0])?, backend(&scopes[1])?]))();
        let cleanup = Cleanup::FoundationDb(Box::new(scopes));
        match prepared {
            Ok(backends) => Ok(Self { backends, cleanup }),
            Err(error) => {
                cleanup.finish().await?;
                Err(error)
            }
        }
    }

    pub async fn finish(self) -> Result<()> {
        self.cleanup.finish().await
    }
}

impl Cleanup {
    async fn finish(self) -> Result<()> {
        let mut result = Ok(());
        match self {
            Cleanup::Files => {}
            Cleanup::Postgres { dsn, schemas } => {
                let client = postgres_client(&dsn).await?;
                for schema in schemas {
                    result = result.and(
                        client
                            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
                            .await
                            .map_err(Into::into),
                    );
                }
            }
            #[cfg(all(feature = "foundationdb", feature = "test-support"))]
            Cleanup::FoundationDb(scopes) => {
                for scope in *scopes {
                    result = result.and(scope.cleanup().await.map_err(Into::into));
                }
            }
        }
        result
    }
}

async fn postgres_client(dsn: &str) -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}
