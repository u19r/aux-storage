mod constants;

use std::collections::HashMap;

#[cfg(feature = "turso")]
use storage_provider::TursoSettings;
use storage_provider::{
    PostgresSettings, RemoteCredentialStrategy, RemoteStaticCredentials, RemoteStorageSettings,
    RemoteTimeoutOverrides, RocksdbSettings, SqliteSettings, StorageBackend, StorageConfig,
    StorageConnectionConfig, StorageConnectionRegistry,
};
use storage_types::{StorageError, StorageResult};
use tokio::{net::TcpStream, time::timeout};
use tracing::error;

use self::constants::DEFAULT_DEPENDENCY_CONNECT_TIMEOUT;
use crate::{DatabaseManager, DatabaseManagerRuntimeOptions, admission::AdmissionConfig};

#[cfg(feature = "turso")]
fn turso_backend_configured(backends: &config::Backends) -> bool {
    backends.turso.is_some()
}

#[cfg(not(feature = "turso"))]
fn turso_backend_configured(backends: &config::Backends) -> bool {
    serde_json::to_value(backends)
        .ok()
        .and_then(|value| value.get("turso").cloned())
        .is_some_and(|value| !value.is_null())
}

pub fn storage_config_from_backends(backends: &config::Backends) -> StorageResult<StorageConfig> {
    let connection = try_storage_connection_config_from_backends(backends)?;
    Ok(StorageConfig {
        backend_type: connection.backend_type,
        connection_string: connection.connection_string,
        file_path: connection.file_path,
        sqlite: connection.sqlite,
        postgres: connection.postgres,
        turso: connection.turso,
        rocksdb: connection.rocksdb,
        foundationdb: connection.foundationdb,
        remote: connection.remote,
    })
}

pub fn try_storage_connection_config_from_backends(
    backends: &config::Backends,
) -> StorageResult<StorageConnectionConfig> {
    if let Some(sqlite) = &backends.sqlite {
        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::SQLite,
            connection_string: Some(sqlite.db_path.clone()),
            file_path: None,
            sqlite: Some(SqliteSettings {
                immediate_gsi_consistency: sqlite.immediate_gsi_consistency,
                ..SqliteSettings::default()
            }),
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }

    #[cfg(feature = "turso")]
    if let Some(turso) = &backends.turso {
        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::Turso,
            connection_string: Some(turso.db_path.clone()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: Some(TursoSettings {
                immediate_gsi_consistency: turso.immediate_gsi_consistency,
            }),
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }

    #[cfg(not(feature = "turso"))]
    if turso_backend_configured(backends) {
        return Err(StorageError::validation(
            "configuration selects turso backend but binary was built without turso support",
        ));
    }

    if let Some(postgres) = &backends.postgres {
        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::Postgres,
            connection_string: Some(postgres.dsn.clone()),
            file_path: None,
            sqlite: None,
            postgres: Some(PostgresSettings {
                dsn: postgres.dsn.clone(),
                max_pool_size: postgres.max_pool_size,
                background_max_pool_size: postgres.background_max_pool_size,
                tls: postgres.tls,
                immediate_gsi_consistency: postgres.immediate_gsi_consistency,
            }),
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }

    if let Some(rocksdb) = &backends.rocksdb {
        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::RocksDB,
            connection_string: Some(rocksdb.db_path.clone()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: Some(RocksdbSettings {
                immediate_gsi_consistency: rocksdb.immediate_gsi_consistency,
            }),
            foundationdb: None,
            remote: None,
        });
    }

    if let Some(fdb) = &backends.foundationdb {
        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::FoundationDb,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: Some(storage_provider::FoundationDbSettings {
                transaction_byte_limit: fdb.transaction_byte_limit,
                cluster_file: fdb.cluster_file.clone(),
                tenant_name: fdb.tenant_name.clone(),
                subspace_prefix: fdb.subspace_prefix.clone(),
                cache_read_version_ms: fdb.cache_read_version_ms,
                immediate_gsi_consistency: fdb.immediate_gsi_consistency,
            }),
            remote: None,
        });
    }

    if let Some(remote) = &backends.remote {
        if let Some(credentials) = &remote.credentials {
            credentials.validate().map_err(|err| {
                StorageError::validation(format!("invalid remote credentials config: {err}"))
            })?;
        }

        let credentials = remote
            .credentials
            .as_ref()
            .and_then(|cfg| {
                if let Some(static_creds) = &cfg.r#static {
                    Some(RemoteCredentialStrategy::Static(RemoteStaticCredentials {
                        access_key_id: static_creds.access_key.clone(),
                        secret_access_key: static_creds.secret_key.clone(),
                        session_token: static_creds.session_token.clone(),
                    }))
                } else if cfg.instance_keys.unwrap_or(false)
                    || (cfg.r#static.is_none() && cfg.instance_keys.is_none())
                {
                    Some(RemoteCredentialStrategy::DefaultChain)
                } else {
                    None
                }
            })
            .unwrap_or(RemoteCredentialStrategy::DefaultChain);

        let timeouts = remote
            .timeout_overrides
            .as_ref()
            .map(|ov| RemoteTimeoutOverrides {
                connect_timeout_ms: ov.connect_timeout_ms,
                request_timeout_ms: ov.request_timeout_ms,
            });

        let settings = RemoteStorageSettings {
            endpoint_urls: remote.endpoint_urls.clone(),
            region: remote.region.clone(),
            tls: remote.tls,
            credentials,
            timeouts,
        };
        settings.validate()?;

        return Ok(StorageConnectionConfig {
            backend_type: StorageBackend::Remote,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: Some(settings),
        });
    }

    Err(StorageError::validation(
        "exactly one storage backend must be configured",
    ))
}

pub fn storage_connection_registry_from_features(
    features: &config::Features,
) -> StorageResult<StorageConnectionRegistry> {
    if let Some(registry_cfg) = features.storage_connections.as_ref() {
        let mut connections: HashMap<String, StorageConnectionConfig> =
            HashMap::with_capacity(registry_cfg.connections.len());
        for (connection_id, backends) in &registry_cfg.connections {
            connections.insert(
                connection_id.clone(),
                try_storage_connection_config_from_backends(backends)?,
            );
        }
        if !connections.contains_key(&registry_cfg.default_connection) {
            return Err(StorageError::validation(format!(
                "default connection '{}' missing in features.storage_connections.connections",
                registry_cfg.default_connection
            )));
        }
        return Ok(StorageConnectionRegistry {
            default_connection_id: registry_cfg.default_connection.clone(),
            connections,
        });
    }

    Ok(StorageConnectionRegistry {
        default_connection_id: "default".to_string(),
        connections: HashMap::from([(
            "default".to_string(),
            try_storage_connection_config_from_backends(&features.backends)?,
        )]),
    })
}

pub async fn database_manager_from_features(
    features: &config::Features,
    mut runtime_options: DatabaseManagerRuntimeOptions,
) -> StorageResult<DatabaseManager> {
    let registry = storage_connection_registry_from_features(features)?;
    runtime_options.load_regional_connections(&features.regional_connections)?;
    runtime_options.admission_config = AdmissionConfig::from_config(&features.storage_admission)
        .map_err(|error| StorageError::validation(error.to_string()))?;
    DatabaseManager::new_with_connection_registry_and_runtime_options(registry, runtime_options)
        .await
}

pub fn storage_config_for_configured_process(
    config: &config::Config,
) -> StorageResult<StorageConfig> {
    storage_config_from_backends(&config.root.features.backends)
}

pub fn ensure_backend_matches(backends: &config::Backends) -> Result<(), String> {
    if backends.sqlite.is_some() && !cfg!(feature = "sqlite") {
        return Err(
            "configuration selects sqlite backend but binary was built without sqlite support"
                .to_string(),
        );
    }
    if turso_backend_configured(backends) && !cfg!(feature = "turso") {
        return Err(
            "configuration selects turso backend but binary was built without turso support"
                .to_string(),
        );
    }
    if backends.postgres.is_some() && !cfg!(feature = "postgres") {
        return Err(
            "configuration selects postgres backend but binary was built without postgres support"
                .to_string(),
        );
    }
    if backends.rocksdb.is_some() && !cfg!(feature = "rocksdb") {
        return Err(
            "configuration selects rocksdb backend but binary was built without rocksdb support"
                .to_string(),
        );
    }
    if backends.foundationdb.is_some() && !cfg!(feature = "foundationdb") {
        return Err(
            "configuration selects foundationdb backend but binary was built without foundationdb \
             support"
                .to_string(),
        );
    }
    if backends.remote.is_some() && !cfg!(feature = "remote") {
        return Err(
            "configuration selects remote storage backend but binary was built without remote \
             support"
                .to_string(),
        );
    }

    Ok(())
}

pub async fn log_foundationdb_connectivity_errors(storage_cfg: &StorageConfig) {
    if !matches!(storage_cfg.backend_type, StorageBackend::FoundationDb) {
        return;
    }

    let Some(settings) = storage_cfg.foundationdb.as_ref() else {
        error!(
            target: "deps",
            "foundationdb backend configured but settings missing"
        );
        return;
    };
    let Some(cluster_path) = settings.cluster_file.as_ref() else {
        error!(
            target: "deps",
            "foundationdb backend configured but cluster file path missing"
        );
        return;
    };

    let raw = match std::fs::read_to_string(cluster_path) {
        Ok(raw) => raw,
        Err(err) => {
            error!(
                target: "deps",
                path = %cluster_path,
                error = %err,
                "failed to read foundationdb cluster file"
            );
            return;
        }
    };
    let coordinators = match parse_fdb_cluster_file(&raw) {
        Ok(coords) => coords,
        Err(err) => {
            error!(
                target: "deps",
                path = %cluster_path,
                error = %err,
                "failed to parse foundationdb cluster file"
            );
            return;
        }
    };

    if coordinators.is_empty() {
        error!(
            target: "deps",
            path = %cluster_path,
            "foundationdb cluster file has no coordinators"
        );
        return;
    }

    let coord_labels: Vec<String> = coordinators
        .iter()
        .map(|(host, port)| format!("{host}:{port}"))
        .collect();
    let mut last_err: Option<String> = None;
    for (host, port) in coordinators {
        let addr = format!("{host}:{port}");
        match timeout(DEFAULT_DEPENDENCY_CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(_)) => return,
            Ok(Err(err)) => last_err = Some(err.to_string()),
            Err(_) => {
                last_err = Some(format!(
                    "timeout after {}s",
                    DEFAULT_DEPENDENCY_CONNECT_TIMEOUT.as_secs()
                ));
            }
        }
    }

    error!(
        target: "deps",
        path = %cluster_path,
        coordinators = ?coord_labels,
        error = %last_err.unwrap_or_else(|| "unknown error".to_string()),
        "foundationdb coordinators are unreachable"
    );
}

fn parse_fdb_cluster_file(raw: &str) -> Result<Vec<(String, u16)>, String> {
    let trimmed = raw.trim();
    let (_, coord_part) = trimmed
        .split_once('@')
        .ok_or_else(|| "cluster file missing @ separator".to_string())?;
    let mut coords = Vec::new();
    for entry in coord_part.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (host, port_raw) = entry
            .rsplit_once(':')
            .ok_or_else(|| format!("coordinator missing port: {entry}"))?;
        let port = port_raw
            .parse::<u16>()
            .map_err(|err| format!("invalid coordinator port {port_raw}: {err}"))?;
        coords.push((host.to_string(), port));
    }
    Ok(coords)
}
