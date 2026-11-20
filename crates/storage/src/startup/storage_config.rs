use std::collections::HashMap;

use storage_provider::{
    FoundationDbSettings, PostgresSettings, RemoteCredentialStrategy, RemoteStaticCredentials,
    RemoteStorageSettings, RemoteTimeoutOverrides, RocksdbSettings, SqliteSettings, StorageBackend,
    StorageConfig, StorageConnectionRegistry, TursoSettings,
};
use storage_types::{StorageError, StorageResult};
use tokio::{net::TcpStream, time::timeout};
use tracing::error;

use super::constants::DEFAULT_DEPENDENCY_CONNECT_TIMEOUT;
use crate::database_manager::ROUTED_DEFAULT_CONNECTION_ID;

/// The one translation of a file backend section. A backend this build does
/// not include is rejected when its store opens (`open_store`).
pub fn storage_config_from_backends(backends: &config::Backends) -> StorageResult<StorageConfig> {
    let initial_stream_lineages = backends.initial_stream_lineages;
    if let Some(sqlite) = &backends.sqlite {
        return Ok(StorageConfig {
            initial_stream_lineages,
            backend_type: StorageBackend::SQLite,
            connection_string: Some(sqlite.db_path.clone()),
            file_path: None,
            sqlite: Some(SqliteSettings::default()),
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }
    if let Some(turso) = &backends.turso {
        return Ok(StorageConfig {
            initial_stream_lineages,
            backend_type: StorageBackend::Turso,
            connection_string: Some(turso.db_path.clone()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: Some(TursoSettings::default()),
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }
    if let Some(postgres) = &backends.postgres {
        return Ok(StorageConfig {
            initial_stream_lineages,
            backend_type: StorageBackend::Postgres,
            connection_string: Some(postgres.dsn.clone()),
            file_path: None,
            sqlite: None,
            postgres: Some(PostgresSettings {
                dsn: postgres.dsn.clone(),
                max_pool_size: postgres.max_pool_size,
                tls: postgres.tls,
            }),
            turso: None,
            rocksdb: None,
            foundationdb: None,
            remote: None,
        });
    }
    if let Some(rocksdb) = &backends.rocksdb {
        return Ok(StorageConfig {
            initial_stream_lineages,
            backend_type: StorageBackend::RocksDB,
            connection_string: Some(rocksdb.db_path.clone()),
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: Some(RocksdbSettings {
                writer_queue_capacity: rocksdb.writer_queue_capacity,
            }),
            foundationdb: None,
            remote: None,
        });
    }
    if let Some(fdb) = &backends.foundationdb {
        return Ok(StorageConfig {
            initial_stream_lineages,
            backend_type: StorageBackend::FoundationDb,
            connection_string: None,
            file_path: None,
            sqlite: None,
            postgres: None,
            turso: None,
            rocksdb: None,
            foundationdb: Some(FoundationDbSettings {
                transaction_byte_limit: fdb.transaction_byte_limit,
                cluster_file: fdb.cluster_file.clone(),
                tenant_name: fdb.tenant_name.clone(),
                subspace_prefix: fdb.subspace_prefix.clone(),
                cache_read_version_ms: fdb.cache_read_version_ms,
                reply_byte_limit: fdb.reply_byte_limit,
            }),
            remote: None,
        });
    }
    if let Some(remote) = &backends.remote {
        let credentials = match remote
            .credentials
            .as_ref()
            .and_then(|credentials| credentials.r#static.as_ref())
        {
            Some(static_credentials) => RemoteCredentialStrategy::Static(RemoteStaticCredentials {
                access_key_id: static_credentials.access_key.clone(),
                secret_access_key: static_credentials.secret_key.clone(),
                session_token: static_credentials.session_token.clone(),
            }),
            None => RemoteCredentialStrategy::DefaultChain,
        };
        let timeouts = remote
            .timeout_overrides
            .as_ref()
            .map(|overrides| RemoteTimeoutOverrides {
                connect_timeout_ms: overrides.connect_timeout_ms,
                request_timeout_ms: overrides.request_timeout_ms,
            });
        let settings = RemoteStorageSettings {
            endpoint_urls: remote.endpoint_urls.clone(),
            region: remote.region.clone(),
            tls: remote.tls,
            credentials,
            timeouts,
        };
        settings.validate()?;
        return Ok(StorageConfig {
            initial_stream_lineages,
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
        let mut connections = HashMap::with_capacity(registry_cfg.connections.len());
        for (connection_id, backends) in &registry_cfg.connections {
            connections.insert(
                connection_id.clone(),
                storage_config_from_backends(backends)?,
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
        default_connection_id: ROUTED_DEFAULT_CONNECTION_ID.to_owned(),
        connections: HashMap::from([(
            ROUTED_DEFAULT_CONNECTION_ID.to_owned(),
            storage_config_from_backends(&features.backends)?,
        )]),
    })
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
