use std::{ffi::OsString, path::PathBuf, sync::Arc};

use clap::Parser;
use serde_json::Value;

use crate::{
    Backends, Config, ConfigError, Cors, MetricsConfig, RootConfig, StorageReplicationConfig,
    StorageSyncReplicationConfig, Tracing, load_optional_with_overrides,
};

#[derive(Parser, Debug)]
#[command(name = "local-dynamodb-server")]
#[command(about = "A local DynamoDB server implementation in Rust")]
struct StorageApiArgs {
    #[arg(long, value_enum)]
    storage: Option<StorageBackendArg>,
    #[arg(long)]
    db_path: Option<String>,
    #[arg(long)]
    postgres_dsn: Option<String>,
    #[arg(long)]
    postgres_max_pool_size: Option<usize>,
    #[arg(long)]
    postgres_background_max_pool_size: Option<usize>,
    #[arg(long)]
    postgres_tls: Option<bool>,
    #[arg(short, long)]
    port: Option<String>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = false)]
    enable_internal_helper_routes: bool,
    #[arg(long = "overrides", value_name = "PATH=VALUE")]
    overrides: Vec<String>,
}

#[derive(clap::ValueEnum, Clone, Debug, PartialEq, Eq)]
pub enum StorageBackendArg {
    #[value(name = "sqlite")]
    Sqlite,
    #[value(name = "turso")]
    Turso,
    #[value(name = "postgres")]
    Postgres,
    #[value(name = "rocksdb")]
    Rocksdb,
    #[value(name = "foundationdb")]
    Foundationdb,
}

#[derive(Debug, Clone)]
pub struct LaunchInputs {
    pub config_path: Option<PathBuf>,
    pub top_level_overrides: Vec<(String, String)>,
    pub json_path_overrides: Vec<(String, String)>,
    pub enable_internal_helper_routes: bool,
}

#[derive(Debug, Clone)]
pub struct StorageApiLaunchEffectiveConfig {
    pub root: RootConfig,
    pub config: Arc<Config>,
    pub bind_addr: String,
    pub cors: Cors,
    pub tracing: Tracing,
    pub metrics: MetricsConfig,
    pub backends: Backends,
    pub storage_replication: StorageReplicationConfig,
    pub storage_sync_replication: StorageSyncReplicationConfig,
    pub enable_background_workers: bool,
    pub enable_internal_helper_routes: bool,
    pub config_watch_path: Option<PathBuf>,
    pub effective_json: Value,
}

#[derive(Debug, Clone)]
pub struct StorageApiLaunchConfig {
    pub inputs: LaunchInputs,
    pub effective: StorageApiLaunchEffectiveConfig,
}

impl StorageApiLaunchConfig {
    pub fn from_args<I, T>(args: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let args = StorageApiArgs::try_parse_from(args)
            .map_err(|error| ConfigError::argument(error.to_string()))?;
        Self::from_parsed_args(args)
    }

    fn from_parsed_args(args: StorageApiArgs) -> Result<Self, ConfigError> {
        let mut top_level_overrides = Vec::new();
        collect_top_level_overrides(&args, &mut top_level_overrides);

        let json_path_overrides = parse_override_args(&args.overrides)?;
        let all_overrides = top_level_overrides
            .iter()
            .chain(json_path_overrides.iter())
            .cloned()
            .collect::<Vec<_>>();
        let config =
            load_optional_with_overrides(args.config.as_deref(), all_overrides.as_slice())?;
        let root = config.root.clone();
        let effective = StorageApiLaunchEffectiveConfig {
            bind_addr: root.http.bind_addr.clone(),
            cors: root.http.cors.clone(),
            tracing: root.features.tracing.clone(),
            metrics: root.features.metrics.clone(),
            backends: root.features.backends.clone(),
            storage_replication: root.jobs.storage_replication.clone(),
            storage_sync_replication: root.features.storage_sync_replication.clone(),
            enable_background_workers: root.features.runtime.enable_background_workers,
            enable_internal_helper_routes: args.enable_internal_helper_routes,
            config_watch_path: args.config.clone(),
            effective_json: config.effective_json.clone(),
            root,
            config: config.clone(),
        };

        Ok(Self {
            inputs: LaunchInputs {
                config_path: args.config,
                top_level_overrides,
                json_path_overrides,
                enable_internal_helper_routes: effective.enable_internal_helper_routes,
            },
            effective,
        })
    }
}

fn collect_top_level_overrides(args: &StorageApiArgs, overrides: &mut Vec<(String, String)>) {
    let postgres_selected_by_setting = args.postgres_dsn.is_some()
        || args.postgres_max_pool_size.is_some()
        || args.postgres_background_max_pool_size.is_some()
        || args.postgres_tls.is_some();
    let selected_backend = args
        .storage
        .clone()
        .or_else(|| postgres_selected_by_setting.then_some(StorageBackendArg::Postgres));
    if let Some(storage) = selected_backend {
        select_storage_backend(&storage, overrides);
    }
    if let Some(port) = &args.port {
        overrides.push(("http.bind_addr".to_string(), format!("0.0.0.0:{port}")));
    }
    if let Some(db_path) = &args.db_path {
        let backend = args.storage.clone().unwrap_or(StorageBackendArg::Sqlite);
        let path = match backend {
            StorageBackendArg::Sqlite => "features.backends.sqlite.db_path",
            StorageBackendArg::Turso => "features.backends.turso.db_path",
            StorageBackendArg::Rocksdb => "features.backends.rocksdb.db_path",
            StorageBackendArg::Postgres | StorageBackendArg::Foundationdb => {
                "features.backends.sqlite.db_path"
            }
        };
        overrides.push((path.to_string(), db_path.clone()));
    }
    if let Some(dsn) = &args.postgres_dsn {
        overrides.push(("features.backends.postgres.dsn".to_string(), dsn.clone()));
    }
    if let Some(max_pool_size) = args.postgres_max_pool_size {
        overrides.push((
            "features.backends.postgres.max_pool_size".to_string(),
            max_pool_size.to_string(),
        ));
    }
    if let Some(background_max_pool_size) = args.postgres_background_max_pool_size {
        overrides.push((
            "features.backends.postgres.background_max_pool_size".to_string(),
            background_max_pool_size.to_string(),
        ));
    }
    if let Some(tls) = args.postgres_tls {
        overrides.push((
            "features.backends.postgres.tls".to_string(),
            tls.to_string(),
        ));
    }
}

fn select_storage_backend(backend: &StorageBackendArg, overrides: &mut Vec<(String, String)>) {
    for path in [
        "features.backends.sqlite",
        "features.backends.turso",
        "features.backends.postgres",
        "features.backends.rocksdb",
        "features.backends.foundationdb",
        "features.backends.remote",
    ] {
        overrides.push((path.to_string(), "null".to_string()));
    }
    let selected_path = match backend {
        StorageBackendArg::Sqlite => "features.backends.sqlite",
        StorageBackendArg::Turso => "features.backends.turso",
        StorageBackendArg::Postgres => "features.backends.postgres",
        StorageBackendArg::Rocksdb => "features.backends.rocksdb",
        StorageBackendArg::Foundationdb => "features.backends.foundationdb",
    };
    overrides.push((selected_path.to_string(), "{}".to_string()));
}

fn parse_override_args(args: &[String]) -> Result<Vec<(String, String)>, ConfigError> {
    let mut parsed = Vec::new();
    for arg in args {
        for assignment in split_escaped(arg, ',') {
            if assignment.trim().is_empty() {
                continue;
            }
            let Some((path, value)) = split_assignment(&assignment) else {
                return Err(ConfigError::override_parse(format!(
                    "override assignment '{assignment}' must contain '='"
                )));
            };
            if path.trim().is_empty() {
                return Err(ConfigError::override_parse(
                    "override assignment path must not be empty",
                ));
            }
            parsed.push((unescape(path.trim()), unescape(value.trim())));
        }
    }
    Ok(parsed)
}

fn split_assignment(input: &str) -> Option<(&str, &str)> {
    let mut escaped = false;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '=' {
            return input.get(..index).zip(input.get(index + 1..));
        }
    }
    None
}

fn split_escaped(input: &str, separator: char) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            current.push('\\');
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == separator {
            parts.push(current);
            current = String::new();
        } else {
            current.push(ch);
        }
    }
    if escaped {
        current.push('\\');
    }
    parts.push(current);
    parts
}

fn unescape(input: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    if escaped {
        out.push('\\');
    }
    out
}
