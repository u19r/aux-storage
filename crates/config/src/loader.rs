use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde_json::{Value, json};

use crate::{
    error::ConfigError, model::RootConfig, schema,
    sync_replication::validate_storage_sync_replication,
};

const MAX_INTERPOLATION_DEPTH: usize = 16;

#[derive(Debug, Clone)]
pub struct Config {
    pub root: RootConfig,
    pub effective_json: Value,
}

impl Config {
    pub fn schema_json() -> Result<String, ConfigError> {
        schema::schema_json()
    }

    pub fn write_schema_to(path: &Path) -> Result<(), ConfigError> {
        fs::write(path, Self::schema_json()?)?;
        Ok(())
    }

    #[must_use]
    pub fn effective_pretty(&self) -> String {
        serde_json::to_string_pretty(&self.effective_json).unwrap_or_default()
    }
}

pub fn load(path: &Path) -> Result<Arc<Config>, ConfigError> {
    load_with_overrides(path, &[])
}

pub fn load_with_overrides(
    path: &Path,
    cli_overrides: &[(String, String)],
) -> Result<Arc<Config>, ConfigError> {
    load_impl(Some(path), true, cli_overrides)
}

pub fn load_optional_with_overrides(
    path: Option<&Path>,
    cli_overrides: &[(String, String)],
) -> Result<Arc<Config>, ConfigError> {
    load_impl(path, false, cli_overrides)
}

fn load_impl(
    path: Option<&Path>,
    require_file: bool,
    cli_overrides: &[(String, String)],
) -> Result<Arc<Config>, ConfigError> {
    let mut value = serde_json::to_value(RootConfig::default())?;
    let mut overlay = match path {
        Some(path) => match fs::read_to_string(path) {
            Ok(raw) => serde_json::from_str(&raw)?,
            Err(error) if require_file => return Err(error.into()),
            Err(error) => {
                tracing::warn!(
                    target = "config",
                    path = %path.display(),
                    error = %error,
                    "Config file missing/unreadable; using defaults"
                );
                json!({})
            }
        },
        None => json!({}),
    };

    if !cli_overrides.is_empty() {
        apply_kv_overrides(&mut overlay, cli_overrides)?;
    }
    merge_config_overlay(&mut value, &overlay)?;
    let base_dir = path
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    resolve_placeholders_in_value(&mut value, &base_dir, "$")?;
    validate_against_schema(&value)?;

    let root: RootConfig = serde_json::from_value(value.clone())?;
    validate_root(&root)?;

    Ok(Arc::new(Config {
        root,
        effective_json: value,
    }))
}

fn validate_root(root: &RootConfig) -> Result<(), ConfigError> {
    validate_backend_details(&root.features.backends, "features.backends")?;
    if let Some(registry) = &root.features.storage_connections {
        if registry.default_connection.trim().is_empty() {
            return Err(ConfigError::validation(
                "features.storage_connections.default_connection must not be empty",
            ));
        }
        if registry.connections.is_empty() {
            return Err(ConfigError::validation(
                "features.storage_connections.connections must not be empty",
            ));
        }
        if !registry
            .connections
            .contains_key(registry.default_connection.as_str())
        {
            return Err(ConfigError::validation(format!(
                "features.storage_connections.default_connection '{}' not found in connections",
                registry.default_connection
            )));
        }
        for (connection_id, backends) in &registry.connections {
            validate_backend_details(
                backends,
                &format!("features.storage_connections.connections.{connection_id}"),
            )?;
        }
    }
    validate_queue_config(root)?;
    validate_pubsub_config(root)?;
    validate_storage_sync_replication(&root.features.storage_sync_replication)?;
    if root.features.storage_point_read_cache.capacity == 0 {
        return Err(ConfigError::validation(
            "features.storage_point_read_cache.capacity must be greater than 0",
        ));
    }
    if root
        .features
        .metrics
        .prometheus
        .bearer_token
        .as_deref()
        .is_some_and(|token| token.trim().is_empty())
    {
        return Err(ConfigError::validation(
            "features.metrics.prometheus.bearer_token must not be empty when set",
        ));
    }
    if let Some(max_bytes) = root.features.storage_point_read_cache.max_bytes
        && max_bytes == 0
    {
        return Err(ConfigError::validation(
            "features.storage_point_read_cache.max_bytes must be greater than 0 when set",
        ));
    }
    Ok(())
}

fn validate_queue_config(root: &RootConfig) -> Result<(), ConfigError> {
    if root.queue.account_id.trim().is_empty() {
        return Err(ConfigError::validation(
            "queue.account_id must not be empty",
        ));
    }
    if let Some(public_base_url) = root.queue.public_base_url.as_deref()
        && public_base_url.trim().is_empty()
    {
        return Err(ConfigError::validation(
            "queue.public_base_url must not be empty when set",
        ));
    }
    Ok(())
}

fn validate_pubsub_config(_root: &RootConfig) -> Result<(), ConfigError> {
    Ok(())
}

fn validate_backend_details(backends: &crate::Backends, path: &str) -> Result<(), ConfigError> {
    if let Some(postgres) = &backends.postgres {
        if postgres.dsn.trim().is_empty() {
            return Err(ConfigError::validation(format!(
                "{path}.postgres.dsn must not be empty"
            )));
        }
        if postgres.max_pool_size == 0 {
            return Err(ConfigError::validation(format!(
                "{path}.postgres.max_pool_size must be greater than 0"
            )));
        }
    }
    if let Some(remote) = &backends.remote {
        if remote.endpoint_urls.is_empty() {
            return Err(ConfigError::validation(format!(
                "{path}.remote.endpoint_urls must not be empty"
            )));
        }
        if remote
            .region
            .as_deref()
            .is_some_and(|region| region.trim().is_empty())
        {
            return Err(ConfigError::validation(format!(
                "{path}.remote.region must not be empty"
            )));
        }
        if let Some(credentials) = &remote.credentials
            && let Err(error) = credentials.validate()
        {
            return Err(ConfigError::validation(error.to_string()));
        }
    }
    Ok(())
}

fn validate_against_schema(value: &Value) -> Result<(), ConfigError> {
    let schema_value: Value = serde_json::from_str(&schema::schema_json()?)?;
    let validator = jsonschema::validator_for(&schema_value)
        .map_err(|error| ConfigError::schema(error.to_string()))?;
    let errors: Vec<String> = validator
        .iter_errors(value)
        .map(|error| error.to_string())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::validation(errors.join("; ")))
    }
}

fn merge_config_overlay(base: &mut Value, overlay: &Value) -> Result<(), ConfigError> {
    match (base, overlay) {
        (Value::Object(base_map), Value::Object(overlay_map)) => {
            for (key, overlay_value) in overlay_map {
                if overlay_value.is_null() {
                    base_map.insert(key.clone(), Value::Null);
                } else if key == "mode" {
                    base_map.insert(key.clone(), overlay_value.clone());
                } else if let Some(base_value) = base_map.get_mut(key) {
                    merge_config_overlay(base_value, overlay_value)?;
                } else {
                    base_map.insert(key.clone(), overlay_value.clone());
                }
            }
            Ok(())
        }
        (base, overlay) => {
            *base = overlay.clone();
            Ok(())
        }
    }
}

fn apply_kv_overrides(
    target: &mut Value,
    overrides: &[(String, String)],
) -> Result<(), ConfigError> {
    for (path, value) in overrides {
        let parsed = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.clone()));
        set_dot_path(target, path, parsed)?;
    }
    Ok(())
}

fn set_dot_path(target: &mut Value, path: &str, value: Value) -> Result<(), ConfigError> {
    let mut current = target;
    let mut parts = path.split('.').peekable();
    while let Some(part) = parts.next() {
        if part.is_empty() {
            return Err(ConfigError::validation(
                "override path contains an empty segment",
            ));
        }
        if parts.peek().is_none() {
            ensure_object(current)?.insert(part.to_string(), value);
            return Ok(());
        }
        current = ensure_object(current)?
            .entry(part.to_string())
            .or_insert_with(|| json!({}));
    }
    Ok(())
}

fn ensure_object(value: &mut Value) -> Result<&mut serde_json::Map<String, Value>, ConfigError> {
    if !value.is_object() {
        *value = json!({});
    }
    value
        .as_object_mut()
        .ok_or_else(|| ConfigError::validation("expected object while applying override"))
}

fn resolve_placeholders_in_value(
    value: &mut Value,
    base_dir: &Path,
    json_path: &str,
) -> Result<(), ConfigError> {
    match value {
        Value::String(raw) => {
            *raw = resolve_string(raw, base_dir, json_path, 0)?;
            Ok(())
        }
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                resolve_placeholders_in_value(item, base_dir, &format!("{json_path}[{index}]"))?;
            }
            Ok(())
        }
        Value::Object(map) => {
            for (key, value) in map.iter_mut() {
                resolve_placeholders_in_value(value, base_dir, &format!("{json_path}.{key}"))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn resolve_string(
    raw: &str,
    base_dir: &Path,
    json_path: &str,
    depth: usize,
) -> Result<String, ConfigError> {
    if depth > MAX_INTERPOLATION_DEPTH {
        return Err(ConfigError::interpolation(
            json_path,
            "resolver nesting limit exceeded",
        ));
    }

    let mut output = String::new();
    let mut index = 0;
    while index < raw.len() {
        let Some(rest) = raw.get(index..) else {
            return Err(ConfigError::interpolation(
                json_path,
                "resolver parser reached a non-character boundary",
            ));
        };
        if rest.starts_with("${") {
            let end = find_env_end(raw, index + 2).ok_or_else(|| {
                ConfigError::interpolation(json_path, "unterminated environment resolver")
            })?;
            let expression = raw.get(index + 2..end).ok_or_else(|| {
                ConfigError::interpolation(
                    json_path,
                    "environment resolver contains invalid UTF-8 boundaries",
                )
            })?;
            let name = resolve_string(expression, base_dir, json_path, depth + 1)?;
            if name.trim().is_empty() {
                return Err(ConfigError::interpolation(
                    json_path,
                    "empty environment resolver",
                ));
            }
            let value = std::env::var(&name).map_err(|_| {
                ConfigError::interpolation(
                    json_path,
                    format!("environment variable '{name}' is not set"),
                )
            })?;
            output.push_str(&value);
            index = end + 1;
        } else if rest.starts_with("file::") {
            let content_start = index + "file::".len();
            let end = find_file_end(raw, content_start).ok_or_else(|| {
                ConfigError::interpolation(json_path, "unterminated file resolver")
            })?;
            let expression = raw.get(content_start..end).ok_or_else(|| {
                ConfigError::interpolation(
                    json_path,
                    "file resolver contains invalid UTF-8 boundaries",
                )
            })?;
            let path = resolve_string(expression, base_dir, json_path, depth + 1)?;
            if path.trim().is_empty() {
                return Err(ConfigError::interpolation(
                    json_path,
                    "empty file resolver path",
                ));
            }
            let resolved_path = resolve_file_path(base_dir, &path);
            let contents = fs::read_to_string(&resolved_path).map_err(|error| {
                ConfigError::interpolation(
                    json_path,
                    format!("failed to read file '{}': {error}", resolved_path.display()),
                )
            })?;
            output.push_str(&contents);
            index = end + 2;
        } else if let Some(ch) = rest.chars().next() {
            output.push(ch);
            index += ch.len_utf8();
        } else {
            break;
        }
    }
    Ok(output)
}

fn resolve_file_path(base_dir: &Path, raw_path: &str) -> PathBuf {
    let path = Path::new(raw_path);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn find_env_end(raw: &str, mut index: usize) -> Option<usize> {
    let mut nested = 0usize;
    while index < raw.len() {
        let rest = raw.get(index..)?;
        if rest.starts_with("${") {
            nested += 1;
            index += 2;
        } else if rest.starts_with('}') {
            if nested == 0 {
                return Some(index);
            }
            nested -= 1;
            index += 1;
        } else if let Some(ch) = rest.chars().next() {
            index += ch.len_utf8();
        } else {
            return None;
        }
    }
    None
}

fn find_file_end(raw: &str, mut index: usize) -> Option<usize> {
    let mut env_depth = 0usize;
    while index < raw.len() {
        let rest = raw.get(index..)?;
        if rest.starts_with("${") {
            env_depth += 1;
            index += 2;
        } else if rest.starts_with('}') && env_depth > 0 {
            env_depth -= 1;
            index += 1;
        } else if rest.starts_with("::") && env_depth == 0 {
            return Some(index);
        } else if let Some(ch) = rest.chars().next() {
            index += ch.len_utf8();
        } else {
            return None;
        }
    }
    None
}
