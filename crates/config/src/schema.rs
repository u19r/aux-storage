use schemars::schema_for;
use serde_json::{Value, json};

use crate::{constants::AUTO_GENERATED_SCHEMA_COMMENT, error::ConfigError, model::RootConfig};

pub fn schema_value() -> Result<Value, ConfigError> {
    let schema = schema_for!(RootConfig);
    let mut json_value = serde_json::to_value(schema).map_err(ConfigError::Json)?;
    patch_schema_json(&mut json_value);
    Ok(json_value)
}

pub fn schema_json() -> Result<String, ConfigError> {
    let value = schema_value()?;
    serde_json::to_string_pretty(&value).map_err(ConfigError::Json)
}

// Post-process generated JSON Schema to enforce constraints not expressible via
// derives
fn patch_schema_json(root: &mut Value) {
    if let Some(root_obj) = root.as_object_mut() {
        root_obj.insert(
            "$comment".to_string(),
            Value::String(AUTO_GENERATED_SCHEMA_COMMENT.to_string()),
        );
    }

    // 1) Enforce oneOf for Backends (exactly one backend block present).
    if let Some(defs) = root.get_mut("$defs").and_then(|v| v.as_object_mut()) {
        if let Some(backends) = defs.get_mut("Backends").and_then(|v| v.as_object_mut()) {
            let enforce_exactly_one = |keep: &str| {
                let mut properties = serde_json::Map::new();
                properties.insert(keep.to_string(), json!({ "not": { "type": "null" } }));
                for candidate in [
                    "sqlite",
                    "turso",
                    "postgres",
                    "rocksdb",
                    "foundationdb",
                    "remote",
                ] {
                    if candidate == keep {
                        continue;
                    }
                    properties.insert(candidate.to_string(), json!({ "type": "null" }));
                }

                json!({
                    "required": [keep],
                    "properties": Value::Object(properties),
                })
            };

            backends.insert(
                "oneOf".to_string(),
                json!([
                    enforce_exactly_one("sqlite"),
                    enforce_exactly_one("turso"),
                    enforce_exactly_one("postgres"),
                    enforce_exactly_one("rocksdb"),
                    enforce_exactly_one("foundationdb"),
                    enforce_exactly_one("remote"),
                ]),
            );
        }

        // 1.6) Ensure default:null is present for optional remote session
        // tokens.
        if let Some(creds) = defs
            .get_mut("RemoteStaticCredentialsConfig")
            .and_then(|v| v.as_object_mut())
            && let Some(props) = creds.get_mut("properties").and_then(|v| v.as_object_mut())
            && let Some(field) = props
                .get_mut("session_token")
                .and_then(|v| v.as_object_mut())
            && !field.contains_key("default")
        {
            field.insert("default".to_string(), Value::Null);
        }

        // 2) Cap jitter_percent maximum to 100.
        if let Some(jobs) = defs.get_mut("Jobs").and_then(|v| v.as_object_mut())
            && let Some(props) = jobs.get_mut("properties").and_then(|v| v.as_object_mut())
            && let Some(jitter) = props
                .get_mut("jitter_percent")
                .and_then(|v| v.as_object_mut())
        {
            jitter.insert("maximum".to_string(), serde_json::json!(100));
        }
    }
}
