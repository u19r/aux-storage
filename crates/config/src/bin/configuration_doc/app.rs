use std::{
    error::Error,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

use chrono::Utc;
use clap::Parser;
use config::{CompileTimeManifest, CrateFeature, ManifestCrate};
use serde_json::Value;

#[derive(Parser, Debug)]
#[command(
    about = "Generate CONFIGURATION.md from workspace metadata and schemas",
    version
)]
struct Cmd {
    /// Location of the compile-time manifest TOML.
    #[arg(long, default_value = "config/compile-time.toml")]
    compile_manifest: PathBuf,
    /// Location of the app-start JSON schema.
    #[arg(long, default_value = "crates/config/config.schema.json")]
    app_schema: PathBuf,
    /// Location of the runtime tenant configuration JSON schema.
    #[arg(long, default_value = "config/tenant-config.schema.json")]
    runtime_schema: PathBuf,
    /// Output markdown path.
    #[arg(long, default_value = "CONFIGURATION.md")]
    output: PathBuf,
}

pub(crate) fn run() -> Result<(), Box<dyn Error>> {
    let cmd = Cmd::parse();
    let manifest = CompileTimeManifest::read_from_path(cmd.compile_manifest.as_path())?;
    let app_schema_value: Value = load_json(&cmd.app_schema)?;
    let runtime_schema_value: Value = load_json(&cmd.runtime_schema)?;

    let mut contents = String::new();
    render_header(&mut contents)?;
    render_compile_time(&manifest, &mut contents)?;
    render_app_start(&app_schema_value, &mut contents)?;
    render_runtime(&runtime_schema_value, &mut contents)?;

    if let Some(parent) = cmd.output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&cmd.output, contents)?;
    println!("Wrote configuration guide to {}", cmd.output.display());
    Ok(())
}

fn load_json(path: &Path) -> Result<Value, Box<dyn Error>> {
    let data = fs::read_to_string(path)?;
    let value: Value = serde_json::from_str(&data)?;
    Ok(value)
}

fn render_header(buffer: &mut String) -> Result<(), std::fmt::Error> {
    let timestamp = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    writeln!(buffer, "# aux-storage Configuration Guide")?;
    writeln!(buffer)?;
    writeln!(
        buffer,
        "_Last generated: {} UTC_\n",
        timestamp.trim_end_matches('Z')
    )?;
    Ok(())
}

pub(crate) fn render_compile_time(
    manifest: &CompileTimeManifest,
    buffer: &mut String,
) -> Result<(), std::fmt::Error> {
    writeln!(buffer, "## Compile-time\n")?;

    if manifest.crates.is_empty() {
        writeln!(
            buffer,
            "No crate-level features detected in the workspace.\n"
        )?;
        return Ok(());
    }

    for krate in &manifest.crates {
        if !has_non_default_features(krate) {
            continue;
        }
        let visible_features = collect_visible_features(krate);
        if visible_features.is_empty() || all_default(&visible_features) {
            continue;
        }

        writeln!(buffer, "### `{}`\n", krate.name)?;
        if let Some(description) = &krate.description {
            writeln!(buffer, "{description}\n")?;
        }

        if !krate.default_members.is_empty() {
            writeln!(
                buffer,
                "- **Default feature includes:** {}\n",
                format_list(&krate.default_members)
            )?;
        }

        writeln!(buffer, "| Feature | Enables |")?;
        writeln!(buffer, "| --- | --- |")?;
        for feature in visible_features {
            let enables = if feature.members.is_empty() {
                "-".to_string()
            } else {
                feature.members.join("<br>")
            };
            writeln!(buffer, "| `{}` | {} |", feature.name, enables)?;
        }
        writeln!(buffer)?;
    }

    Ok(())
}

fn render_app_start(schema: &Value, buffer: &mut String) -> Result<(), std::fmt::Error> {
    writeln!(buffer, "## App Start\n")?;

    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        writeln!(
            buffer,
            "App start schema is unavailable. Regenerate `crates/config/config.schema.json`.\n"
        )?;
        return Ok(());
    };
    let required_fields: std::collections::BTreeSet<String> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();

    writeln!(
        buffer,
        "| Field | Type | Required | Default | Description |"
    )?;
    writeln!(buffer, "| --- | --- | --- | --- | --- |")?;

    for (name, property) in properties.iter() {
        let (field_type, _) = type_info(property);
        let is_required = if required_fields.contains(name) {
            "yes"
        } else {
            "no"
        };
        let default_value = property
            .get("default")
            .map(compact_json)
            .unwrap_or_else(|| "-".to_string());
        let description = property
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .replace('\n', " ");

        writeln!(
            buffer,
            "| `{}` | {} | {} | {} | {} |",
            name, field_type, is_required, default_value, description
        )?;
    }

    writeln!(buffer)?;
    Ok(())
}

fn render_runtime(schema: &Value, buffer: &mut String) -> Result<(), std::fmt::Error> {
    writeln!(buffer, "## Runtime\n")?;
    writeln!(
        buffer,
        "Tenant runtime configuration is applied via tenant-api `POST /tenant/config` using \
         `SetTenantConfigRequest` (resolved by the Host header) and persisted in DynamoDB via \
         `TenantConfigEntity`. The table below reflects the operator-facing runtime payload \
         schema.\n"
    )?;

    let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
        writeln!(
            buffer,
            "Runtime schema is unavailable. Regenerate `config/tenant-config.schema.json`.\n"
        )?;
        return Ok(());
    };

    let required_fields: std::collections::BTreeSet<String> = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default();

    let mut property_names: Vec<&String> = properties.keys().collect();
    property_names.sort();

    let definitions = schema_definitions(schema);
    let mut referenced_defs = std::collections::BTreeSet::new();

    writeln!(buffer, "| Field | Type | Nullable | Description |")?;
    writeln!(buffer, "| --- | --- | --- | --- |")?;

    for name in property_names {
        let Some(property) = properties.get(name) else {
            continue;
        };
        let (field_type, property_nullable) = type_info(property);
        let is_required = required_fields.contains(name);
        let nullable = if is_required { property_nullable } else { true };
        let description = property
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("-")
            .replace('\n', " ");

        writeln!(
            buffer,
            "| `{}` | {} | {} | {} |",
            name,
            field_type,
            if nullable { "yes" } else { "no" },
            description
        )?;

        if let Some(defs) = definitions {
            collect_property_refs(property, defs, &mut referenced_defs);
        }
    }
    writeln!(buffer)?;

    if let Some(defs) = definitions
        && !referenced_defs.is_empty()
    {
        writeln!(buffer, "### Runtime Field Definitions\n")?;
        for def_name in referenced_defs {
            if let Some(definition) = defs.get(&def_name) {
                render_definition(buffer, &def_name, definition, defs)?;
            }
        }
    }

    Ok(())
}

fn compact_json(value: &Value) -> String {
    match value {
        Value::String(s) => s.to_string(),
        Value::Array(items) => {
            let parts: Vec<String> = items.iter().map(compact_json).collect();
            format!("[{}]", parts.join(", "))
        }
        Value::Object(map) => {
            let parts: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", k, compact_json(v)))
                .collect();
            format!("{{{}}}", parts.join(", "))
        }
        Value::Null => "null".to_string(),
        other => other.to_string(),
    }
}

fn format_list(items: &[String]) -> String {
    if items.is_empty() {
        "-".to_string()
    } else {
        items.join(", ")
    }
}

pub(crate) fn schema_definitions(schema: &Value) -> Option<&serde_json::Map<String, Value>> {
    schema
        .get("$defs")
        .and_then(Value::as_object)
        .or_else(|| schema.get("definitions").and_then(Value::as_object))
}

pub(crate) fn type_info(property: &Value) -> (String, bool) {
    if let Some(Value::String(single)) = property.get("type") {
        match single.as_str() {
            "null" => return ("null".to_string(), true),
            "array" => {
                if let Some(items) = property.get("items") {
                    let (inner, nullable) = type_info(items);
                    return (format!("array<{}>", inner), nullable);
                }
                return ("array".to_string(), false);
            }
            _ => return (single.to_string(), false),
        }
    }

    if let Some(Value::Array(items)) = property.get("type") {
        let mut nullable = false;
        let mut parts = Vec::new();
        for item in items {
            if let Some(single) = item.as_str() {
                if single == "null" {
                    nullable = true;
                } else if single == "array" {
                    if let Some(child) = property.get("items") {
                        let (inner, child_nullable) = type_info(child);
                        nullable |= child_nullable;
                        parts.push(format!("array<{}>", inner));
                    } else {
                        parts.push("array".to_string());
                    }
                } else {
                    parts.push(single.to_string());
                }
            }
        }
        if parts.is_empty() {
            parts.push("object".to_string());
        } else {
            parts.sort();
            parts.dedup();
        }
        return (parts.join(" | "), nullable);
    }

    if let Some(reference) = property.get("$ref").and_then(Value::as_str) {
        return (format_ref_name(reference), false);
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(Value::Array(items)) = property.get(keyword) {
            let mut parts = Vec::new();
            let mut nullable = false;
            for item in items {
                let (ty, is_nullable) = type_info(item);
                nullable |= is_nullable;
                if ty != "null" {
                    parts.push(ty);
                }
            }
            if parts.is_empty() {
                parts.push("object".to_string());
            } else {
                parts.sort();
                parts.dedup();
            }
            return (parts.join(" | "), nullable);
        }
    }

    if let Some(items) = property.get("items") {
        let (inner, nullable) = type_info(items);
        return (format!("array<{}>", inner), nullable);
    }

    if property.get("enum").is_some() {
        return ("enum".to_string(), false);
    }

    ("object".to_string(), false)
}

fn format_ref_name(reference: &str) -> String {
    let raw = reference
        .rsplit('/')
        .next()
        .unwrap_or(reference)
        .to_string();
    raw.rsplit('.')
        .next()
        .map(ToString::to_string)
        .unwrap_or(raw)
}

fn reference_name(reference: &str) -> Option<String> {
    reference
        .strip_prefix("#/definitions/")
        .or_else(|| reference.strip_prefix("#/$defs/"))
        .map(ToString::to_string)
}

pub(crate) fn collect_property_refs(
    property: &Value,
    definitions: &serde_json::Map<String, Value>,
    acc: &mut std::collections::BTreeSet<String>,
) {
    if let Some(reference) = property.get("$ref").and_then(Value::as_str)
        && let Some(name) = reference_name(reference)
        && acc.insert(name.clone())
        && let Some(definition) = definitions.get(&name)
    {
        collect_definition_refs(definition, definitions, acc);
    }

    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(Value::Array(items)) = property.get(keyword) {
            for item in items {
                collect_property_refs(item, definitions, acc);
            }
        }
    }

    if let Some(items) = property.get("items") {
        collect_property_refs(items, definitions, acc);
    }

    if let Some(props) = property.get("properties").and_then(Value::as_object) {
        for value in props.values() {
            collect_property_refs(value, definitions, acc);
        }
    }
}

fn collect_definition_refs(
    definition: &Value,
    definitions: &serde_json::Map<String, Value>,
    acc: &mut std::collections::BTreeSet<String>,
) {
    collect_property_refs(definition, definitions, acc);
}

fn render_definition(
    buffer: &mut String,
    raw_name: &str,
    definition: &Value,
    _definitions: &serde_json::Map<String, Value>,
) -> Result<(), std::fmt::Error> {
    let title = raw_name.rsplit('.').next().unwrap_or(raw_name).to_string();
    writeln!(buffer, "#### `{}`\n", title)?;

    if let Some(description) = definition.get("description").and_then(Value::as_str) {
        writeln!(buffer, "{}\n", description)?;
    }

    if let Some(enum_values) = definition.get("enum").and_then(Value::as_array) {
        let values: Vec<String> = enum_values
            .iter()
            .map(|value| match value {
                Value::String(s) => s.to_string(),
                other => other.to_string(),
            })
            .collect();
        writeln!(buffer, "Allowed values: `{}`.\n", values.join("`, `"))?;
        return Ok(());
    }

    if let Some(properties) = definition.get("properties").and_then(Value::as_object) {
        let required_fields: std::collections::BTreeSet<String> = definition
            .get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default();

        let mut names: Vec<&String> = properties.keys().collect();
        names.sort();

        writeln!(buffer, "| Field | Type | Nullable | Description |")?;
        writeln!(buffer, "| --- | --- | --- | --- |")?;
        for name in names {
            let Some(property) = properties.get(name) else {
                continue;
            };
            let (field_type, property_nullable) = type_info(property);
            let is_required = required_fields.contains(name);
            let nullable = if is_required { property_nullable } else { true };
            let description = property
                .get("description")
                .and_then(Value::as_str)
                .unwrap_or("-")
                .replace('\n', " ");
            writeln!(
                buffer,
                "| `{}` | {} | {} | {} |",
                name,
                field_type,
                if nullable { "yes" } else { "no" },
                description
            )?;
        }
        writeln!(buffer)?;
        return Ok(());
    }

    let (type_label, _) = type_info(definition);
    writeln!(buffer, "Type: `{}`.\n", type_label)?;
    Ok(())
}

fn has_non_default_features(krate: &ManifestCrate) -> bool {
    krate
        .features
        .iter()
        .any(|feature| feature.name != "default")
}

pub(crate) fn collect_visible_features(krate: &ManifestCrate) -> Vec<&CrateFeature> {
    let has_non_default_visible = krate
        .features
        .iter()
        .any(|feature| feature.name != "default" && !feature.members.is_empty());

    krate
        .features
        .iter()
        .filter(|feature| {
            if feature.name == "default" {
                if has_non_default_visible {
                    return true;
                }
                !feature.members.is_empty()
            } else {
                !feature.members.is_empty()
            }
        })
        .collect()
}

fn all_default(features: &[&CrateFeature]) -> bool {
    features.iter().all(|feature| feature.name == "default")
}
