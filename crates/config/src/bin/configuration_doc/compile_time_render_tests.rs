use std::collections::BTreeSet;

use config::{CompileTimeManifest, CrateFeature, ManifestCrate};
use serde_json::json;

use super::{collect_property_refs, render_compile_time, type_info};

#[test]
fn type_info_with_nullable_array_and_ref_reports_both_shapes() {
    let property = json!({
        "type": ["null", "string", "array"],
        "items": { "$ref": "#/$defs/my::schema::NestedValue" }
    });

    let (field_type, nullable) = type_info(&property);

    assert_eq!(field_type, "array<my::schema::NestedValue> | string");
    assert!(nullable);
}

#[test]
fn collect_property_refs_recurses_into_nested_definition_references() {
    let definitions = json!({
        "$defs": {
            "TopLevel": {
                "type": "object",
                "properties": {
                    "nested": { "$ref": "#/$defs/Nested" }
                }
            },
            "Nested": {
                "anyOf": [
                    { "$ref": "#/$defs/Leaf" },
                    { "type": "null" }
                ]
            },
            "Leaf": {
                "type": "string"
            }
        }
    });
    let property = json!({
        "$ref": "#/$defs/TopLevel"
    });
    let mut refs = BTreeSet::new();

    collect_property_refs(
        &property,
        definitions
            .get("$defs")
            .and_then(serde_json::Value::as_object)
            .expect("schema definitions"),
        &mut refs,
    );

    assert_eq!(
        refs,
        BTreeSet::from([
            "Leaf".to_string(),
            "Nested".to_string(),
            "TopLevel".to_string(),
        ])
    );
}

#[test]
fn render_compile_time_skips_default_only_crates_and_lists_visible_features() {
    let manifest = CompileTimeManifest {
        version: "0.1.0".to_string(),
        crates: vec![
            ManifestCrate {
                name: "default-only".to_string(),
                manifest_path: "crates/default-only/Cargo.toml".to_string(),
                description: Some("Only default wiring".to_string()),
                default_members: vec!["dep:a".to_string()],
                features: vec![CrateFeature {
                    name: "default".to_string(),
                    members: vec!["dep:a".to_string()],
                }],
            },
            ManifestCrate {
                name: "featureful".to_string(),
                manifest_path: "crates/featureful/Cargo.toml".to_string(),
                description: Some("Has optional runtime knobs".to_string()),
                default_members: vec!["dep:core".to_string()],
                features: vec![
                    CrateFeature {
                        name: "default".to_string(),
                        members: vec!["dep:core".to_string()],
                    },
                    CrateFeature {
                        name: "s3".to_string(),
                        members: vec!["dep:aws-sdk-s3".to_string()],
                    },
                    CrateFeature {
                        name: "empty".to_string(),
                        members: Vec::new(),
                    },
                ],
            },
        ],
        extra: Default::default(),
    };
    let mut buffer = String::new();

    render_compile_time(&manifest, &mut buffer).expect("render compile-time section");

    assert!(buffer.contains("### `featureful`"));
    assert!(buffer.contains("Has optional runtime knobs"));
    assert!(buffer.contains("`default` | dep:core"));
    assert!(buffer.contains("`s3` | dep:aws-sdk-s3"));
    assert!(!buffer.contains("### `default-only`"));
    assert!(!buffer.contains("`empty`"));
}
