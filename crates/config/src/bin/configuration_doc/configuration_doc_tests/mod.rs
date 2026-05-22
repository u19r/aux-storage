use std::collections::BTreeSet;

use config::{CrateFeature, ManifestCrate};
use serde_json::json;

use crate::{collect_property_refs, collect_visible_features, schema_definitions, type_info};

#[test]
fn type_info_reports_nullable_reference_unions() {
    let property = json!({
        "anyOf": [
            { "$ref": "#/$defs/authn.TenantMfaPolicy" },
            { "type": "null" }
        ]
    });

    let (type_label, nullable) = type_info(&property);

    assert_eq!(type_label, "TenantMfaPolicy");
    assert!(nullable);
}

#[test]
fn schema_definitions_supports_both_json_schema_locations() {
    let defs_schema = json!({
        "$defs": {
            "Widget": { "type": "object" }
        }
    });
    let legacy_schema = json!({
        "definitions": {
            "Widget": { "type": "object" }
        }
    });

    assert!(schema_definitions(&defs_schema).is_some());
    assert!(schema_definitions(&legacy_schema).is_some());
}

#[test]
fn collect_property_refs_walks_nested_items_and_properties() {
    let schema = json!({
        "$defs": {
            "Nested": {
                "type": "object",
                "properties": {
                    "leaf": { "$ref": "#/$defs/Leaf" }
                }
            },
            "Leaf": {
                "type": "string"
            }
        }
    });
    let definitions = schema_definitions(&schema).expect("definitions");
    let property = json!({
        "type": "array",
        "items": {
            "$ref": "#/$defs/Nested"
        }
    });

    let mut refs = BTreeSet::new();
    collect_property_refs(&property, definitions, &mut refs);

    assert_eq!(
        refs,
        BTreeSet::from(["Leaf".to_string(), "Nested".to_string()])
    );
}

#[test]
fn collect_visible_features_keeps_default_only_when_it_explains_real_members() {
    let manifest = ManifestCrate {
        name: "http-api".to_string(),
        manifest_path: "crates/http-api/Cargo.toml".to_string(),
        description: None,
        default_members: vec!["role-authn".to_string()],
        features: vec![
            CrateFeature {
                name: "default".to_string(),
                members: vec!["role-authn".to_string()],
            },
            CrateFeature {
                name: "role-authn".to_string(),
                members: vec!["authn".to_string()],
            },
            CrateFeature {
                name: "unused".to_string(),
                members: Vec::new(),
            },
        ],
    };

    let visible = collect_visible_features(&manifest);
    let visible_names = visible
        .iter()
        .map(|feature| feature.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(visible_names, vec!["default", "role-authn"]);
}
