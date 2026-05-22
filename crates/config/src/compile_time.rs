use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompileTimeManifest {
    pub version: String,
    pub crates: Vec<ManifestCrate>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

impl CompileTimeManifest {
    pub fn empty() -> Self {
        Self {
            version: "0.1.0".to_string(),
            crates: Vec::new(),
            extra: BTreeMap::new(),
        }
    }

    pub fn read_from_path(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let manifest = toml::from_str(&content)?;
        Ok(manifest)
    }

    pub fn write_to_path(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let serialized = toml::to_string_pretty(self)?;
        fs::write(path, serialized)
            .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
    }

    #[cfg(feature = "compile-time-manifest")]
    pub fn from_metadata(metadata: &cargo_metadata::Metadata) -> Self {
        let package_map: BTreeMap<cargo_metadata::PackageId, _> = metadata
            .packages
            .iter()
            .map(|pkg| (pkg.id.clone(), pkg))
            .collect();

        let mut crates = metadata
            .workspace_members
            .iter()
            .filter_map(|pkg_id| package_map.get(pkg_id))
            .map(|pkg| ManifestCrate::from_package(pkg))
            .filter(|entry| !entry.features.is_empty())
            .collect::<Vec<_>>();
        crates.sort_by(|left, right| left.name.cmp(&right.name));

        Self {
            version: "0.1.0".to_string(),
            crates,
            extra: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCrate {
    pub name: String,
    pub manifest_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub default_members: Vec<String>,
    pub features: Vec<CrateFeature>,
}

impl ManifestCrate {
    #[cfg(feature = "compile-time-manifest")]
    fn from_package(pkg: &cargo_metadata::Package) -> Self {
        let mut features = pkg
            .features
            .iter()
            .map(|(name, members)| CrateFeature::new(name, members))
            .collect::<Vec<_>>();
        features.sort_by(|left, right| left.name.cmp(&right.name));

        let default_members = pkg
            .features
            .get("default")
            .map(|members| sorted_unique(members))
            .unwrap_or_default();

        Self {
            name: pkg.name.to_string(),
            manifest_path: pkg.manifest_path.to_string(),
            description: pkg.description.clone(),
            default_members,
            features,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrateFeature {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub members: Vec<String>,
}

impl CrateFeature {
    #[cfg(feature = "compile-time-manifest")]
    fn new(name: &str, members: &[String]) -> Self {
        Self {
            name: name.to_string(),
            members: sorted_unique(members),
        }
    }
}

#[cfg(feature = "compile-time-manifest")]
fn sorted_unique(items: &[String]) -> Vec<String> {
    let mut deduped = items.iter().map(ToString::to_string).collect::<Vec<_>>();
    deduped.sort();
    deduped.dedup();
    deduped
}
