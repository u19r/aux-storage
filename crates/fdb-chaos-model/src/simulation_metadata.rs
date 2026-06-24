use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::constants::ARTIFACT_SCHEMA_VERSION;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SimulationRunMetadata {
    pub schema_version: u32,
    pub workload: String,
    pub profile: String,
    pub seed: u64,
    pub buggify: String,
    pub test_file: String,
    pub library_path: String,
    pub library_name: String,
    pub rerun_command: String,
    pub options: BTreeMap<String, String>,
}

impl SimulationRunMetadata {
    #[must_use]
    pub fn new(input: SimulationRunMetadataInput) -> Self {
        Self {
            schema_version: ARTIFACT_SCHEMA_VERSION,
            workload: input.workload,
            profile: input.profile,
            seed: input.seed,
            buggify: input.buggify,
            test_file: input.test_file,
            library_path: input.library_path,
            library_name: input.library_name,
            rerun_command: input.rerun_command,
            options: input.options,
        }
    }
}

/// Constructor input kept separate to make call sites explicit as the schema
/// grows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimulationRunMetadataInput {
    pub workload: String,
    pub profile: String,
    pub seed: u64,
    pub buggify: String,
    pub test_file: String,
    pub library_path: String,
    pub library_name: String,
    pub rerun_command: String,
    pub options: BTreeMap<String, String>,
}
