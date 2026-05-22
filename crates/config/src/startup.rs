use std::{path::PathBuf, sync::Arc};

use thiserror::Error;

use crate::{Config, ConfigError, load_optional_with_overrides};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupProfileName {
    Test,
    SingleNode,
    Storage,
    RemoteStorage,
}

impl StartupProfileName {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Test => "test",
            Self::SingleNode => "single-node",
            Self::Storage => "storage",
            Self::RemoteStorage => "remote-storage",
        }
    }
}

impl std::str::FromStr for StartupProfileName {
    type Err = ConfigStartupError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "test" => Ok(Self::Test),
            "single-node" => Ok(Self::SingleNode),
            "storage" => Ok(Self::Storage),
            "remote-storage" => Ok(Self::RemoteStorage),
            other => Err(ConfigStartupError::invalid_input(format!(
                "unknown startup profile '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedProfileMetadata {
    pub profile: StartupProfileName,
    pub config_path: PathBuf,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct StartupIntent {
    pub config_path: Option<PathBuf>,
    pub profile: Option<String>,
    pub storage_nodes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct StartupLoadResult {
    pub config: Arc<Config>,
    pub generated_profile: Option<GeneratedProfileMetadata>,
}

#[derive(Debug, Error)]
pub enum ConfigStartupError {
    #[error("{message}")]
    InvalidInput { message: String },
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("failed to generate startup profile {profile:?}: {message}")]
    ProfileGeneration {
        profile: StartupProfileName,
        message: String,
    },
}

impl ConfigStartupError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }
}

pub fn load_startup_config<F, E>(
    intent: StartupIntent,
    mut generate_profile: F,
) -> Result<StartupLoadResult, ConfigStartupError>
where
    F: FnMut(StartupProfileName, Vec<String>) -> Result<GeneratedProfileMetadata, E>,
    E: std::fmt::Display,
{
    if intent.config_path.is_some() && intent.profile.is_some() {
        return Err(ConfigStartupError::invalid_input(
            "--config and --profile are mutually exclusive",
        ));
    }

    let generated_profile = if let Some(profile_name) = intent.profile {
        let profile = profile_name.parse::<StartupProfileName>()?;
        Some(
            generate_profile(profile, intent.storage_nodes).map_err(|source| {
                ConfigStartupError::ProfileGeneration {
                    profile,
                    message: source.to_string(),
                }
            })?,
        )
    } else {
        None
    };

    let config_path = intent.config_path.or_else(|| {
        generated_profile
            .as_ref()
            .map(|profile| profile.config_path.clone())
    });
    let config = load_optional_with_overrides(config_path.as_deref(), &[])?;

    Ok(StartupLoadResult {
        config,
        generated_profile,
    })
}
