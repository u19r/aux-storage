use thiserror::Error;
#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("invalid configuration JSON: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("invalid replication configuration: {0}")]
    Invalid(&'static str),
}
