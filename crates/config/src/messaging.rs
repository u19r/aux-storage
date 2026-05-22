use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::constants::{
    DEFAULT_QUEUE_MESSAGE_RETENTION_SECONDS, DEFAULT_QUEUE_VISIBILITY_TIMEOUT_SECONDS,
};

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QueueConfig {
    #[serde(default = "default_queue_account_id")]
    #[schemars(default = "default_queue_account_id")]
    pub account_id: String,
    #[serde(default)]
    #[schemars(default)]
    pub public_base_url: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub visibility_timeout_seconds: u32,
    #[serde(default = "default_queue_message_retention_seconds")]
    #[schemars(default = "default_queue_message_retention_seconds")]
    pub message_retention_seconds: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            account_id: default_queue_account_id(),
            public_base_url: None,
            visibility_timeout_seconds: DEFAULT_QUEUE_VISIBILITY_TIMEOUT_SECONDS,
            message_retention_seconds: default_queue_message_retention_seconds(),
        }
    }
}

fn default_queue_account_id() -> String {
    "000000000000".to_string()
}

fn default_queue_message_retention_seconds() -> u32 {
    DEFAULT_QUEUE_MESSAGE_RETENTION_SECONDS
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PubsubConfig {}
