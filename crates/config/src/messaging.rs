use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ConfigError;

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct QueueConfig {
    /// Serves the SQS-compatible `/queue` route and starts its workers. Off by
    /// default; a backend without a queue provider fails startup when on.
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    #[serde(default = "default_queue_account_id")]
    #[schemars(default = "default_queue_account_id")]
    pub account_id: String,
    /// Region of SNS topic ARNs and of the SQS endpoint and dead-letter ARNs
    /// that SNS accepts. Set it before the first topic is created: a stored
    /// topic keeps the ARN it was created with.
    #[serde(default = "default_messaging_region")]
    #[schemars(default = "default_messaging_region")]
    pub region: String,
    /// Partition of SNS topic ARNs and of the SQS ARNs that SNS accepts.
    #[serde(default = "default_messaging_partition")]
    #[schemars(default = "default_messaging_partition")]
    pub partition: String,
    #[serde(default)]
    #[schemars(default)]
    pub public_base_url: Option<String>,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            account_id: default_queue_account_id(),
            region: default_messaging_region(),
            partition: default_messaging_partition(),
            public_base_url: None,
        }
    }
}

fn default_queue_account_id() -> String {
    "000000000000".to_string()
}

fn default_messaging_region() -> String {
    "us-east-1".to_string()
}

fn default_messaging_partition() -> String {
    "aws".to_string()
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PubsubConfig {
    /// Serves the SNS-compatible `/pubsub` route and starts the delivery
    /// runner. Off by default; a backend without a pubsub provider fails
    /// startup when on.
    #[serde(default)]
    #[schemars(default)]
    pub enabled: bool,
    /// Public base URL of the pubsub route, used for `SubscribeURL` and
    /// `UnsubscribeURL` in notifications. Without it the fields are empty.
    #[serde(default)]
    #[schemars(default)]
    pub public_base_url: Option<String>,
    #[serde(default)]
    #[schemars(default)]
    pub delivery: PubsubRunnerConfig,
}

/// The delivery runner of each node. `PubsubConfig::validate` checks every
/// rule at load and in `config check` (finding 9): `lease_seconds >= 2 *
/// request_timeout_seconds + 5` and at most 900; `request_timeout_seconds`
/// 1 to 60; `idle_probe_millis` 10 to 60,000; `max_in_flight` 1 to 1,024;
/// `per_destination` 1 to `max_in_flight`.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct PubsubRunnerConfig {
    #[serde(default = "default_max_in_flight")]
    #[schemars(default = "default_max_in_flight")]
    pub max_in_flight: usize,
    #[serde(default = "default_per_destination")]
    #[schemars(default = "default_per_destination")]
    pub per_destination: usize,
    #[serde(default = "default_request_timeout_seconds")]
    #[schemars(default = "default_request_timeout_seconds")]
    pub request_timeout_seconds: u64,
    #[serde(default = "default_lease_seconds")]
    #[schemars(default = "default_lease_seconds")]
    pub lease_seconds: u64,
    #[serde(default = "default_idle_probe_millis")]
    #[schemars(default = "default_idle_probe_millis")]
    pub idle_probe_millis: u64,
}

impl Default for PubsubRunnerConfig {
    fn default() -> Self {
        Self {
            max_in_flight: default_max_in_flight(),
            per_destination: default_per_destination(),
            request_timeout_seconds: default_request_timeout_seconds(),
            lease_seconds: default_lease_seconds(),
            idle_probe_millis: default_idle_probe_millis(),
        }
    }
}

impl PubsubConfig {
    /// The one owner of the delivery rules (finding 9). The runner builds
    /// its settings from a configuration that passed this check.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.delivery.validate()?;
        if let Some(base) = self.public_base_url.as_deref() {
            let is_http = url::Url::parse(base)
                .is_ok_and(|url| matches!(url.scheme(), "http" | "https") && url.has_host());
            if !is_http {
                return Err(ConfigError::validation(
                    "pubsub.public_base_url must be an http or https URL",
                ));
            }
        }
        Ok(())
    }
}

impl PubsubRunnerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        let rules = [
            (
                (1..=60).contains(&self.request_timeout_seconds),
                "pubsub.delivery.request_timeout_seconds must be 1 to 60",
            ),
            (
                self.lease_seconds >= self.request_timeout_seconds.saturating_mul(2) + 5,
                "pubsub.delivery.lease_seconds must be at least 2 x request_timeout_seconds + 5",
            ),
            (
                self.lease_seconds <= 900,
                "pubsub.delivery.lease_seconds must be at most 900",
            ),
            (
                (10..=60_000).contains(&self.idle_probe_millis),
                "pubsub.delivery.idle_probe_millis must be 10 to 60000",
            ),
            (
                (1..=1_024).contains(&self.max_in_flight),
                "pubsub.delivery.max_in_flight must be 1 to 1024",
            ),
            (
                (1..=self.max_in_flight).contains(&self.per_destination),
                "pubsub.delivery.per_destination must be 1 to max_in_flight",
            ),
        ];
        match rules.into_iter().find(|(is_valid, _)| !is_valid) {
            Some((_, message)) => Err(ConfigError::validation(message)),
            None => Ok(()),
        }
    }
}

fn default_max_in_flight() -> usize {
    64
}

fn default_per_destination() -> usize {
    8
}

fn default_request_timeout_seconds() -> u64 {
    10
}

fn default_lease_seconds() -> u64 {
    30
}

fn default_idle_probe_millis() -> u64 {
    1_000
}
