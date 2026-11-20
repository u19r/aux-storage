//! Connection settings of the remote SQS-compatible queue client.
use std::time::Duration;

use storage_types::{StorageError, StorageResult};

#[derive(Debug, Clone)]
pub struct RemoteQueueSettings {
    pub endpoint_urls: Vec<String>,
    pub region: Option<String>,
    pub tls: bool,
    pub credentials: RemoteCredentialStrategy,
    pub timeouts: Option<RemoteTimeoutOverrides>,
    pub sigv4: RemoteSigv4Settings,
}

impl RemoteQueueSettings {
    pub fn validate(&self) -> StorageResult<()> {
        if self.endpoint_urls.is_empty() {
            return Err(StorageError::validation(
                "remote queue requires at least one endpoint URL",
            ));
        }
        if self.sigv4.enabled && self.region.as_deref().is_none_or(str::is_empty) {
            return Err(StorageError::validation(
                "remote queue with SigV4 enabled requires a region",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum RemoteCredentialStrategy {
    DefaultChain,
    Static(RemoteStaticCredentials),
}

/// `Debug` redacts every field: all three are credentials.
#[derive(Clone)]
pub struct RemoteStaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl std::fmt::Debug for RemoteStaticCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteStaticCredentials")
            .field("access_key_id", &format_args!("<redacted>"))
            .field("secret_access_key", &format_args!("<redacted>"))
            .field("session_token", &format_args!("<redacted>"))
            .finish()
    }
}

/// Longest default connect timeout: a connect that fails ends well before the
/// request deadline, so it is a known-not-sent failure.
const MAX_DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Default)]
pub struct RemoteTimeoutOverrides {
    /// Default: a quarter of the request timeout, at most 2 s.
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
}

impl RemoteTimeoutOverrides {
    #[must_use]
    pub fn request_timeout(&self) -> Option<Duration> {
        self.request_timeout_ms.map(Duration::from_millis)
    }

    /// The configured connect timeout, else a quarter of the request timeout
    /// capped at 2 s (2 s without a request timeout).
    #[must_use]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout_ms.map_or_else(
            || {
                self.request_timeout()
                    .map_or(MAX_DEFAULT_CONNECT_TIMEOUT, |request| {
                        (request / 4).min(MAX_DEFAULT_CONNECT_TIMEOUT)
                    })
            },
            Duration::from_millis,
        )
    }
}

#[derive(Debug, Clone)]
pub struct RemoteSigv4Settings {
    pub enabled: bool,
    pub service_name: String,
}

impl Default for RemoteSigv4Settings {
    fn default() -> Self {
        Self {
            enabled: false,
            service_name: "queue".to_string(),
        }
    }
}
