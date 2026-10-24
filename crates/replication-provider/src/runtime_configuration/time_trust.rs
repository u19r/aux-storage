//! Operator trust selects eligible evidence; it never asserts qualified clock
//! accuracy.
use std::{
    net::IpAddr,
    num::{NonZeroU32, NonZeroU64, NonZeroUsize},
    path::PathBuf,
    time::Duration,
};

use replication_types::protocol::CanonicalDigest;
use serde::{Deserialize, Serialize};

use super::error::ConfigurationError;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeTrustConfiguration {
    pub policy: CanonicalDigest,
    pub minimum_retention_ms: NonZeroU64,
    pub executable: PathBuf,
    pub socket: PathBuf,
    pub report_version: ChronyReportVersion,
    pub sources: Vec<TrustedTimeSource>,
    pub acquisition_timeout_ms: NonZeroU64,
    pub maximum_stdout_bytes: NonZeroUsize,
    pub maximum_measurement_age_ms: NonZeroU64,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChronyReportVersion {
    V4_6,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedTimeSource {
    pub address: IpAddr,
    pub authentication: SourceAuthentication,
    pub upstream_accuracy_ns: NonZeroU64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SourceAuthentication {
    Nts {
        algorithm: NtsAlgorithm,
    },
    SymmetricKey {
        key_id: NonZeroU32,
        algorithm: SymmetricAlgorithm,
    },
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NtsAlgorithm {
    AesSivCmac256,
    Aes128GcmSiv,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SymmetricAlgorithm {
    Sha256,
    Sha384,
    Sha512,
    Sha3_256,
    Sha3_384,
    Sha3_512,
    Aes128,
    Aes256,
}

impl TimeTrustConfiguration {
    pub fn require_retention_policy(
        &self,
        policy: CanonicalDigest,
        minimum_retention_ms: NonZeroU64,
    ) -> Result<(), ConfigurationError> {
        if policy != self.policy || minimum_retention_ms < self.minimum_retention_ms {
            return Err(ConfigurationError::Invalid(
                "trim policy differs from host retention policy",
            ));
        }
        Ok(())
    }

    pub(super) fn validate(&self) -> Result<(), ConfigurationError> {
        require_absolute_monitoring_path(&self.executable)?;
        require_absolute_monitoring_path(&self.socket)?;
        if self.sources.is_empty()
            || self
                .sources
                .windows(2)
                .any(|pair| pair[0].address >= pair[1].address)
        {
            return Err(ConfigurationError::Invalid(
                "time sources must be ordered and unique",
            ));
        }
        if self
            .sources
            .iter()
            .any(|source| source.address.is_unspecified() || source.address.is_multicast())
        {
            return Err(ConfigurationError::Invalid("unusable time source address"));
        }
        let duration = Duration::from_millis(self.acquisition_timeout_ms.get());
        if i64::try_from(self.acquisition_timeout_ms.get()).is_err()
            || std::time::Instant::now().checked_add(duration).is_none()
        {
            return Err(ConfigurationError::Invalid(
                "time acquisition cannot form a finite deadline",
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for TimeTrustConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TimeTrustConfiguration")
            .finish_non_exhaustive()
    }
}

fn require_absolute_monitoring_path(path: &std::path::Path) -> Result<(), ConfigurationError> {
    let Some(text) = path.to_str() else {
        return Err(ConfigurationError::Invalid("invalid monitoring path"));
    };
    if !path.is_absolute() || text.contains([',', '\0', '\n', '\r']) {
        return Err(ConfigurationError::Invalid(
            "monitoring requires one absolute local path",
        ));
    }
    Ok(())
}
