use std::{collections::HashSet, net::SocketAddr, num::NonZeroU64, str::FromStr, time::Duration};

use replication_types::identity::{
    journal_partition::JournalPartitionCount,
    origin::JournalGeneration,
    replica::{DatabaseGroupId, RegionId, ReplicaId},
    resource::{DeploymentId, TenantId},
};
use serde::{Deserialize, Serialize};

use super::{
    delivery_budgets::DeliveryBudgets,
    error::ConfigurationError,
    limits::{BacklogBudgets, NativeBudgets, WorkLimits},
    transport_capacity::TransportCapacity,
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeploymentConfiguration {
    pub deployment: DeploymentId,
    /// Stable owner of the native tenant database, supplied by the host.
    pub tenant: TenantId,
    pub region: RegionId,
    pub group: DatabaseGroupId,
    pub replica: ReplicaId,
    pub journal_partitions: JournalPartitionCount,
    pub journal_generation: JournalGeneration,
    pub endpoint: SocketAddr,
    /// Existing operator-owned durable filesystem; peer messages cannot select
    /// it.
    #[serde(default)]
    pub archive_root: Option<std::path::PathBuf>,
    /// Missing or unavailable local time evidence defers retention only.
    #[serde(default)]
    pub time_trust: Option<super::time_trust::TimeTrustConfiguration>,
    pub peers: Vec<PeerConfiguration>,
    pub limits: WorkLimits,
    pub backlog: BacklogBudgets,
    pub timing: TimingConfiguration,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PeerConfiguration {
    pub region: RegionId,
    pub group: DatabaseGroupId,
    pub generation: JournalGeneration,
    pub replica: ReplicaId,
    pub endpoint: SocketAddr,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimingConfiguration {
    pub retry_min_ms: u64,
    pub retry_max_ms: u64,
    pub clock_alert_ms: NonZeroU64,
    pub request_timeout_ms: u64,
}
#[derive(Debug)]
pub struct QualifiedConfiguration {
    deployment: DeploymentConfiguration,
    native: NativeBudgets,
    transport: TransportCapacity,
    delivery: DeliveryBudgets,
}
impl DeploymentConfiguration {
    /// Validate enforced operator budgets. Store qualification precedes
    /// publication.
    pub fn qualify(
        self,
        native: NativeBudgets,
    ) -> Result<QualifiedConfiguration, ConfigurationError> {
        validate_deployment(&self)?;
        let transport = self.limits.qualify(native)?;
        self.backlog.qualify(native)?;
        let delivery = DeliveryBudgets::try_new(&self.timing, self.limits.frame_events)?;
        Ok(QualifiedConfiguration {
            deployment: self,
            native,
            transport,
            delivery,
        })
    }
}
impl QualifiedConfiguration {
    pub fn native_budgets(&self) -> NativeBudgets {
        self.native
    }
    pub fn transport_capacity(&self) -> TransportCapacity {
        self.transport
    }
    pub fn delivery_budgets(&self) -> DeliveryBudgets {
        self.delivery
    }
}
impl AsRef<DeploymentConfiguration> for QualifiedConfiguration {
    fn as_ref(&self) -> &DeploymentConfiguration {
        &self.deployment
    }
}
impl FromStr for DeploymentConfiguration {
    type Err = ConfigurationError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let configuration = serde_json::from_str(value)?;
        validate_deployment(&configuration)?;
        Ok(configuration)
    }
}
fn validate_deployment(configuration: &DeploymentConfiguration) -> Result<(), ConfigurationError> {
    configuration
        .timing
        .require_controller_budget(configuration.peers.len())?;
    if let Some(trust) = &configuration.time_trust {
        trust.validate()?;
    }
    if configuration
        .archive_root
        .as_ref()
        .is_some_and(|root| !root.is_absolute())
    {
        return Err(ConfigurationError::Invalid("archive root must be absolute"));
    }
    if configuration.endpoint.port() == 0 || configuration.endpoint.ip().is_unspecified() {
        return Err(ConfigurationError::Invalid("unusable listener endpoint"));
    }
    let mut replicas = HashSet::from([configuration.replica]);
    let mut endpoints = HashSet::from([configuration.endpoint]);
    let mut origins = HashSet::new();
    for peer in &configuration.peers {
        if peer.region == configuration.region
            || peer.endpoint.port() == 0
            || peer.endpoint.ip().is_unspecified()
            || !replicas.insert(peer.replica)
            || !endpoints.insert(peer.endpoint)
            || !origins.insert((&peer.region, peer.group, peer.generation))
        {
            return Err(ConfigurationError::Invalid(
                "duplicate, local or unusable peer destination",
            ));
        }
    }
    Ok(())
}

impl TimingConfiguration {
    /// Checked complete turn budget; saturation cannot turn a finite timeout
    /// into an unrepresentable scheduler or monotonic deadline.
    pub fn request_budget(&self, exchanges: u32) -> Result<Duration, ConfigurationError> {
        let milliseconds = self
            .request_timeout_ms
            .checked_mul(u64::from(exchanges))
            .ok_or(ConfigurationError::Invalid("request budget overflowed"))?;
        require_deadline_duration(milliseconds)
    }

    fn require_controller_budget(&self, peers: usize) -> Result<(), ConfigurationError> {
        if self.retry_min_ms == 0 || self.retry_max_ms < self.retry_min_ms {
            return Err(ConfigurationError::Invalid("invalid timing bounds"));
        }
        require_deadline_duration(self.retry_max_ms)?;
        let exchanges = u32::try_from(peers)
            .ok()
            .and_then(|count| count.checked_add(1))
            .and_then(|count| count.checked_mul(4))
            .ok_or(ConfigurationError::Invalid(
                "controller exchange count overflowed",
            ))?;
        self.request_budget(exchanges)?;
        Ok(())
    }
}

fn require_deadline_duration(milliseconds: u64) -> Result<Duration, ConfigurationError> {
    let duration = Duration::from_millis(milliseconds);
    if milliseconds == 0
        || i64::try_from(milliseconds).is_err()
        || duration.as_nanos() > u128::from(u64::MAX)
        || std::time::Instant::now().checked_add(duration).is_none()
    {
        return Err(ConfigurationError::Invalid(
            "timing interval cannot form a finite deadline",
        ));
    }
    Ok(duration)
}
