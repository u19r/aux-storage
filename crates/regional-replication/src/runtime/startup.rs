use infrastructure_provider::{limits::InfrastructureLimits, physical_store::PhysicalRecordStore};
use replication_provider::runtime_configuration::{
    deployment::{DeploymentConfiguration, QualifiedConfiguration},
    limits::NativeBudgets,
};
use storage_provider::native_admission::NativeAttemptSource;

/// Operator input. Native facts are obtained only from the initialized port.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionalStartup {
    pub configuration: DeploymentConfiguration,
    pub budgets: NativeBudgets,
    pub physical_limits: InfrastructureLimits,
}

impl RegionalStartup {
    pub(super) fn qualify<P: NativeAttemptSource + PhysicalRecordStore>(
        &self,
        port: &P,
    ) -> Result<QualifiedConfiguration, super::super::transport::TransportError> {
        let configuration = self
            .configuration
            .clone()
            .qualify(self.budgets)
            .map_err(|_| super::super::transport::TransportError::Configuration)?;
        port.attempt_capacity()?
            .require_foreground_attempts(self.budgets.reserved_attempts.get(), 1)?;
        port.record_capacity(self.physical_limits)?
            .require_bounded_admission()?;
        port.bind_work_limits(self.budgets.attempt)?;
        Ok(configuration)
    }
}
