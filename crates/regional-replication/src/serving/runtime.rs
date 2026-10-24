use std::sync::Arc;

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec,
    runtime_configuration::deployment::QualifiedConfiguration,
};
use storage_provider::DynamoPort;

use crate::{journal::JournalStore, origin::OriginExecution};

/// Native write policy at the existing public Dynamo operation boundary.
/// The ordinary-only host retains its original DynamoOperations value.
pub struct RegionalDynamo<P> {
    pub(super) port: Arc<P>,
    pub(super) journal: Arc<JournalStore>,
    pub(super) configuration:
        Arc<replication_provider::runtime_configuration::deployment::DeploymentConfiguration>,
    pub(super) codec: Arc<CanonicalJsonCodec>,
}

impl<P: DynamoPort> RegionalDynamo<P> {
    pub(crate) fn try_new(
        port: Arc<P>,
        journal: Arc<JournalStore>,
        codec: Arc<CanonicalJsonCodec>,
        configuration: &QualifiedConfiguration,
    ) -> storage_types::StorageResult<Self> {
        journal.require_deployment(configuration.as_ref())?;
        journal.require_source_byte_allowances(
            configuration.native_budgets(),
            configuration.as_ref().limits.frame_events,
        )?;
        journal.require_origin_receipt_capacity(
            configuration.native_budgets().maximum_origin_receipt_bytes,
        )?;
        Ok(Self {
            port,
            journal,
            codec,
            configuration: Arc::new(configuration.as_ref().clone()),
        })
    }

    pub(super) fn origin_execution(&self) -> OriginExecution {
        OriginExecution {
            journal: Arc::clone(&self.journal),
            codec: Arc::clone(&self.codec),
        }
    }
}
