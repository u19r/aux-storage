use infrastructure_provider::limits::InfrastructureLimits;
use replication_types::identity::{
    journal_partition::JournalPartitionCount,
    origin::JournalGeneration,
    replica::{DatabaseGroupId, RegionId},
    resource::TenantId,
};
use storage_types::StorageResult;

use crate::native_record::NativeRecords;

pub(crate) enum NativeAttempt {
    Items(storage_types::AttemptMode),
    Schema,
}

/// One fixed regional database journal generation. No API-process identity.
pub struct JournalStore {
    pub(super) region: RegionId,
    pub(super) tenant: TenantId,
    pub(super) group: DatabaseGroupId,
    pub(super) generation: JournalGeneration,
    pub(super) partitions: JournalPartitionCount,
    pub(super) records: NativeRecords,
    pub(super) source_byte_allowances:
        std::sync::OnceLock<super::source_declaration::SourcePublicationLimits>,
    retention_time: crate::trim::time::state::RetentionTimeState,
    pub(super) clock_alert_ms: std::num::NonZeroU64,
    pub(super) time_trust: std::sync::OnceLock<
        Option<replication_provider::runtime_configuration::time_trust::TimeTrustConfiguration>,
    >,
    pub(crate) archive: std::sync::OnceLock<std::sync::Arc<crate::archive::CanonicalArchive>>,
    pub(crate) native_admission:
        std::sync::Arc<storage_provider::native_admission::NativeAdmissionBinding>,
}

impl JournalStore {
    pub fn try_new(
        identity: (RegionId, DatabaseGroupId, JournalGeneration, TenantId),
        partitions: JournalPartitionCount,
        limits: InfrastructureLimits,
        policy: (
            replication_provider::runtime_configuration::limits::BacklogBudgets,
            std::num::NonZeroU64,
        ),
    ) -> StorageResult<Self> {
        let (region, group, generation, tenant) = identity;
        let capacity_key = Self::build_record_key(
            (&region, group, generation),
            crate::constants::HISTORY_CAPACITY_SUFFIX.to_vec(),
        )?;
        Ok(Self {
            region,
            tenant,
            group,
            generation,
            partitions,
            records: NativeRecords::try_new(limits, (capacity_key, tenant, policy.0))?,
            clock_alert_ms: policy.1,
            source_byte_allowances: std::sync::OnceLock::new(),
            retention_time: crate::trim::time::state::RetentionTimeState::default(),
            time_trust: std::sync::OnceLock::new(),
            archive: std::sync::OnceLock::new(),
            native_admission: std::sync::Arc::new(
                storage_provider::native_admission::NativeAdmissionBinding::default(),
            ),
        })
    }
}

impl JournalStore {
    pub(crate) async fn with_admitted_attempt<P, R, F>(
        self: std::sync::Arc<Self>,
        input: (&P, storage_provider::native_admission::NativeWork),
        kind: crate::journal::NativeAttempt,
        operation: F,
    ) -> StorageResult<R>
    where
        P: storage_provider::DynamoPort,
        R: Send + 'static,
        F: for<'native, 'borrow> FnOnce(
                &'borrow mut P::Attempt<'native>,
                storage_types::TimestampMillis,
            ) -> std::pin::Pin<
                Box<dyn Future<Output = StorageResult<R>> + Send + 'borrow>,
            > + Send
            + 'static,
    {
        let (port, work) = input;
        let port = port.clone();
        self.native_admission
            .settle(work, move || async move {
                let observed_at = port.now();
                match kind {
                    crate::journal::NativeAttempt::Items(mode) => {
                        port.with_dynamo_attempt(mode, move |attempt| {
                            operation(attempt, observed_at)
                        })
                        .await
                    }
                    crate::journal::NativeAttempt::Schema => {
                        port.with_dynamo_schema_attempt(move |attempt| {
                            operation(attempt, observed_at)
                        })
                        .await
                    }
                }
            })
            .await
    }

    pub(crate) async fn require_acceptance_capacity<
        A: infrastructure_provider::physical_attempt::PhysicalRecordAttempt,
    >(
        &self,
        attempt: &mut A,
    ) -> StorageResult<()> {
        self.records.require_acceptance_capacity(attempt).await
    }

    pub(crate) fn physical_limits(&self) -> InfrastructureLimits {
        self.records.limits
    }

    pub(crate) fn require_original_capacity(
        &self,
        native: replication_provider::runtime_configuration::limits::NativeBudgets,
        maximum_events: std::num::NonZeroUsize,
    ) -> StorageResult<()> {
        self.records.require_original_capacity(native)?;
        self.require_initialization_capacity()?;
        self.bind_origin_receipt_capacity(native.maximum_origin_receipt_bytes)?;
        self.source_byte_allowances.get_or_init(|| {
            super::source_declaration::SourcePublicationLimits::new(native, maximum_events)
        });
        self.require_source_byte_allowances(native, maximum_events)
    }

    pub(crate) fn require_deployment(
        &self,
        configuration: &replication_provider::runtime_configuration::deployment::DeploymentConfiguration,
    ) -> StorageResult<()> {
        self.records
            .capacity
            .require_policy(&configuration.backlog)?;
        if self.tenant != configuration.tenant
            || self.region != configuration.region
            || self.group != configuration.group
            || self.partitions != configuration.journal_partitions
            || self.generation != configuration.journal_generation
            || self.clock_alert_ms != configuration.timing.clock_alert_ms
            || self.time_trust.get().and_then(Option::as_ref) != configuration.time_trust.as_ref()
        {
            return Err(storage_types::StorageError::unsupported(
                "regional deployment does not match native journal",
            ));
        }
        Ok(())
    }
}

impl JournalStore {
    pub(crate) fn bind_time_trust(
        &self,
        configuration: &replication_provider::runtime_configuration::deployment::QualifiedConfiguration,
    ) -> StorageResult<()> {
        self.time_trust
            .set(configuration.as_ref().time_trust.clone())
            .map_err(|_| {
                storage_types::StorageError::unsupported("host time policy is already bound")
            })
    }

    pub(super) fn require_public_trim_policy(
        &self,
        proposal: &crate::trim::proposal::PublicTrimProposal,
    ) -> StorageResult<()> {
        if let Some(trust) = self.time_trust.get().and_then(Option::as_ref) {
            trust
                .require_retention_policy(
                    proposal.policy.clock_policy,
                    proposal.policy.minimum_retention_ms,
                )
                .map_err(|_| {
                    storage_types::StorageError::validation(
                        "Trim policy differs from configured retention policy",
                    )
                })?;
        }
        Ok(())
    }
}

impl JournalStore {
    pub(crate) fn begin_trim_time(
        &self,
        operation: replication_types::identity::resource::ControlOperationId,
    ) -> crate::trim::time::state::TrimTimeAttempt<'_> {
        self.retention_time.begin_attempt(operation)
    }

    pub(crate) async fn observe_trim_time(
        &self,
        input: (
            &crate::trim::proposal::PublicTrimProposal,
            tokio::time::Instant,
        ),
        prefix: Option<super::public_trim_scan::CompleteTrimPrefix>,
    ) -> Option<crate::trim::time::age::QualifiedPrefixAge> {
        let trust = self.time_trust.get().and_then(Option::as_ref);
        self.retention_time
            .observe((input.0.operation, input.1), prefix, trust, &input.0.policy)
            .await
    }
}
