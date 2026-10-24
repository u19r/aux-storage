//! Discovery of installed original sources and their locally complete
//! frontiers.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{installed::RegionalSourceBinding, membership::MembershipRevision},
    identity::{
        origin::{OriginId, OriginPosition},
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::read::{CanonicalHistoryOperations, NativeHistoryReader};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalHistoryDiscoveryRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub after: Option<HistorySourceCursor>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistorySourceCursor {
    pub trim: super::trim_authority::TrimRevision,
    pub physical: storage_types::TableHistoryId,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub next: HistorySourcePosition,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
pub enum HistorySourcePosition {
    Current {
        origin: OriginId,
    },
    Retired {
        source: RegionalSourceBinding,
        partition: u32,
    },
    Historical {
        after: Option<infrastructure_provider::record::DurableRecordKey>,
    },
}

#[derive(Serialize)]
#[serde(tag = "state", content = "binding")]
pub enum DiscoveredHistorySource {
    Current(RegionalSourceBinding),
    Retired(RegionalSourceBinding),
    Historical {
        lineage: TableLineage,
        membership: MembershipRevision,
        source: RegionalSourceBinding,
    },
}

/// A locally complete transport frontier, not a public cursor or trim
/// certificate.
#[derive(Serialize)]
pub struct HistorySourceFrontier {
    pub origin: OriginId,
    pub through: OriginPosition,
}

#[derive(Serialize)]
pub struct CanonicalHistorySources {
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub source: Option<DiscoveredHistorySource>,
    pub available: Vec<HistorySourceFrontier>,
    pub continuation: Option<HistorySourceCursor>,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn discover_sources(
        &self,
        request: CanonicalHistoryDiscoveryRequest,
    ) -> StorageResult<CanonicalHistorySources> {
        if !self.matches((&request.serving_region, &request.serving_group)) {
            return Err(StorageError::validation(
                "History serving database does not match",
            ));
        }
        let source = self
            .transport
            .reserve_history_source()
            .await
            .map_err(|_| StorageError::from(storage_types::StorageEnum::HistoryUnavailable))?;
        let port = std::sync::Arc::clone(&self.port);
        let journal = std::sync::Arc::clone(&self.journal);
        let limits = self.transport.source_limits();
        crate::transport::settle_source_with(source, move |source| async move {
            std::sync::Arc::clone(&journal)
                .with_admitted_attempt(
                    (
                        port.as_ref(),
                        storage_provider::native_admission::NativeWork::RangeRead,
                    ),
                    crate::journal::NativeAttempt::Items(AttemptMode::ReadOnly),
                    move |attempt, _| {
                        Box::pin(async move {
                            journal
                                .load_history_sources(attempt, &request, (&limits, source))
                                .await
                        })
                    },
                )
                .await
                .map_err(|error| Box::new(error) as Box<dyn std::error::Error + Send + Sync>)
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native history discovery failed"))
        })
    }
}
