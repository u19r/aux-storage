//! A complete original-source inventory, independently verified by each
//! recipient.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::membership::MembershipRevision,
    identity::{
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::{
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    source_cut::CanonicalSourceCut,
};

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalTableCut {
    pub trim: super::trim_authority::TrimRevision,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub sources: Vec<CanonicalSourceCut>,
}

impl CanonicalTableCut {
    pub(crate) fn overlaps_manifest(
        &self,
        manifest: &replication_types::event::interval::SourceIntervalManifest,
    ) -> bool {
        manifest.links.iter().any(|link| {
            self.sources.iter().any(|cut| {
                super::publication_scope::SourcePublicationScope::from(cut)
                    == super::publication_scope::SourcePublicationScope::new(
                        link.lineage,
                        &manifest.origin,
                        link.table,
                    )
                    && cut.after < link.position
                    && link.position <= cut.through
            })
        })
    }

    pub(crate) fn require_inventory(&self) -> StorageResult<()> {
        if self.sources.is_empty() {
            return Err(crate::native_record::corrupt_record());
        }
        let mut seen = std::collections::BTreeSet::new();
        for cut in &self.sources {
            cut.require_source()?;
            if cut.trim != self.trim
                || cut.lineage != self.lineage
                || cut.membership != self.membership
                || !seen.insert((&cut.origin, cut.source.table, cut.source.config))
            {
                return Err(crate::native_record::corrupt_record());
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CanonicalTableCutAction {
    Capture,
    CaptureHistorical {
        lineage: TableLineage,
        membership: MembershipRevision,
    },
    Verify(CanonicalTableCut),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalTableCutRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub table_cut: CanonicalTableCutAction,
}

#[derive(Serialize)]
pub struct CanonicalTableCutObservation {
    pub cut: CanonicalTableCut,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn observe_table_cut(
        &self,
        request: CanonicalTableCutRequest,
    ) -> StorageResult<CanonicalTableCutObservation> {
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
        let codec = std::sync::Arc::clone(&self.codec);
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
                                .observe_table_cut(attempt, (&request, source), (&limits, &codec))
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
                .unwrap_or_else(|_| StorageError::internal("Native table cut observation failed"))
        })
    }
}
