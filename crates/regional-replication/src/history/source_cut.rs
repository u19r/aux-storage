//! Portable source-prefix observations require native verification at every
//! reader.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::{installed::RegionalSourceBinding, membership::MembershipRevision},
    identity::{
        origin::{OriginId, OriginPosition},
        resource::TableLineage,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult};

use super::{
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    source_selection::CanonicalSourceSelection,
};

#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalSourceCut {
    pub trim: super::trim_authority::TrimRevision,
    pub after: OriginPosition,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub source: RegionalSourceBinding,
    pub origin: OriginId,
    pub through: OriginPosition,
    pub terminal_manifest: Option<CanonicalDigest>,
}

impl CanonicalSourceCut {
    pub(crate) fn require_source(&self) -> StorageResult<()> {
        if self.after > self.through {
            return Err(crate::native_record::corrupt_record());
        }
        if self.trim == super::trim_authority::TrimRevision::GENESIS
            && self.after != OriginPosition::from(0)
        {
            return Err(StorageError::from(
                storage_types::StorageEnum::HistoryExpired,
            ));
        }
        if self.source.region != self.origin.region
            || self.source.database_group != self.origin.database_group
            || self.source.generation != self.origin.journal_generation
            || (u64::from(self.through) == 0) != self.terminal_manifest.is_none()
        {
            return Err(crate::native_record::corrupt_record());
        }
        self.source
            .partitions
            .require_partition(self.origin.journal_partition)
            .map_err(|_| crate::native_record::corrupt_record())
    }

    pub(crate) fn require_selection(
        &self,
        selection: &CanonicalSourceSelection,
    ) -> StorageResult<()> {
        self.require_source()?;
        if self.lineage != selection.lineage
            || self.membership != selection.membership
            || self.source != selection.source
            || self.origin != selection.origin
        {
            return Err(StorageError::validation(
                "Canonical source cut does not match its selection",
            ));
        }
        Ok(())
    }
}

#[derive(Serialize)]
pub struct CanonicalSourceCutObservation {
    pub cut: CanonicalSourceCut,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn observe_source_cut(
        &self,
        selection: CanonicalSourceSelection,
    ) -> StorageResult<CanonicalSourceCutObservation> {
        if !self.matches((&selection.serving_region, &selection.serving_group)) {
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
                                .observe_source_cut(attempt, (selection, source), (&limits, &codec))
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
                .unwrap_or_else(|_| StorageError::internal("Native source cut observation failed"))
        })
    }
}
