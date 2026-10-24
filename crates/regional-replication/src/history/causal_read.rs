//! Complete bounded recorded ancestry, ordered before its selected root.
use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::membership::MembershipRevision,
    event::identity::CanonicalEventId,
    identity::{
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
};
use serde::{Deserialize, Serialize};
use storage_common::retry::retry_native_conflicts;
use storage_provider::{DYNAMO_WRITE_RETRY, DynamoPort};
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::{
    checkpoint::CanonicalCheckpointId,
    read::{CanonicalHistoryOperations, NativeHistoryReader},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalCausalReadRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub checkpoint: CanonicalCheckpointId,
    pub causal_history: CanonicalEventId,
    #[serde(default)]
    pub ancestry_scopes: super::ancestry_scope::CanonicalAncestryScopes,
}

#[derive(Serialize)]
pub struct CanonicalCausalReadPage {
    pub root: CanonicalEventId,
    pub originals: Vec<String>,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

#[derive(Serialize)]
pub(crate) struct CanonicalCausalReadObservation {
    pub(crate) root: CanonicalEventId,
    pub(crate) originals: Vec<String>,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn read_causal_closure(
        &self,
        request: CanonicalCausalReadRequest,
    ) -> StorageResult<CanonicalCausalReadPage> {
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
        let port = Arc::clone(&self.port);
        let journal = Arc::clone(&self.journal);
        let codec = Arc::clone(&self.codec);
        let limits = self.transport.source_limits();
        let request = Arc::new(request);
        crate::transport::settle_source_with(source, move |source| async move {
            let observed = retry_native_conflicts(port.as_ref(), DYNAMO_WRITE_RETRY, || {
                let request = Arc::clone(&request);
                let journal = Arc::clone(&journal);
                let codec = Arc::clone(&codec);
                let port = Arc::clone(&port);
                let limits = Arc::clone(&limits);
                async move {
                    std::sync::Arc::clone(&journal)
                        .with_admitted_attempt(
                            (
                                port.as_ref(),
                                storage_provider::native_admission::NativeWork::Write,
                            ),
                            crate::journal::NativeAttempt::Items(AttemptMode::ReadWrite),
                            move |attempt, observed_at| {
                                Box::pin(async move {
                                    super::retention::settle_expiry(
                                        journal
                                            .load_causal_closure(
                                                attempt,
                                                (&request, observed_at),
                                                (&limits, &codec),
                                            )
                                            .await,
                                    )
                                })
                            },
                        )
                        .await
                }
            })
            .await??;
            Ok(CanonicalCausalReadPage {
                root: observed.root,
                originals: observed.originals,
                _source: source,
            })
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native causal history read failed"))
        })
    }
}
