//! Receiver-local first-seen canonical delivery, independent of source
//! ordering.

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::{
    configuration::membership::MembershipRevision,
    event::identity::CanonicalEventId,
    identity::{
        origin::OriginId,
        replica::{DatabaseGroupId, RegionId},
        resource::TableLineage,
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_provider::DynamoPort;
use storage_types::{AttemptMode, StorageError, StorageResult, TableName};

use super::{
    read::{CanonicalHistoryOperations, NativeHistoryReader},
    selection::{CanonicalViewCursor, CanonicalViewSelection},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(
    try_from = "storage_types::StreamItemId",
    into = "storage_types::StreamItemId"
)]
pub struct DeliveryPosition(u64);

impl From<u64> for DeliveryPosition {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl TryFrom<storage_types::StreamItemId> for DeliveryPosition {
    type Error = StorageError;
    fn try_from(value: storage_types::StreamItemId) -> StorageResult<Self> {
        let bytes = value.as_bytes();
        if bytes[..4] != [0; 4] {
            return Err(StorageError::validation(
                "Canonical delivery ID has an invalid domain prefix",
            ));
        }
        Ok(Self(u64::from_be_bytes(
            bytes[4..]
                .try_into()
                .map_err(|_| crate::native_record::corrupt_record())?,
        )))
    }
}
impl From<DeliveryPosition> for storage_types::StreamItemId {
    fn from(value: DeliveryPosition) -> Self {
        let mut bytes = *storage_types::StreamItemId::default().as_bytes();
        bytes[4..].copy_from_slice(&value.bytes());
        Self::from(bytes)
    }
}

impl DeliveryPosition {
    pub(crate) fn checked_next(self) -> StorageResult<Self> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or_else(crate::native_record::corrupt_record)
    }
    pub(crate) fn bytes(self) -> [u8; 8] {
        self.0.to_be_bytes()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalViewPointer {
    pub receiver: OriginId,
    pub position: DeliveryPosition,
    pub lineage: TableLineage,
    pub event: CanonicalEventId,
    pub digest: CanonicalDigest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalViewRequest {
    pub serving_region: RegionId,
    pub serving_group: DatabaseGroupId,
    pub table: TableName,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub receiver: OriginId,
    pub selection: CanonicalViewSelection,
    pub checkpoint: Option<super::checkpoint::CanonicalCheckpointId>,
    pub after: Option<CanonicalViewCursor>,
}

#[derive(Serialize)]
pub struct CanonicalViewRecord {
    pub pointer: CanonicalViewPointer,
    pub original: String,
}

#[derive(Serialize)]
pub struct CanonicalViewPage {
    pub through: CanonicalViewCursor,
    pub observed_head: DeliveryPosition,
    pub record: Option<CanonicalViewRecord>,
    #[serde(skip_serializing)]
    pub(crate) _source: crate::transport::SourceReservation,
}

#[derive(Serialize)]
pub(crate) struct CanonicalViewObservation {
    pub(crate) through: CanonicalViewCursor,
    pub(crate) observed_head: DeliveryPosition,
    pub(crate) record: Option<CanonicalViewRecord>,
}

impl<P: DynamoPort + 'static> NativeHistoryReader<P>
where for<'native> P::Attempt<'native>: PhysicalRecordAttempt
{
    pub(crate) async fn read_view(
        &self,
        request: CanonicalViewRequest,
    ) -> StorageResult<CanonicalViewPage> {
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
        let request = std::sync::Arc::new(request);
        crate::transport::settle_source_with(source, move |source| async move {
            let mode = if request.checkpoint.is_some() {
                AttemptMode::ReadWrite
            } else {
                AttemptMode::ReadOnly
            };
            let observed = storage_common::retry::retry_native_conflicts(
                port.as_ref(),
                storage_provider::DYNAMO_WRITE_RETRY,
                || {
                    let port = std::sync::Arc::clone(&port);
                    let journal = std::sync::Arc::clone(&journal);
                    let codec = std::sync::Arc::clone(&codec);
                    let request = std::sync::Arc::clone(&request);
                    let limits = std::sync::Arc::clone(&limits);
                    async move {
                        std::sync::Arc::clone(&journal)
                            .with_admitted_attempt(
                                (
                                    port.as_ref(),
                                    if matches!(mode, AttemptMode::ReadOnly) {
                                        storage_provider::native_admission::NativeWork::RangeRead
                                    } else {
                                        storage_provider::native_admission::NativeWork::Write
                                    },
                                ),
                                crate::journal::NativeAttempt::Items(mode),
                                move |attempt, observed_at| {
                                    Box::pin(async move {
                                        super::retention::settle_expiry(
                                            journal
                                                .load_canonical_view(
                                                    attempt,
                                                    (&request, (&limits, &codec), observed_at),
                                                )
                                                .await,
                                        )
                                    })
                                },
                            )
                            .await
                    }
                },
            )
            .await??;
            Ok(CanonicalViewPage {
                through: observed.through,
                observed_head: observed.observed_head,
                record: observed.record,
                _source: source,
            })
        })
        .await
        .map_err(|error| {
            error
                .downcast::<StorageError>()
                .map(|error| *error)
                .unwrap_or_else(|_| StorageError::internal("Native canonical view failed"))
        })
    }
}
