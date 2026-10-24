use std::{num::NonZeroUsize, sync::Arc};

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    CANONICAL_EVENT_SEMANTIC_VERSION, canonical_event_codec::CanonicalJsonCodec,
    regional_application::error::RegionalApplicationError,
};
use replication_types::{
    event::{
        content::{CanonicalEvent, CanonicalEventCodec, CanonicalPayload},
        disposition::{CanonicalAcceptanceReceipt, EventDisposition},
        identity::{CanonicalEventId, OriginEventId},
        interval::CanonicalEventLink,
    },
    protocol::CanonicalBytes,
};
use storage_provider::CatalogAttempt;
use storage_types::{StorageResult, TableHistoryId, TableName, storage_serde};
use stream_provider::{
    StreamAppendAllocation, StreamAppendReceipt, StreamAttempt, TableAppendReconciliation,
    TableAppendRequest, UserStreamAppend,
};

use super::{JournalStore, PartitionAllocation};
use crate::native_record::corrupt_record;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TableAppendProof {
    physical: TableHistoryId,
    name: TableName,
    table: replication_types::identity::resource::TableIdentity,
    original: CanonicalEventId,
    digest: replication_types::protocol::CanonicalDigest,
}

pub struct CapturedTableAppend {
    request: TableAppendRequest,
    proof: TableAppendProof,
}

impl JournalStore {
    /// The protected catalog selection, stream row and original share one
    /// native transaction. An extension never creates item winner effects.
    pub(crate) async fn capture_table_append<
        A: stream_provider::retained_image::RetainedOwnershipAttempt + CatalogAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &TableAppendRequest,
        input: (&UserStreamAppend, &CanonicalJsonCodec),
    ) -> StorageResult<Option<CapturedTableAppend>> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some(stamp) = attempt.read_catalog_stamp(request.table()).await? else {
                return Ok(None);
            };
            let bound = attempt.read_table_at_stamp(request.table(), stamp).await?;
            let state = self.load_capture_state(attempt, &bound).await?;
            self.require_origin_admission(state.as_ref())?;
            let Some(state) = state.filter(|state| state.state().captures_origin()) else {
                return Ok(None);
            };
            // Table-wide appends share the first partition with control
            // history.
            let observed = self.load_partition(attempt, 0u16.into()).await?;
            let wall_ms = u64::try_from(input.0.captured_at().timestamp_millis())
                .map_err(|_| corrupt_record())?;
            let clock = self.issue_regional_clock((observed.floor(), wall_ms), None)?;
            let allocation = self.allocate_partition(attempt, observed, clock).await?;
            let event = encode_table_append(state.state(), (&allocation, input.0, input.1))?;
            let receipt = CanonicalAcceptanceReceipt {
                event: event.as_ref().id.clone(),
                digest: event.as_ref().digest,
                disposition: EventDisposition::HistoryAccepted,
            };
            let proof = TableAppendProof {
                physical: bound.history_id,
                name: request.table().clone(),
                table: event.source_table(),
                original: receipt.event.clone(),
                digest: receipt.digest,
            };
            let link = CanonicalEventLink {
                position: allocation.position(),
                publication_clock: allocation.clock(),
                ordinal: 0u32.into(),
                table: event.source_table(),
                lineage: event.lineage(),
                event: receipt.event.clone(),
                digest: receipt.digest,
            };
            self.store_source_publications(
                attempt,
                vec![(allocation, vec![link])],
                &[(&event, &receipt, None)],
            )
            .await?;
            Ok(Some(CapturedTableAppend {
                request: request.clone(),
                proof,
            }))
        }
        .await;
        settlement_guard.complete(result)
    }

    pub(crate) async fn reconcile_table_append<
        A: StreamAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        request: &TableAppendRequest,
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<TableAppendReconciliation> {
        let mut settlement_guard =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement_guard.as_mut();
        let result = async {
            let Some(receipt) = attempt.load_append_receipt(request.operation()).await? else {
                return Ok(TableAppendReconciliation::Unresolved);
            };
            receipt.require_request(request)?;
            let proof: TableAppendProof = storage_serde::from_bytes(&receipt.proof)?;
            if proof.name != *request.table() {
                return Err(corrupt_record());
            }
            self.require_append_incarnation(attempt, &proof).await?;
            let CanonicalEventId::Origin(original) = &proof.original else {
                return Err(corrupt_record());
            };
            if original.origin != self.origin(0u16.into())?
                || u32::from(original.ordinal) != 0
                || u64::from(original.position) == 0
            {
                return Err(corrupt_record());
            }
            let accepted = self
                .load_acceptance(attempt, &proof.original, codec)
                .await?
                .ok_or_else(corrupt_record)?;
            if accepted.receipt.digest != proof.digest
                || accepted.receipt.disposition != EventDisposition::HistoryAccepted
                || accepted.event.source_table() != proof.table
            {
                return Err(corrupt_record());
            }
            let CanonicalPayload::TableStream { bytes, .. } = &accepted.event.as_ref().payload
            else {
                return Err(corrupt_record());
            };
            if TableAppendRequest::new(request.operation(), request.table().clone(), bytes.as_ref())
                .digest()
                != request.digest()
            {
                return Err(corrupt_record());
            }
            Ok(TableAppendReconciliation::Durable(
                receipt.response.resolve(None)?,
            ))
        }
        .await;
        settlement_guard.complete(result)
    }

    async fn require_append_incarnation<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        proof: &TableAppendProof,
    ) -> StorageResult<()> {
        let key = self.identity_key(crate::constants::CAPTURE_SUFFIX, &proof.physical)?;
        let rows = self
            .records
            .load(attempt, &[key], storage_types::ReadProtection::Snapshot)
            .await?;
        let record = rows[0].as_ref().ok_or_else(corrupt_record)?;
        if self
            .records
            .decode::<replication_types::identity::resource::TableIdentity>(record)?
            != proof.table
        {
            return Err(corrupt_record());
        }
        Ok(())
    }
}

impl CapturedTableAppend {
    pub(crate) async fn store_response<A: StreamAttempt + PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        response: StreamAppendAllocation,
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let receipt = StreamAppendReceipt::new(
                &self.request,
                storage_serde::to_bytes(&self.proof)?,
                response,
            );
            attempt.store_append_receipt(&receipt).await
        }
        .await;
        settlement.complete(result)
    }
}

fn encode_table_append(
    state: &replication_types::configuration::installed::InstalledTableState,
    input: (&PartitionAllocation, &UserStreamAppend, &CanonicalJsonCodec),
) -> StorageResult<CanonicalEvent> {
    let (allocation, append, codec) = input;
    let lineage = state.configuration.lineage;
    let identity = CanonicalEventId::Origin(OriginEventId {
        lineage,
        origin: allocation.origin().clone(),
        position: allocation.position(),
        ordinal: 0u32.into(),
    });
    let bytes = CanonicalBytes::try_new(
        Arc::from(append.data()),
        NonZeroUsize::new(append.data().len()).ok_or_else(corrupt_record)?,
    )
    .map_err(RegionalApplicationError::from)?;
    let payload = CanonicalPayload::TableStream {
        table: state.configuration.table,
        lineage,
        bytes,
    };
    let encoded = codec
        .encode_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &identity, &payload)
        .map_err(RegionalApplicationError::from_local_encoding)?;
    codec
        .decode_event(encoded.as_ref())
        .map_err(RegionalApplicationError::from)
        .map_err(Into::into)
}
