use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_provider::{
    CANONICAL_EVENT_SEMANTIC_VERSION, canonical_event_codec::CanonicalJsonCodec,
    regional_application::error::RegionalApplicationError,
};
use replication_types::event::{
    content::{AcceptedControlEvent, CanonicalEvent, CanonicalEventCodec, CanonicalPayload},
    disposition::{CanonicalAcceptanceReceipt, EventDisposition},
    identity::{CanonicalEventId, OriginEventId},
    interval::CanonicalEventLink,
};
use storage_types::{ReadProtection, StorageResult, TimestampMillis};

use crate::{
    control::{ControlDecision, ControlRecord},
    journal::JournalStore,
    native_record::corrupt_record,
};

impl JournalStore {
    /// The primary's decision, original and delivery address share one attempt.
    /// Participant installation remains a separate certified operation.
    pub(super) async fn capture_control_history<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        record: &ControlRecord,
        input: (&CanonicalJsonCodec, TimestampMillis),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (codec, now) = input;
            if record.proposal.operation().home_region != self.region
                || !matches!(
                    record.decision,
                    ControlDecision::Committed
                        | ControlDecision::Installed
                        | ControlDecision::Completed
                )
            {
                return Err(corrupt_record());
            }
            let operation = record.proposal.operation();
            let identity = CanonicalEventId::Control {
                lineage: operation.lineage,
                operation: operation.id,
            };
            if let Some(accepted) = self.load_acceptance(attempt, &identity, codec).await? {
                return self
                    .verify_control_history(attempt, record, &accepted, ReadProtection::Shared)
                    .await;
            }
            if record.decision != ControlDecision::Committed {
                return Err(corrupt_record());
            }
            let predecessor = self
                .load_control_predecessor(attempt, record, ReadProtection::Shared)
                .await?;
            // Controls are table-wide, so they use the fixed layout's first
            // partition. They do not acquire an item key or participate in
            // winner ordering.
            let observed = self.load_partition(attempt, 0u16.into()).await?;
            let wall_ms = u64::try_from(now.timestamp_millis()).map_err(|_| corrupt_record())?;
            let clock = self.issue_regional_clock((observed.floor(), wall_ms), None)?;
            let allocation = self.allocate_partition(attempt, observed, clock).await?;
            let event = encode_control_history(record, (&allocation, codec), predecessor)?;
            let receipt = CanonicalAcceptanceReceipt {
                event: event.as_ref().id.clone(),
                digest: event.as_ref().digest,
                disposition: EventDisposition::HistoryAccepted,
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
            .await
        }
        .await;
        settlement.complete(result)
    }

    pub(crate) async fn require_control_history<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        record: &ControlRecord,
        encoding: (&CanonicalJsonCodec, ReadProtection),
    ) -> StorageResult<()> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (codec, protection) = encoding;
            let operation = record.proposal.operation();
            if operation.home_region != self.region
                || matches!(
                    record.decision,
                    ControlDecision::Prepared | ControlDecision::Cancelled
                )
            {
                return Ok(());
            }
            let identity = CanonicalEventId::Control {
                lineage: operation.lineage,
                operation: operation.id,
            };
            let accepted = self
                .load_acceptance_protected(attempt, &identity, codec, protection)
                .await?
                .ok_or_else(corrupt_record)?;
            self.verify_control_history(attempt, record, &accepted, protection)
                .await
        }
        .await;
        settlement.complete(result)
    }

    async fn verify_control_history<A: PhysicalRecordAttempt>(
        &self,
        attempt: &mut A,
        record: &ControlRecord,
        accepted: &replication_provider::regional_application::observation::StoredAcceptance,
        protection: ReadProtection,
    ) -> StorageResult<()> {
        let CanonicalPayload::System(control) = &accepted.event.as_ref().payload else {
            return Err(corrupt_record());
        };
        if control.proposal != record.proposal
            || accepted.receipt.disposition != EventDisposition::HistoryAccepted
            || control.publication.origin
                != self.origin(control.publication.origin.journal_partition)?
            || control.predecessor
                != self
                    .load_control_predecessor(attempt, record, protection)
                    .await?
        {
            return Err(corrupt_record());
        }
        let source = self
            .load_source_position(
                attempt,
                &control.publication.origin,
                control.publication.position,
            )
            .await?
            .ok_or_else(corrupt_record)?;
        let [link] = source.events.as_slice() else {
            return Err(corrupt_record());
        };
        link.require_event(&accepted.event, &source.origin)
            .map_err(|_| corrupt_record())
    }
}

fn encode_control_history(
    record: &ControlRecord,
    input: (&crate::journal::PartitionAllocation, &CanonicalJsonCodec),
    predecessor: Option<replication_types::identity::resource::ControlOperationId>,
) -> StorageResult<CanonicalEvent> {
    let (allocation, codec) = input;
    let operation = record.proposal.operation();
    let identity = CanonicalEventId::Control {
        lineage: operation.lineage,
        operation: operation.id,
    };
    let publication = OriginEventId {
        lineage: operation.lineage,
        origin: allocation.origin().clone(),
        position: allocation.position(),
        ordinal: 0u32.into(),
    };
    let payload = CanonicalPayload::System(Box::new(AcceptedControlEvent {
        proposal: record.proposal.clone(),
        publication,
        predecessor,
    }));
    let bytes = codec
        .encode_event(CANONICAL_EVENT_SEMANTIC_VERSION.into(), &identity, &payload)
        .map_err(RegionalApplicationError::from_local_encoding)?;
    codec
        .decode_event(bytes.as_ref())
        .map_err(RegionalApplicationError::from)
        .map_err(Into::into)
}
