use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::{
    event::{
        content::{CanonicalEvent, CanonicalPayload},
        identity::{CanonicalEventId, CanonicalItemKey, VersionIdentity},
        order::WinnerProvenance,
    },
    identity::resource::TableLineage,
};
use storage_provider::ItemWriteObservation;
use storage_types::{
    InfrastructureContext, InfrastructureResultKind, ItemProvenance, StorageResult,
};

use crate::{journal::JournalStore, native_record::corrupt_record};

pub(crate) fn materialized_winner(
    observation: &ItemWriteObservation,
    scope: (TableLineage, &CanonicalItemKey),
    codec: &CanonicalJsonCodec,
) -> StorageResult<Option<WinnerProvenance>> {
    let metadata = match observation {
        ItemWriteObservation::Absent => return Ok(None),
        ItemWriteObservation::Deleted { provenance, .. } => provenance,
        ItemWriteObservation::Live { provenance, .. } => provenance,
    };
    if metadata.as_bytes() == ItemProvenance::LOCAL_BYTES {
        return Err(corrupt_record());
    }
    let provenance = codec
        .decode_winner_provenance(metadata.as_bytes(), scope.1)
        .map_err(|_| corrupt_record())?;
    if provenance.lineage != scope.0 {
        return Err(corrupt_record());
    }
    Ok(Some(provenance))
}

/// Native declaration authority, never a flag accepted from a peer.
pub(crate) enum ImportAuthority {
    Current,
    Retained(Vec<replication_types::configuration::table::SchemaRevision>),
}

/// Validate deterministic child content against the immutable victim, never
/// against the receiver's current winner or current schema.
pub(super) async fn verify_expiration<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (
        &CanonicalEvent,
        &CanonicalJsonCodec,
        &crate::journal::PreparedImportEvents,
    ),
) -> StorageResult<()> {
    let (event, codec, inventory) = input;
    let CanonicalEventId::Expiration { victim, .. } = &event.as_ref().id else {
        return Ok(());
    };
    if matches!(victim, VersionIdentity::Baseline { .. }) {
        let baseline = journal
            .load_baseline(attempt, victim, codec)
            .await?
            .ok_or_else(victim_unavailable)?;
        let (identity, child) = baseline
            .item()
            .expiration_child()
            .map_err(|_| corrupt_record())?;
        let bytes = codec
            .encode_item_event(
                replication_provider::CANONICAL_EVENT_SEMANTIC_VERSION.into(),
                &identity,
                &child,
            )
            .map_err(|_| corrupt_record())?;
        return require_child(event, &identity, bytes.as_ref());
    }
    let VersionIdentity::Mutation(victim) = victim else {
        return Err(corrupt_record());
    };
    let identity = CanonicalEventId::Origin(victim.clone());
    let accepted = match inventory.observed_acceptance(&identity) {
        Some(accepted) => accepted,
        None => journal
            .load_acceptance(attempt, &identity, codec)
            .await?
            .ok_or_else(victim_unavailable)?,
    };
    let CanonicalPayload::Item(item) = &accepted.event.as_ref().payload else {
        return Err(corrupt_record());
    };
    let provenance = WinnerProvenance {
        lineage: item.lineage,
        key: item.canonical_key.clone(),
        version: item.version.clone(),
        event: Some(accepted.event.as_ref().id.clone()),
        order: item.order.clone(),
        digest: accepted.event.as_ref().digest,
    };
    let (identity, child) = accepted
        .event
        .try_expiration_child(&provenance)
        .map_err(|_| corrupt_record())?;
    let bytes = codec
        .encode_item_event(accepted.event.as_ref().semantics, &identity, &child)
        .map_err(|_| corrupt_record())?;
    require_child(event, &identity, bytes.as_ref())
}

fn require_child(
    event: &CanonicalEvent,
    identity: &CanonicalEventId,
    bytes: &[u8],
) -> StorageResult<()> {
    if &event.as_ref().id != identity || event.as_ref().bytes.as_ref() != bytes {
        return Err(corrupt_record());
    }
    Ok(())
}

fn victim_unavailable() -> storage_types::StorageError {
    InfrastructureResultKind::Unavailable {
        context: InfrastructureContext::PublicationHistory,
    }
    .into()
}
