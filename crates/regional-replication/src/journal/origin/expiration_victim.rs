use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, canonical_item_original::AuthoredItemOriginal,
};
use replication_types::event::content::{CanonicalEventCodec, MutationCause};
use storage_provider::{AcceptedDeleteItem, AcceptedMutationRef};
use storage_types::StorageResult;

use super::{
    super::{JournalStore, TableStateObservation, reconstruction::WinnerAuthority},
    OriginMutation,
    predecessor::OriginPredecessor,
};
use crate::native_record::corrupt_record;

pub(super) async fn load_expiration_victim<
    A: stream_provider::retained_image::RetainedOwnershipAttempt,
>(
    journal: &JournalStore,
    attempt: &mut A,
    input: (
        &TableStateObservation,
        &AcceptedDeleteItem,
        &CanonicalJsonCodec,
    ),
) -> StorageResult<OriginPredecessor> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (table, accepted, codec) = input;
        let mutation = OriginMutation {
            table,
            mutation: AcceptedMutationRef::Delete(accepted),
            cause: MutationCause::Delete,
            ordinal: 0.into(),
        };
        let view = mutation.view()?;
        mutation.table.require_origination(journal, view.physical)?;
        if view.images.old.is_none() {
            return Err(corrupt_record());
        }
        let key = codec.encode_key(view.key).map_err(|_| corrupt_record())?;
        let victim = journal
            .observe_origin_predecessor(attempt, (mutation.table, &view, &key), codec)
            .await?
            .ok_or_else(corrupt_record)?;
        if victim.authority.images()?.new_indexers != Some(accepted.old_indexers.as_slice()) {
            return Err(corrupt_record());
        }
        Ok(victim)
    }
    .await;
    settlement.complete(result)
}

pub(super) fn encode_expiration_child(
    victim: &OriginPredecessor,
    codec: &CanonicalJsonCodec,
) -> StorageResult<AuthoredItemOriginal> {
    match &victim.authority {
        WinnerAuthority::Baseline(authority) => {
            let (identity, child) = authority
                .item()
                .expiration_child()
                .map_err(|_| corrupt_record())?;
            codec.author_item_event(replication_provider::CANONICAL_EVENT_SEMANTIC_VERSION.into(), &identity, &child).map_err(|error| replication_provider::regional_application::error::RegionalApplicationError::from_local_encoding(error).into())
        }
        WinnerAuthority::Event(event) => {
            let (identity, child) = event
                .try_expiration_child(&victim.provenance)
                .map_err(|_| corrupt_record())?;
            codec.author_item_event(event.as_ref().semantics, &identity, &child).map_err(|error| replication_provider::regional_application::error::RegionalApplicationError::from_local_encoding(error).into())
        }
    }
}
