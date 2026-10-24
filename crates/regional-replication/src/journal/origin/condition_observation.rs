use replication_provider::canonical_event_codec::CanonicalJsonCodec;
use replication_types::event::{
    content::{CanonicalEventCodec, SourceImagesRef},
    identity::CanonicalItemKey,
    order::WinnerProvenance,
};
use storage_provider::AcceptedTransactConditionCheck;
use storage_types::StorageResult;

use super::{
    super::{JournalStore, TableStateObservation},
    mutation::MutationView,
};
use crate::native_record::corrupt_record;

impl JournalStore {
    pub(crate) async fn observe_transaction_conditions<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        inputs: &[(&TableStateObservation, &AcceptedTransactConditionCheck)],
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Vec<WinnerProvenance>> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let result = async {
            let inputs = inputs
                .iter()
                .map(|input| ConditionObservationInput::try_new(self, *input, codec))
                .collect::<StorageResult<Vec<_>>>()?;
            let views = inputs
                .iter()
                .map(|input| (input.table, &input.view, &input.key))
                .collect::<Vec<_>>();
            let previous = self
                .observe_origin_predecessors(settlement.as_mut(), &views, codec)
                .await?;
            if previous.len() != inputs.len() {
                return Err(corrupt_record());
            }
            Ok(previous
                .into_iter()
                .flatten()
                .map(|previous| previous.provenance)
                .collect())
        }
        .await;
        settlement.complete(result)
    }
}

/// Accepted check observations are borrowed, never re-read as a customer item.
struct ConditionObservationInput<'accepted> {
    table: &'accepted TableStateObservation,
    view: MutationView<'accepted>,
    key: CanonicalItemKey,
}

impl<'accepted> ConditionObservationInput<'accepted> {
    fn try_new(
        journal: &JournalStore,
        input: (
            &'accepted TableStateObservation,
            &'accepted AcceptedTransactConditionCheck,
        ),
        codec: &CanonicalJsonCodec,
    ) -> StorageResult<Self> {
        let (table, condition) = input;
        table.require_origination(journal, &condition.table)?;
        let old = condition.old_item.as_deref();
        let view = MutationView {
            physical: &condition.table,
            key: &condition.key,
            provenance: condition.old_provenance.as_ref(),
            source: condition
                .old_item
                .as_ref()
                .and_then(|item| item.stored_source()),
            images: SourceImagesRef {
                old,
                old_indexers: old.map(|_| condition.old_indexers.as_slice()),
                new: None,
                new_indexers: None,
            },
        };
        let key = codec
            .encode_key(&condition.key)
            .map_err(|_| corrupt_record())?;
        Ok(Self { table, view, key })
    }
}
