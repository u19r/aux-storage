use replication_types::{
    configuration::membership::RegionalTableBinding,
    event::{
        content::{AcceptedItemEvent, CanonicalEvent},
        disposition::CanonicalAcceptanceReceipt,
    },
};
use storage_types::StorageResult;

use super::observation::EventObservation;
/// Physical reads and writes join the caller's attempt; this port chooses no
/// winner.
pub trait RegionalMutationStore<A: Send>: Send + Sync {
    /// Protected primary/catalog/lifecycle observations consumed by projection.
    type Observation: Send;
    /// Stores each complete table-page receipt in this attempt, with stable
    /// source identity.
    fn store_import_pages(
        &mut self,
        attempt: &mut A,
        pages: &[super::observation::TablePageAcceptance],
    ) -> impl Future<Output = StorageResult<()>> + Send;
    fn load_event_observation(
        &mut self,
        attempt: &mut A,
        binding: &RegionalTableBinding,
        event: &CanonicalEvent,
    ) -> impl Future<Output = StorageResult<EventObservation<Self::Observation>>> + Send;
    fn store_winner_effects(
        &mut self,
        attempt: &mut A,
        input: (WinnerMutation<'_>, Self::Observation),
    ) -> impl Future<Output = StorageResult<()>> + Send;
    /// Stores canonical history and every required extension view together with
    /// this receipt. System/control materialization is certified separately
    /// by the control owner.
    fn store_acceptance(
        &mut self,
        attempt: &mut A,
        event: &CanonicalEvent,
        receipt: &CanonicalAcceptanceReceipt,
    ) -> impl Future<Output = StorageResult<()>> + Send;
}
/// The resolved winner and protected native observation feed the existing
/// shared primary/GSI/TTL/stream preparation. Source old images are history
/// only.
pub struct WinnerMutation<'event> {
    pub item: &'event AcceptedItemEvent,
    pub provenance: &'event replication_types::event::order::WinnerProvenance,
}
