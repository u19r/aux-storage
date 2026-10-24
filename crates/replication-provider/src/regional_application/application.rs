use replication_types::{
    configuration::membership::RegionalTableBinding,
    error::ReplicationProtocolError,
    event::{
        content::{CanonicalEvent, CanonicalPayload},
        disposition::{CanonicalAcceptanceReceipt, EventDisposition},
        order::WinnerProvenance,
    },
};

use super::{
    error::RegionalApplicationResult,
    import_admission::BoundedImport,
    observation::{EventObservation, TablePageAcceptance},
    port::{RegionalMutationStore, WinnerMutation},
};
use crate::ordering::winner::{WinnerDecision, WinnerEffectPlan};
/// Results describe effects within an uncommitted attempt. They are not
/// application ACKs.
pub struct PendingImport {
    pub pages: Vec<TablePageAcceptance>,
}
/// Complete receipt storage is prepared before the native import attempt loads
/// configuration, originals or winner observations.
pub struct PreparedImportApplication<'import> {
    import: &'import BoundedImport,
    pages: Vec<TablePageAcceptance>,
}

impl<'import> TryFrom<&'import BoundedImport> for PreparedImportApplication<'import> {
    type Error = super::error::RegionalApplicationError;

    fn try_from(input: &'import BoundedImport) -> RegionalApplicationResult<Self> {
        let import = input.as_ref();
        let mut pages = Vec::new();
        pages
            .try_reserve_exact(import.bindings().len())
            .map_err(|_| ReplicationProtocolError::CapacityRejected)?;
        for binding in import.bindings() {
            let count = import
                .event_bindings()
                .filter(|(selected, _)| selected.source_table == binding.source_table)
                .count();
            let mut events = Vec::new();
            events
                .try_reserve_exact(count)
                .map_err(|_| ReplicationProtocolError::CapacityRejected)?;
            pages.push(TablePageAcceptance {
                page: import.table_page_id(binding)?,
                selected_tables: import.fragment_selection()?,
                events,
            });
        }
        Ok(Self {
            import: input,
            pages,
        })
    }
}

impl PreparedImportApplication<'_> {
    pub async fn apply<A: Send, S: RegionalMutationStore<A>>(
        mut self,
        store: &mut S,
        attempt: &mut A,
    ) -> RegionalApplicationResult<PendingImport> {
        let import = self.import.as_ref();
        for (binding, event) in import.event_bindings() {
            let index = import
                .bindings()
                .binary_search_by_key(&binding.source_table, |binding| binding.source_table)
                .map_err(|_| ReplicationProtocolError::InvalidScope {
                    context: "application.binding absent",
                })?;
            self.pages[index]
                .events
                .push(apply_event(store, attempt, (&import.bindings()[index], event)).await?);
        }
        store.store_import_pages(attempt, &self.pages).await?;
        Ok(PendingImport { pages: self.pages })
    }
}
async fn apply_event<A: Send, S: RegionalMutationStore<A>>(
    store: &mut S,
    attempt: &mut A,
    input: (&RegionalTableBinding, &CanonicalEvent),
) -> RegionalApplicationResult<CanonicalAcceptanceReceipt> {
    let (binding, event) = input;
    let observed = store
        .load_event_observation(attempt, binding, event)
        .await?;
    if let Some(accepted) = &observed.accepted {
        validate_duplicate(event, accepted)?;
        return Ok(accepted.receipt.clone());
    }
    validate_schema(event, &observed)?;
    let disposition = materialize_event(store, attempt, (event, observed)).await?;
    let receipt = CanonicalAcceptanceReceipt {
        event: event.as_ref().id.clone(),
        digest: event.as_ref().digest,
        disposition,
    };
    store.store_acceptance(attempt, event, &receipt).await?;
    Ok(receipt)
}
async fn materialize_event<A: Send, S: RegionalMutationStore<A>>(
    store: &mut S,
    attempt: &mut A,
    input: (&CanonicalEvent, EventObservation<S::Observation>),
) -> RegionalApplicationResult<EventDisposition> {
    let (event, observed) = input;
    if observed.application == super::observation::EventApplication::RetainHistory {
        return Ok(EventDisposition::HistoryAccepted);
    }
    let CanonicalPayload::Item(item) = &event.as_ref().payload else {
        return Ok(EventDisposition::HistoryAccepted);
    };
    let incoming = WinnerProvenance {
        lineage: item.lineage,
        key: item.canonical_key.clone(),
        version: item.version.clone(),
        event: Some(event.as_ref().id.clone()),
        order: item.order.clone(),
        digest: event.as_ref().digest,
    };
    let decision = WinnerEffectPlan {
        incoming: &incoming,
        observed: observed.winner.as_ref(),
        known_identity: None,
    }
    .decide_winner()?;
    if decision != WinnerDecision::Replace {
        return Ok(EventDisposition::HistoryAccepted);
    }
    store
        .store_winner_effects(
            attempt,
            (
                WinnerMutation {
                    item,
                    provenance: &incoming,
                },
                observed.native,
            ),
        )
        .await?;
    Ok(EventDisposition::WinnerApplied)
}
fn validate_duplicate(
    event: &CanonicalEvent,
    accepted: &super::observation::StoredAcceptance,
) -> RegionalApplicationResult<()> {
    let candidate = event.as_ref();
    if accepted.receipt.event != candidate.id
        || accepted.receipt.digest != candidate.digest
        || accepted.event.as_ref().bytes != candidate.bytes
    {
        return Err(ReplicationProtocolError::IdentityContentMismatch {
            context: "application.existing event differs",
        }
        .into());
    }
    Ok(())
}

fn validate_schema<W>(
    event: &CanonicalEvent,
    observed: &EventObservation<W>,
) -> RegionalApplicationResult<()> {
    let expected = match &event.as_ref().payload {
        CanonicalPayload::Item(item) => item.schema,
        CanonicalPayload::System(_) | CanonicalPayload::TableStream { .. } => return Ok(()),
    };
    if observed.schema != Some(expected) {
        return Err(replication_types::error::PeerError::SchemaUnavailable {
            context: "exact event schema not installed",
        }
        .into());
    }
    Ok(())
}
