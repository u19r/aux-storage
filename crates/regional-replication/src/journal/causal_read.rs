//! Native bounded closure: receipts, held source publications and recorded item
//! edges.
use std::collections::BTreeMap;

use replication_provider::{
    canonical_event_codec::CanonicalJsonCodec, runtime_configuration::limits::WorkLimits,
};
use replication_types::event::{content::CanonicalEvent, identity::CanonicalEventId};
use storage_provider::CatalogAttempt;
use storage_types::{ReadProtection, StorageError, StorageResult};

use super::{JournalStore, causal_scope::HeldCausalScope};
use crate::{
    history::{ancestry_scope::CanonicalAncestryScope, causal_read::CanonicalCausalReadRequest},
    native_record::corrupt_record,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum VisitState {
    Visiting,
    Complete,
}
struct CausalNode {
    event: CanonicalEvent,
    parents: Vec<CanonicalEventId>,
    state: VisitState,
}
struct CausalClosure {
    nodes: BTreeMap<CanonicalEventId, CausalNode>,
    order: Vec<CanonicalEventId>,
    pending: Vec<(CanonicalEventId, VisitState)>,
    bytes: usize,
    edges: usize,
    maximum: usize,
}

impl CausalClosure {
    fn new(root: CanonicalEventId, limits: &WorkLimits) -> Self {
        Self {
            nodes: BTreeMap::new(),
            order: Vec::new(),
            pending: vec![(root, VisitState::Visiting)],
            bytes: limits.frame_bytes.get(),
            edges: limits.frame_events.get(),
            maximum: limits.frame_events.get(),
        }
    }

    fn complete_or_require_new(
        &mut self,
        identity: &CanonicalEventId,
        phase: VisitState,
    ) -> StorageResult<bool> {
        if phase == VisitState::Complete {
            let node = self.nodes.get_mut(identity).ok_or_else(corrupt_record)?;
            node.state = VisitState::Complete;
            self.order.push(identity.clone());
            return Ok(false);
        }
        match self.nodes.get(identity) {
            Some(node) if node.state == VisitState::Complete => Ok(false),
            Some(_) => Err(corrupt_record()),
            None if self.nodes.len() == self.maximum => Err(closure_too_large()),
            None => Ok(true),
        }
    }

    fn retain(&mut self, event: &CanonicalEvent) -> StorageResult<()> {
        let candidate = event.as_ref();
        let ancestry = super::causal_ancestry::RecordedAncestry::new(event);
        let count = ancestry.count();
        self.edges = self
            .edges
            .checked_sub(count)
            .ok_or_else(closure_too_large)?;
        let parents = (0..count)
            .map(|index| ancestry.parent(index).ok_or_else(corrupt_record))
            .collect::<StorageResult<Vec<_>>>()?;
        let original =
            std::str::from_utf8(candidate.bytes.as_ref()).map_err(|_| corrupt_record())?;
        self.bytes = self
            .bytes
            .checked_sub(crate::history::response_budget::encoded_length(
                &original, self.bytes,
            )?)
            .ok_or_else(closure_too_large)?;
        self.pending
            .push((candidate.id.clone(), VisitState::Complete));
        self.pending.extend(
            parents
                .iter()
                .rev()
                .cloned()
                .map(|identity| (identity, VisitState::Visiting)),
        );
        self.nodes.insert(
            candidate.id.clone(),
            CausalNode {
                event: event.clone(),
                parents,
                state: VisitState::Visiting,
            },
        );
        Ok(())
    }

    fn into_originals(mut self) -> StorageResult<Vec<String>> {
        for child in self.nodes.values() {
            for identity in &child.parents {
                let parent = self.nodes.get(identity).ok_or_else(corrupt_record)?;
                super::causal_ancestry::RecordedAncestry::new(&child.event)
                    .require_parent(&parent.event)?;
            }
        }
        self.order
            .into_iter()
            .map(|identity| {
                let node = self.nodes.remove(&identity).ok_or_else(corrupt_record)?;
                String::from_utf8(node.event.as_ref().bytes.as_ref().to_vec())
                    .map_err(|_| corrupt_record())
            })
            .collect()
    }
}

impl JournalStore {
    pub(crate) async fn load_causal_closure<
        A: CatalogAttempt + stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&CanonicalCausalReadRequest, storage_types::TimestampMillis),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<crate::history::causal_read::CanonicalCausalReadObservation> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (request, observed_at) = input;
            if !request.causal_history.is_bound_to(request.lineage) {
                return Err(StorageError::validation(
                    "Causal root does not match the requested lineage",
                ));
            }
            let root = CanonicalAncestryScope {
                table: request.table.clone(),
                lineage: request.lineage,
                membership: request.membership,
                checkpoint: request.checkpoint,
            };
            let scopes = self
                .load_held_causal_scopes(
                    attempt,
                    (&root, &request.ancestry_scopes, observed_at),
                    encoding,
                )
                .await?;
            let mut closure = CausalClosure::new(request.causal_history.clone(), encoding.0);
            while let Some((identity, phase)) = closure.pending.pop() {
                if !closure.complete_or_require_new(&identity, phase)? {
                    continue;
                }
                let event = self
                    .load_verified_causal_identity(attempt, (&identity, &scopes), encoding)
                    .await?;
                closure.retain(&event)?;
            }
            let observation = crate::history::causal_read::CanonicalCausalReadObservation {
                root: request.causal_history.clone(),
                originals: closure.into_originals()?,
            };
            crate::history::response_budget::encoded_length(
                &observation,
                encoding.0.frame_bytes.get(),
            )?;
            Ok(observation)
        }
        .await;
        settlement.complete(crate::history::retention::settle_expiry(result))?
    }

    pub(super) async fn load_verified_causal_identity<
        A: stream_provider::retained_image::RetainedOwnershipAttempt,
    >(
        &self,
        attempt: &mut A,
        input: (&CanonicalEventId, &[HeldCausalScope]),
        encoding: (&WorkLimits, &CanonicalJsonCodec),
    ) -> StorageResult<CanonicalEvent> {
        let mut settlement =
            infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
        let attempt = settlement.as_mut();
        let result = async {
            let (identity, scopes) = input;
            let accepted = self
                .load_acceptance_protected(attempt, identity, encoding.1, ReadProtection::Snapshot)
                .await?
                .ok_or_else(|| {
                    StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
                })?;
            let scope = scopes
                .iter()
                .find(|scope| scope.declaration.lineage == accepted.event.lineage())
                .ok_or_else(|| {
                    StorageError::from(storage_types::StorageEnum::HistoryUnavailable)
                })?;
            if !identity.is_bound_to(scope.declaration.lineage) {
                return Err(corrupt_record());
            }
            self.require_retained_publication(
                attempt,
                (
                    &scope.declaration.table,
                    &scope.cut,
                    identity,
                    accepted.receipt.digest,
                ),
                encoding,
            )
            .await?;
            Ok(accepted.event)
        }
        .await;
        settlement.complete(result)
    }
}

fn closure_too_large() -> StorageError {
    StorageError::validation("Complete causal closure exceeds the configured source work budget")
}
