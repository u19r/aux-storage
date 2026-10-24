//! One recorded-parent and item-edge policy for closure and streaming
//! traversal.
use replication_types::{
    configuration::{
        installed::{InstalledTableState, RegionalTablePhase},
        table::RegionalMode,
    },
    event::{
        content::{
            AcceptedControlEvent, AcceptedItemEvent, CanonicalEvent, CanonicalPayload,
            MutationCause,
        },
        identity::{CanonicalEventId, OriginEventId, VersionIdentity},
        order::WinnerOrder,
    },
    identity::origin::{OriginId, OriginPosition},
};
use storage_types::StorageResult;

use crate::native_record::corrupt_record;

pub(super) struct RecordedAncestry<'a> {
    event: &'a CanonicalEvent,
}
impl<'a> RecordedAncestry<'a> {
    pub(super) fn new(event: &'a CanonicalEvent) -> Self {
        Self { event }
    }
    pub(super) fn count(&self) -> usize {
        match &self.event.as_ref().payload {
            CanonicalPayload::Item(item) => {
                item.parents.len()
                    + usize::from(
                        self.predecessor()
                            .is_some_and(|id| !item.parents.contains(&id)),
                    )
            }
            CanonicalPayload::System(control) => usize::from(control.predecessor.is_some()),
            _ => 0,
        }
    }
    pub(super) fn parent(&self, index: usize) -> Option<CanonicalEventId> {
        let CanonicalPayload::Item(item) = &self.event.as_ref().payload else {
            return if index == 0 { self.predecessor() } else { None };
        };
        if let Some(id) = item.parents.get(index) {
            return Some(id.clone());
        }
        if index != item.parents.len() {
            return None;
        }
        self.predecessor().filter(|id| !item.parents.contains(id))
    }
    pub(super) fn require_parent(&self, parent: &CanonicalEvent) -> StorageResult<()> {
        if !(0..self.count()).any(|index| self.parent(index).as_ref() == Some(&parent.as_ref().id))
        {
            return Err(corrupt_record());
        }
        match (&self.event.as_ref().payload, &parent.as_ref().payload) {
            (CanonicalPayload::Item(child), CanonicalPayload::Item(_)) => {
                require_item_parent(child, parent)
            }
            (CanonicalPayload::Item(child), CanonicalPayload::System(control))
                if self.event.lineage() == parent.lineage() =>
            {
                require_configuration_parent(child, control)
            }
            (CanonicalPayload::System(child), CanonicalPayload::System(control)) => {
                require_control_predecessor(child, control)
            }
            _ => Err(corrupt_record()),
        }
    }
    fn predecessor(&self) -> Option<CanonicalEventId> {
        match &self.event.as_ref().payload {
            CanonicalPayload::Item(item) => match &item.predecessor {
                Some(VersionIdentity::Mutation(id)) => Some(CanonicalEventId::Origin(id.clone())),
                _ => None,
            },
            CanonicalPayload::System(control) => control.predecessor_identity(),
            _ => None,
        }
    }
}

fn require_item_parent(child: &AcceptedItemEvent, parent: &CanonicalEvent) -> StorageResult<()> {
    let CanonicalPayload::Item(item) = &parent.as_ref().payload else {
        return Err(corrupt_record());
    };
    if let Some(observed) = child
        .observations
        .iter()
        .find(|observed| observed.event.as_ref() == Some(&parent.as_ref().id))
    {
        let authority = super::reconstruction::WinnerAuthority::from_event(parent.clone())
            .ok_or_else(corrupt_record)?;
        return if &authority.provenance()? == observed && observed.order < child.order {
            Ok(())
        } else {
            Err(corrupt_record())
        };
    }
    if child.lineage != item.lineage
        || child.canonical_key != item.canonical_key
        || item.order >= child.order
    {
        return Err(corrupt_record());
    }
    Ok(())
}

fn require_control_predecessor(
    child: &AcceptedControlEvent,
    parent: &AcceptedControlEvent,
) -> StorageResult<()> {
    if child.predecessor != Some(parent.proposal.operation().id) {
        return Err(corrupt_record());
    }
    if child.proposal.operation().lineage != parent.proposal.operation().lineage
        && !matches!(
            parent.proposal.operation().change,
            replication_types::configuration::control::ConfigurationChange::DisableGlobal
        )
    {
        return Err(corrupt_record());
    }
    for participant in child.proposal.participants() {
        let Some(before) = &participant.before else {
            continue;
        };
        let mut before = before.clone();
        if before.phase == RegionalTablePhase::Sealing {
            before.phase = RegionalTablePhase::Active;
        }
        if before
            != parent
                .proposal
                .participant(&participant.region)
                .map_err(|_| corrupt_record())?
                .proposed
        {
            return Err(corrupt_record());
        }
    }
    require_prior_publication(
        &parent.publication,
        (&child.publication.origin, child.publication.position),
    )
}
fn require_configuration_parent(
    child: &AcceptedItemEvent,
    control: &AcceptedControlEvent,
) -> StorageResult<()> {
    let WinnerOrder::Mutation {
        origin, position, ..
    } = &child.order
    else {
        return Err(corrupt_record());
    };
    if child.cause == MutationCause::Expiration {
        return Err(corrupt_record());
    }
    let participant = control
        .proposal
        .participant(&origin.region)
        .map_err(|_| corrupt_record())?;
    let state = &participant.proposed;
    require_configuration_scope(child, state)?;
    require_configuration_origin(child, state, origin)?;
    require_prior_publication(&control.publication, (origin, *position))
}

fn require_configuration_scope(
    child: &AcceptedItemEvent,
    state: &InstalledTableState,
) -> StorageResult<()> {
    let configuration = &state.configuration;
    if state.phase != RegionalTablePhase::Active
        || !matches!(configuration.regional, RegionalMode::Global { .. })
        || configuration.table != child.source_table
        || configuration.lineage != child.lineage
        || configuration.schema != child.schema
    {
        return Err(corrupt_record());
    }
    Ok(())
}

fn require_configuration_origin(
    child: &AcceptedItemEvent,
    state: &InstalledTableState,
    origin: &OriginId,
) -> StorageResult<()> {
    let source = state
        .sources
        .iter()
        .find(|source| source.region == origin.region)
        .ok_or_else(corrupt_record)?;
    if source.table != child.source_table
        || source.config != state.configuration.config
        || source.database_group != origin.database_group
        || source.generation != origin.journal_generation
    {
        return Err(corrupt_record());
    }
    source
        .partitions
        .require_partition(origin.journal_partition)
        .map_err(|_| corrupt_record())
}

fn require_prior_publication(
    publication: &OriginEventId,
    child: (&OriginId, OriginPosition),
) -> StorageResult<()> {
    if &publication.origin == child.0 && publication.position >= child.1 {
        return Err(corrupt_record());
    }
    Ok(())
}
