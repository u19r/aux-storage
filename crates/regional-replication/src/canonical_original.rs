//! Shared retention accepts only validated decoded events or locally authored
//! originals.

use replication_provider::canonical_item_original::AuthoredItemOriginal;
use replication_types::{
    event::{
        content::{CanonicalEvent, CanonicalPayload},
        identity::{CanonicalEventId, CanonicalItemKey},
        order::{HybridLogicalClock, WinnerOrder},
        publication::CanonicalPublication,
    },
    identity::resource::TableLineage,
    protocol::CanonicalDigest,
};

#[derive(Clone, Copy)]
pub(crate) enum CanonicalOriginalRef<'original> {
    Received(&'original CanonicalEvent),
    Authored(&'original AuthoredItemOriginal),
}

impl<'original> From<&'original CanonicalEvent> for CanonicalOriginalRef<'original> {
    fn from(event: &'original CanonicalEvent) -> Self {
        Self::Received(event)
    }
}

impl<'original> From<&'original AuthoredItemOriginal> for CanonicalOriginalRef<'original> {
    fn from(event: &'original AuthoredItemOriginal) -> Self {
        Self::Authored(event)
    }
}

impl<'original> CanonicalOriginalRef<'original> {
    pub(crate) fn identity(self) -> &'original CanonicalEventId {
        match self {
            Self::Received(event) => &event.as_ref().id,
            Self::Authored(event) => event.publication().identity(),
        }
    }
    pub(crate) fn bytes(self) -> &'original [u8] {
        match self {
            Self::Received(event) => event.as_ref().bytes.as_ref(),
            Self::Authored(event) => event.bytes().as_ref(),
        }
    }
    pub(crate) fn digest(self) -> CanonicalDigest {
        match self {
            Self::Received(event) => event.as_ref().digest,
            Self::Authored(event) => event.publication().digest(),
        }
    }
    pub(crate) fn lineage(self) -> TableLineage {
        match self {
            Self::Received(event) => event.lineage(),
            Self::Authored(event) => event.winner().lineage,
        }
    }
    pub(crate) fn item_key(self) -> Option<&'original CanonicalItemKey> {
        match self {
            Self::Received(event) => match &event.as_ref().payload {
                CanonicalPayload::Item(item) => Some(&item.canonical_key),
                _ => None,
            },
            Self::Authored(event) => Some(&event.winner().key),
        }
    }
    pub(crate) fn item_clock(self) -> Option<HybridLogicalClock> {
        let order = match self {
            Self::Received(event) => match &event.as_ref().payload {
                CanonicalPayload::Item(item) => &item.order,
                _ => return None,
            },
            Self::Authored(event) => &event.winner().order,
        };
        match order {
            WinnerOrder::Mutation { clock, .. } => Some(*clock),
            WinnerOrder::Baseline { .. } => None,
        }
    }
    pub(crate) fn item_observations(self) -> Option<bool> {
        match self {
            Self::Received(event) => match &event.as_ref().payload {
                CanonicalPayload::Item(item) => Some(!item.observations.is_empty()),
                _ => None,
            },
            Self::Authored(event) => Some(event.has_observations()),
        }
    }
    pub(crate) fn publication(self) -> CanonicalPublication<'original> {
        match self {
            Self::Received(event) => event.into(),
            Self::Authored(event) => event.publication().clone(),
        }
    }
}
