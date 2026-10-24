//! Compact content evidence permits publication binding without retaining event
//! images.

use std::borrow::Cow;

use super::{
    content::{
        AcceptedItemEvent, CanonicalEvent, CanonicalEventCodec, CanonicalPayload, SourceImagesRef,
    },
    identity::{CanonicalEventId, OriginEventId},
    interval::CanonicalEventLink,
    order::{HybridLogicalClock, WinnerOrder},
};
use crate::{
    error::ReplicationProtocolError,
    identity::{
        origin::OriginId,
        resource::{TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};

/// Constructed from validated received or authored content; never accepted from
/// a peer.
#[derive(Clone)]
pub struct CanonicalPublication<'event> {
    event: Cow<'event, CanonicalEventId>,
    digest: CanonicalDigest,
    table: TableIdentity,
    lineage: TableLineage,
    clock: PublicationClock,
    control: Option<Cow<'event, OriginEventId>>,
}

/// Borrowed item content qualified before its maintained codec authors an
/// original.
pub struct ValidatedItemEvent<'item> {
    identity: &'item CanonicalEventId,
    item: &'item AcceptedItemEvent<SourceImagesRef<'item>>,
}

impl<'item> ValidatedItemEvent<'item> {
    pub fn try_new(
        identity: &'item CanonicalEventId,
        item: &'item AcceptedItemEvent<SourceImagesRef<'item>>,
        codec: &impl CanonicalEventCodec,
    ) -> Result<Self, ReplicationProtocolError> {
        item.require_causal_shape(identity)?;
        item.images.require_item_shape(&item.key, item.cause)?;
        item.require_canonical_key(codec)?;
        Ok(Self { identity, item })
    }

    pub fn identity(&self) -> &'item CanonicalEventId {
        self.identity
    }
    pub fn item(&self) -> &'item AcceptedItemEvent<SourceImagesRef<'item>> {
        self.item
    }

    /// The codec supplies the digest of the exact encoding of this borrowed
    /// content.
    pub fn publication(&self, digest: CanonicalDigest) -> CanonicalPublication<'item> {
        let clock = PublicationClock::for_item(self.identity, &self.item.order);
        CanonicalPublication {
            event: Cow::Borrowed(self.identity),
            digest,
            table: self.item.source_table,
            lineage: self.item.lineage,
            clock,
            control: None,
        }
    }
}

#[derive(Clone, Copy)]
enum PublicationClock {
    Unconstrained,
    Exact(HybridLogicalClock),
    After(HybridLogicalClock),
}

impl<'event> From<&'event CanonicalEvent> for CanonicalPublication<'event> {
    fn from(event: &'event CanonicalEvent) -> Self {
        let candidate = event.as_ref();
        let clock = match &candidate.payload {
            CanonicalPayload::Item(item) => PublicationClock::for_item(&candidate.id, &item.order),
            _ => PublicationClock::Unconstrained,
        };
        let control = match &candidate.payload {
            CanonicalPayload::System(control) => Some(Cow::Borrowed(&control.publication)),
            _ => None,
        };
        Self {
            event: Cow::Borrowed(&candidate.id),
            digest: candidate.digest,
            table: event.source_table(),
            lineage: event.lineage(),
            clock,
            control,
        }
    }
}

impl CanonicalPublication<'_> {
    pub fn identity(&self) -> &CanonicalEventId {
        self.event.as_ref()
    }
    pub fn digest(&self) -> CanonicalDigest {
        self.digest
    }
    pub fn source_table(&self) -> TableIdentity {
        self.table
    }
    pub fn lineage(&self) -> TableLineage {
        self.lineage
    }

    pub fn into_owned(self) -> CanonicalPublication<'static> {
        CanonicalPublication {
            event: Cow::Owned(self.event.into_owned()),
            digest: self.digest,
            table: self.table,
            lineage: self.lineage,
            clock: self.clock,
            control: self
                .control
                .map(|publication| Cow::Owned(publication.into_owned())),
        }
    }

    pub fn require_link(
        &self,
        link: &CanonicalEventLink,
        origin: &OriginId,
    ) -> Result<(), ReplicationProtocolError> {
        link.require_source(origin)?;
        if self.event.as_ref() != &link.event
            || self.digest != link.digest
            || self.lineage != link.lineage
            || (self.table != link.table
                && !matches!(self.event.as_ref(), CanonicalEventId::Expiration { .. }))
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "publication event link",
            });
        }
        self.clock.require_clock(link.publication_clock)?;
        if let Some(publication) = &self.control
            && (publication.origin != *origin
                || publication.position != link.position
                || publication.ordinal != link.ordinal)
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "control publication link",
            });
        }
        Ok(())
    }
}

impl PublicationClock {
    fn for_item(identity: &CanonicalEventId, order: &WinnerOrder) -> Self {
        match order {
            WinnerOrder::Mutation { clock, .. }
                if matches!(identity, CanonicalEventId::Expiration { .. }) =>
            {
                Self::After(*clock)
            }
            WinnerOrder::Mutation { clock, .. } => Self::Exact(*clock),
            WinnerOrder::Baseline { .. } => Self::Unconstrained,
        }
    }

    fn require_clock(self, clock: HybridLogicalClock) -> Result<(), ReplicationProtocolError> {
        let valid = match self {
            Self::Unconstrained => true,
            Self::Exact(expected) => clock == expected,
            Self::After(previous) => clock > previous,
        };
        if !valid {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "publication clock",
            });
        }
        Ok(())
    }
}
