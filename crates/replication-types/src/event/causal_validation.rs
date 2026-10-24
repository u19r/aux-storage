use std::collections::BTreeSet;

use super::{
    content::{AcceptedItemEvent, CanonicalEventCodec, MutationCause},
    identity::{CanonicalEventId, OriginEventId, VersionIdentity},
    order::{EffectRank, WinnerOrder},
};
use crate::{constants::MAX_ITEM_EVENT_PARENTS, error::ReplicationProtocolError};

impl<I> AcceptedItemEvent<I> {
    /// Received and authored content must bind the same logical and canonical
    /// key.
    pub fn require_canonical_key(
        &self,
        codec: &impl CanonicalEventCodec,
    ) -> Result<(), ReplicationProtocolError> {
        if codec.encode_key(&self.key)? != self.canonical_key {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "content.try_validate",
            });
        }
        Ok(())
    }

    /// The same causal contract qualifies borrowed origin encoding and received
    /// originals.
    pub fn require_causal_shape(
        &self,
        identity: &CanonicalEventId,
    ) -> Result<(), ReplicationProtocolError> {
        identity.validate()?;
        self.version.validate()?;
        self.require_identity(identity)?;
        self.require_predecessor()?;
        self.require_parents(identity)?;
        self.require_predecessor_parent()?;
        self.require_observations()?;
        if self.cause == MutationCause::Expiration {
            self.require_expiration_parent()?;
        }
        Ok(())
    }

    fn require_identity(
        &self,
        identity: &CanonicalEventId,
    ) -> Result<(), ReplicationProtocolError> {
        match identity {
            CanonicalEventId::Origin(id) => self.require_mutation_identity(id),
            CanonicalEventId::Expiration { lineage, victim } => {
                self.require_expiration_identity((*lineage, victim))
            }
            _ => Err(invalid_identity()),
        }
    }

    fn require_expiration_identity(
        &self,
        identity: (crate::identity::resource::TableLineage, &VersionIdentity),
    ) -> Result<(), ReplicationProtocolError> {
        let (lineage, victim) = identity;
        if lineage != self.lineage
            || victim != &self.version
            || self.predecessor.as_ref() != Some(victim)
            || self.cause != MutationCause::Expiration
        {
            return Err(invalid_identity());
        }
        let victim_order = self.victim_order.as_ref().ok_or_else(invalid_identity)?;
        if victim_order.is_expiration()
            || !self.version.is_bound_to(victim_order, &self.canonical_key)
            || victim_order.expiration_child() != self.order
        {
            return Err(invalid_identity());
        }
        Ok(())
    }

    fn require_mutation_identity(
        &self,
        identity: &OriginEventId,
    ) -> Result<(), ReplicationProtocolError> {
        if identity.lineage != self.lineage
            || self.cause == MutationCause::Expiration
            || self.victim_order.is_some()
            || !matches!(&self.version, VersionIdentity::Mutation(version) if version == identity)
        {
            return Err(invalid_identity());
        }
        let ordered = match &self.order {
            WinnerOrder::Mutation {
                origin,
                position,
                ordinal,
                rank: EffectRank::Mutation,
                ..
            } => (origin, *position, *ordinal),
            _ => return Err(invalid_identity()),
        };
        if ordered != (&identity.origin, identity.position, identity.ordinal) {
            return Err(invalid_identity());
        }
        Ok(())
    }

    fn require_predecessor(&self) -> Result<(), ReplicationProtocolError> {
        let Some(version) = &self.predecessor else {
            return Ok(());
        };
        version.validate()?;
        if version.lineage() != self.lineage
            || matches!(version, VersionIdentity::Baseline { key, .. } if key != &self.canonical_key)
            || (self.cause != MutationCause::Expiration && version == &self.version)
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "content.validate_item_images",
            });
        }
        Ok(())
    }

    fn require_parents(&self, identity: &CanonicalEventId) -> Result<(), ReplicationProtocolError> {
        let invalid = || ReplicationProtocolError::IdentityContentMismatch {
            context: "content.validate_item_parents",
        };
        if self.parents.len() > MAX_ITEM_EVENT_PARENTS {
            return Err(invalid());
        }
        if self
            .parents
            .iter()
            .filter(|parent| matches!(parent, CanonicalEventId::Control { .. }))
            .count()
            > 1
        {
            return Err(invalid());
        }
        let mut parents = BTreeSet::new();
        for parent in &self.parents {
            parent.validate()?;
            if parent == identity || !parents.insert(parent) {
                return Err(invalid());
            }
            self.require_parent_scope(parent)?;
        }
        Ok(())
    }

    fn require_predecessor_parent(&self) -> Result<(), ReplicationProtocolError> {
        if matches!(&self.predecessor, Some(VersionIdentity::Mutation(_)))
            && !self
                .parents
                .iter()
                .any(|parent| self.is_predecessor_parent(parent))
        {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "missing item predecessor dependency",
            });
        }
        Ok(())
    }

    fn require_parent_scope(
        &self,
        parent: &CanonicalEventId,
    ) -> Result<(), ReplicationProtocolError> {
        if self
            .observations
            .iter()
            .any(|observed| observed.event.as_ref() == Some(parent))
            || self.is_predecessor_parent(parent)
            || matches!(parent, CanonicalEventId::Control { lineage, .. } if *lineage == self.lineage)
        {
            return Ok(());
        }
        Err(ReplicationProtocolError::IdentityContentMismatch {
            context: "unrecorded item dependency",
        })
    }

    fn is_predecessor_parent(&self, parent: &CanonicalEventId) -> bool {
        match (parent, &self.predecessor) {
            (CanonicalEventId::Origin(parent), Some(VersionIdentity::Mutation(version))) => {
                parent == version
            }
            (CanonicalEventId::Expiration { victim, .. }, Some(version)) => victim == version,
            _ => false,
        }
    }

    fn require_observations(&self) -> Result<(), ReplicationProtocolError> {
        let invalid = || ReplicationProtocolError::IdentityContentMismatch {
            context: "content.validate_item_observations",
        };
        if self.observations.len() > storage_types::MAX_TRANSACTION_MEMBERS
            || (!self.observations.is_empty()
                && (self.transaction.is_none() || self.cause == MutationCause::Expiration))
        {
            return Err(invalid());
        }
        let mut versions = BTreeSet::new();
        for observation in &self.observations {
            observation.validate()?;
            if !versions.insert(&observation.version)
                || observation.order >= self.order
                || !has_advanced_observation_clock(&self.order, &observation.order)
                || observation
                    .event
                    .as_ref()
                    .is_some_and(|event| !self.parents.contains(event))
            {
                return Err(invalid());
            }
        }
        Ok(())
    }

    fn require_expiration_parent(&self) -> Result<(), ReplicationProtocolError> {
        let expected = match &self.version {
            VersionIdentity::Mutation(victim) => {
                matches!(self.parents.as_slice(), [CanonicalEventId::Origin(parent)] if parent == victim)
            }
            VersionIdentity::Baseline { .. } => self.parents.is_empty(),
        };
        if !expected || self.transaction.is_some() {
            return Err(ReplicationProtocolError::IdentityContentMismatch {
                context: "expiration causal metadata",
            });
        }
        Ok(())
    }
}

fn invalid_identity() -> ReplicationProtocolError {
    ReplicationProtocolError::IdentityContentMismatch {
        context: "content.validate_item",
    }
}

fn has_advanced_observation_clock(child: &WinnerOrder, observed: &WinnerOrder) -> bool {
    match (child, observed) {
        (
            WinnerOrder::Mutation { clock: child, .. },
            WinnerOrder::Mutation {
                clock: observed, ..
            },
        ) => child > observed,
        (WinnerOrder::Mutation { .. }, WinnerOrder::Baseline { .. }) => true,
        _ => false,
    }
}
