use serde::{Deserialize, Serialize};
use storage_types::KeyAttributes;

use super::{
    content::{AcceptedItemEvent, MutationCause, SourceImages, SourceImagesRef},
    identity::{CanonicalEventId, CanonicalItemKey, VersionIdentity},
    order::{EffectRank, WinnerOrder, WinnerProvenance},
};
use crate::{
    configuration::table::SchemaRevision,
    error::ReplicationProtocolError,
    identity::resource::{TableIdentity, TableLineage},
    protocol::CanonicalDigest,
};

/// Immutable pre-capture authority. It has a deterministic version but no
/// invented original event, source position, or pre-enable stream record.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineItem<I = SourceImages> {
    pub source_table: TableIdentity,
    pub lineage: TableLineage,
    pub schema: SchemaRevision,
    pub key: KeyAttributes,
    pub canonical_key: CanonicalItemKey,
    pub images: I,
}

impl<I> BaselineItem<I> {
    pub fn provenance(&self, digest: CanonicalDigest) -> WinnerProvenance {
        WinnerProvenance {
            lineage: self.lineage,
            key: self.canonical_key.clone(),
            version: VersionIdentity::Baseline {
                lineage: self.lineage,
                key: self.canonical_key.clone(),
            },
            event: None,
            order: WinnerOrder::Baseline {
                lineage: self.lineage,
                key: self.canonical_key.clone(),
                rank: EffectRank::Mutation,
            },
            digest,
        }
    }
}

impl BaselineItem {
    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        validate_baseline_images(&self.key, SourceImagesRef::from(&self.images))
    }

    pub fn expiration_child(
        &self,
    ) -> Result<(CanonicalEventId, AcceptedItemEvent<SourceImagesRef<'_>>), ReplicationProtocolError>
    {
        self.validate()?;
        let image = self.images.new.as_ref().ok_or_else(invalid_baseline)?;
        let version = VersionIdentity::Baseline {
            lineage: self.lineage,
            key: self.canonical_key.clone(),
        };
        let order = WinnerOrder::Baseline {
            lineage: self.lineage,
            key: self.canonical_key.clone(),
            rank: EffectRank::Mutation,
        };
        Ok((
            version.clone().expiration_id(),
            AcceptedItemEvent {
                source_table: self.source_table,
                lineage: self.lineage,
                schema: self.schema,
                key: self.key.clone(),
                canonical_key: self.canonical_key.clone(),
                images: SourceImagesRef {
                    old: Some(image),
                    new: None,
                    old_indexers: self.images.new_indexers.as_deref(),
                    new_indexers: None,
                },
                order: order.expiration_child(),
                version: version.clone(),
                cause: MutationCause::Expiration,
                victim_order: Some(order),
                parents: Vec::new(),
                predecessor: Some(version),
                transaction: None,
                observations: Vec::new(),
            },
        ))
    }
}

impl BaselineItem<SourceImagesRef<'_>> {
    pub fn validate(&self) -> Result<(), ReplicationProtocolError> {
        validate_baseline_images(&self.key, self.images)
    }
}

fn validate_baseline_images(
    key: &KeyAttributes,
    images: SourceImagesRef<'_>,
) -> Result<(), ReplicationProtocolError> {
    images.require_valid_images(key)?;
    if images.old.is_some()
        || images.old_indexers.is_some()
        || images.new.is_some() != images.new_indexers.is_some()
    {
        return Err(invalid_baseline());
    }
    Ok(())
}

fn invalid_baseline() -> ReplicationProtocolError {
    ReplicationProtocolError::IdentityContentMismatch {
        context: "baseline authority",
    }
}
