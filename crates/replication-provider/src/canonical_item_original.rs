//! Local authoring owns exact bytes and compact proof, without decoding its own
//! images.

use replication_types::{
    event::{
        order::WinnerProvenance,
        publication::{CanonicalPublication, ValidatedItemEvent},
    },
    protocol::{CanonicalBytes, CanonicalDigest},
};

/// Only the maintained codec can construct an authored item original.
/// Peer bytes must still pass the canonical event decoder.
pub struct AuthoredItemOriginal {
    bytes: CanonicalBytes,
    publication: CanonicalPublication<'static>,
    winner: WinnerProvenance,
    has_observations: bool,
}

impl AuthoredItemOriginal {
    pub(crate) fn from_encoding(bytes: CanonicalBytes, content: &ValidatedItemEvent<'_>) -> Self {
        let digest = CanonicalDigest::of(bytes.as_ref());
        let item = content.item();
        Self {
            bytes,
            publication: content.publication(digest).into_owned(),
            winner: WinnerProvenance {
                lineage: item.lineage,
                key: item.canonical_key.clone(),
                version: item.version.clone(),
                event: Some(content.identity().clone()),
                order: item.order.clone(),
                digest,
            },
            has_observations: !item.observations.is_empty(),
        }
    }

    pub fn bytes(&self) -> &CanonicalBytes {
        &self.bytes
    }
    pub fn publication(&self) -> &CanonicalPublication<'static> {
        &self.publication
    }
    pub fn winner(&self) -> &WinnerProvenance {
        &self.winner
    }
    pub fn has_observations(&self) -> bool {
        self.has_observations
    }
    pub fn into_bytes(self) -> CanonicalBytes {
        self.bytes
    }
}
