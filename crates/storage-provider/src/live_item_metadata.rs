use storage_common::TtlIndexKey;
use storage_types::ItemProvenance;

/// Metadata persisted with the live primary image.
pub struct LiveItemMetadata<'a> {
    provenance: ItemProvenance,
    ttl_key: Option<&'a TtlIndexKey>,
}

impl<'a> LiveItemMetadata<'a> {
    pub fn new(provenance: ItemProvenance, ttl_key: Option<&'a TtlIndexKey>) -> Self {
        Self {
            provenance,
            ttl_key,
        }
    }
    pub fn provenance(&self) -> &ItemProvenance {
        &self.provenance
    }
    pub fn ttl_epoch(&self) -> Option<i64> {
        self.ttl_key.map(TtlIndexKey::ttl_seconds)
    }
    pub fn ttl_key(&self) -> Option<&TtlIndexKey> {
        self.ttl_key
    }
}
