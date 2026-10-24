use storage_types::{ItemProvenance, ItemRevision, KeyAttributes};

/// A deletion's key and winner authority. There is no live item image.
pub struct ItemTombstone {
    pub key: KeyAttributes,
    pub revision: ItemRevision,
    pub provenance: ItemProvenance,
}
