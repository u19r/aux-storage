use storage_types::{ItemProvenance, ItemRevision, StorageResult};

use crate::AcceptedUpsert;

/// The shared executor chooses these values after stream allocation and
/// provenance decisions; SQL and KV only encode and persist them.
pub struct ItemPrimaryEffects {
    revision: ItemRevision,
    provenance: ItemProvenance,
    ttl_epoch: Option<i64>,
}

impl AcceptedUpsert {
    /// Continue the protected revision after the shared stream and
    /// replication decisions establish any physical revision floor.
    pub fn primary_effects_at_least(
        &self,
        floor: ItemRevision,
        provenance: ItemProvenance,
        ttl_epoch: Option<i64>,
    ) -> StorageResult<ItemPrimaryEffects> {
        Ok(ItemPrimaryEffects {
            revision: ItemRevision::next_at_least(self.old_revision, floor.get())?,
            provenance,
            ttl_epoch,
        })
    }
}

impl ItemPrimaryEffects {
    pub fn for_delete(
        old_revision: Option<ItemRevision>,
        floor: ItemRevision,
        provenance: ItemProvenance,
    ) -> StorageResult<Self> {
        Ok(Self {
            revision: ItemRevision::next_at_least(old_revision, floor.get())?,
            provenance,
            ttl_epoch: None,
        })
    }

    #[must_use]
    pub fn revision(&self) -> ItemRevision {
        self.revision
    }

    #[must_use]
    pub fn provenance(&self) -> &ItemProvenance {
        &self.provenance
    }

    #[must_use]
    pub fn ttl_epoch(&self) -> Option<i64> {
        self.ttl_epoch
    }
}
