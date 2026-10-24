//! Staged facts and one atomic native activation per installed enable.
use replication_types::{
    configuration::installed::RegionalSourceBinding,
    identity::{
        origin::OriginId,
        resource::{ControlOperationId, TableIdentity, TableLineage},
    },
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::TableHistoryId;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogReference {
    pub operation: ControlOperationId,
    pub ordinal: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogSlot {
    pub local: TableIdentity,
    pub physical: TableHistoryId,
    pub lineage: TableLineage,
    pub source: RegionalSourceBinding,
    pub origin: OriginId,
    pub candidate: CatalogReference,
    pub previous: Option<CatalogReference>,
}
#[derive(PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogActivation {
    pub local: TableIdentity,
    pub physical: TableHistoryId,
    pub operation: ControlOperationId,
    pub definition: CanonicalDigest,
    pub cuts: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogLineageReference {
    pub first: CatalogReference,
    pub last: u64,
    pub count: u64,
    pub vector: CanonicalDigest,
}
impl CatalogLineageReference {
    pub(super) fn has_reference(&self, reference: &CatalogReference) -> bool {
        self.first.operation == reference.operation
            && reference.ordinal >= self.first.ordinal
            && reference.ordinal <= self.last
            && self.count != 0
            && self
                .last
                .checked_sub(self.first.ordinal)
                .and_then(|span| span.checked_add(1))
                .is_some_and(|span| self.count <= span)
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogLineage {
    pub local: TableIdentity,
    pub physical: TableHistoryId,
    pub lineage: TableLineage,
    pub membership: replication_types::configuration::membership::MembershipRevision,
    pub candidate: CatalogLineageReference,
    pub previous: Option<CatalogLineageReference>,
}
impl CatalogLineage {
    pub(super) fn seed(&self) -> storage_types::StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(self.physical, self.lineage, self.membership))
            .map_err(|_| crate::native_record::corrupt_record())
    }
    pub(super) fn fold(
        vector: CanonicalDigest,
        cut: &crate::bootstrap::ReenableHistoryCut,
    ) -> storage_types::StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&(vector, cut)).map_err(|_| crate::native_record::corrupt_record())
    }
}
