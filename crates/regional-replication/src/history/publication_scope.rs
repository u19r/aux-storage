//! Configuration authorizes publications; it does not change their original
//! identity.
use replication_types::identity::{
    origin::OriginId,
    resource::{TableIdentity, TableLineage},
};

use super::source_cut::CanonicalSourceCut;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourcePublicationScope<'a> {
    pub(crate) lineage: TableLineage,
    pub(crate) origin: &'a OriginId,
    pub(crate) table: TableIdentity,
}

impl<'a> SourcePublicationScope<'a> {
    pub(crate) fn new(lineage: TableLineage, origin: &'a OriginId, table: TableIdentity) -> Self {
        Self {
            lineage,
            origin,
            table,
        }
    }
}

impl<'a> From<&'a CanonicalSourceCut> for SourcePublicationScope<'a> {
    fn from(cut: &'a CanonicalSourceCut) -> Self {
        Self::new(cut.lineage, &cut.origin, cut.source.table)
    }
}
