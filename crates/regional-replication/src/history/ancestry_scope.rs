use replication_types::{
    configuration::membership::MembershipRevision, identity::resource::TableLineage,
    protocol::CanonicalDigest,
};
use serde::{Deserialize, Serialize};
use storage_types::{StorageError, StorageResult, TableName};

use super::checkpoint::CanonicalCheckpointId;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CanonicalAncestryScope {
    pub table: TableName,
    pub lineage: TableLineage,
    pub membership: MembershipRevision,
    pub checkpoint: CanonicalCheckpointId,
}

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(
    try_from = "Vec<CanonicalAncestryScope>",
    into = "Vec<CanonicalAncestryScope>"
)]
pub struct CanonicalAncestryScopes(Vec<CanonicalAncestryScope>);

impl TryFrom<Vec<CanonicalAncestryScope>> for CanonicalAncestryScopes {
    type Error = StorageError;
    fn try_from(scopes: Vec<CanonicalAncestryScope>) -> StorageResult<Self> {
        for (index, scope) in scopes.iter().enumerate() {
            if scopes[..index].iter().any(|previous| {
                previous.lineage == scope.lineage || previous.checkpoint == scope.checkpoint
            }) {
                return Err(StorageError::validation(
                    "Ancestry scopes must have distinct lineages and checkpoints",
                ));
            }
        }
        Ok(Self(scopes))
    }
}

impl From<CanonicalAncestryScopes> for Vec<CanonicalAncestryScope> {
    fn from(value: CanonicalAncestryScopes) -> Self {
        value.0
    }
}
impl AsRef<[CanonicalAncestryScope]> for CanonicalAncestryScopes {
    fn as_ref(&self) -> &[CanonicalAncestryScope] {
        &self.0
    }
}

impl CanonicalAncestryScopes {
    pub(crate) fn require_root(
        &self,
        root: &CanonicalAncestryScope,
        maximum: usize,
    ) -> StorageResult<()> {
        if self
            .0
            .len()
            .checked_add(1)
            .is_none_or(|count| count > maximum)
            || self
                .0
                .iter()
                .any(|scope| scope.lineage == root.lineage || scope.checkpoint == root.checkpoint)
        {
            return Err(StorageError::validation(
                "Ancestry scopes exceed the work budget or replace the root hold",
            ));
        }
        Ok(())
    }

    pub(crate) fn binding(&self) -> StorageResult<CanonicalDigest> {
        CanonicalDigest::of_json(&self.0)
            .map_err(|_| StorageError::internal("Ancestry scope encoding failed"))
    }
}
