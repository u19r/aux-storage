//! Root-fenced physical observations and verified logical leaf reconstruction.
use std::collections::BTreeMap;

use storage_types::{
    AttributeMap, BoundTableInfo, IndexName, ItemKey, ReadProtection, StorageResult,
};

use super::{
    tree_codec::{Branch, NodeId, Root, bit, corrupt},
    write::{HydratedProjection, hydrate_projections},
};
use crate::{KvAttempt, KvEntry, KvMutation, catalog::KvCatalogAttempt};

#[derive(Default)]
pub(crate) struct TreeChanges {
    protected_roots: BTreeMap<Vec<u8>, Option<Root>>,
    pub(super) rows: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    pub(super) words: BTreeMap<Vec<u8>, Vec<u8>>,
}
impl TreeChanges {
    pub(crate) fn into_mutations(self) -> StorageResult<Vec<KvMutation>> {
        let mut mutations = Vec::new();
        mutations
            .try_reserve_exact(self.rows.len())
            .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
        for (key, value) in self.rows {
            mutations.push(match value {
                Some(value) => KvMutation::PutUnchunked { key, value },
                None => KvMutation::DeleteUnchunked { key },
            });
        }
        Ok(mutations)
    }
    pub(super) async fn load<A: KvAttempt>(
        &self,
        attempt: &A,
        key: Vec<u8>,
        protection: ReadProtection,
        maximum: usize,
    ) -> StorageResult<Option<Vec<u8>>> {
        if let Some(value) = self.rows.get(&key) {
            if value.as_ref().is_some_and(|value| value.len() > maximum) {
                return Err(corrupt());
            }
            return Ok(value.clone());
        }
        let mut values = crate::value_layout::load_value_heads(
            attempt,
            &[key],
            protection,
            crate::KvValueAllowance::new(maximum, maximum).for_head_reads(1)?,
        )
        .await?;
        if values.len() != 1 {
            return Err(corrupt());
        }
        values
            .pop()
            .flatten()
            .map(|raw| {
                crate::value_layout::decode_unchunked_value(&raw, maximum).map(<[u8]>::to_vec)
            })
            .transpose()
    }
}
pub(super) struct PathStep {
    pub id: NodeId,
    pub branch: Branch,
    pub side: usize,
}
pub(super) struct TreePath {
    pub steps: Vec<PathStep>,
    pub leaf: NodeId,
}
pub(super) struct VerifiedLeaf {
    pub word: Vec<u8>,
    pub value: Vec<u8>,
    pub image: HydratedProjection,
}

impl TreeChanges {
    pub(super) async fn descend<A: KvAttempt>(
        &self,
        attempt: &A,
        scope: &[u8; 6],
        child: NodeId,
        word: &[u8],
    ) -> StorageResult<TreePath> {
        let mut node = child;
        let mut steps: Vec<PathStep> = Vec::new();
        while let NodeId::Branch(_) = node {
            let bytes = self
                .load(attempt, node.key(scope), ReadProtection::Snapshot, 71)
                .await?
                .ok_or_else(corrupt)?;
            let branch = Branch::decode(&bytes, steps.last().map(|step| step.branch.bit))?;
            let side = bit(word, branch.bit);
            let next = branch.children[side];
            steps.push(PathStep {
                id: node,
                branch,
                side,
            });
            node = next;
        }
        Ok(TreePath { steps, leaf: node })
    }
}

pub(super) async fn load_leaf<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    input: LeafInput<'_>,
) -> StorageResult<VerifiedLeaf> {
    let value = load_leaf_value(catalog, input.scope, input.node).await?;
    hydrate_leaf(catalog, input, value).await
}

pub(super) struct LeafInput<'a> {
    pub table: &'a BoundTableInfo,
    pub index: &'a IndexName,
    pub scope: &'a [u8; 6],
    pub node: NodeId,
}
pub(super) async fn load_leaf_value<A: KvAttempt>(
    catalog: &KvCatalogAttempt<'_, A>,
    scope: &[u8; 6],
    node: NodeId,
) -> StorageResult<Vec<u8>> {
    let NodeId::Leaf(id) = node else {
        return Err(corrupt());
    };
    let maximum =
        stream_provider::retained_image::RetainedProjectionRecord::encoded_length_bound()?
            .checked_add(10)
            .ok_or_else(corrupt)?;
    let value = catalog
        .projection_tree
        .load(
            catalog.attempt,
            node.key(scope),
            ReadProtection::Snapshot,
            maximum,
        )
        .await?
        .ok_or_else(corrupt)?;
    let tombstone = [
        vec![crate::constants::GSI_TOMBSTONE_FAMILY],
        scope.to_vec(),
        id.to_vec(),
    ]
    .concat();
    if catalog
        .projection_tree
        .load(catalog.attempt, tombstone, ReadProtection::Snapshot, 8)
        .await?
        .is_some()
    {
        return Err(corrupt());
    }
    Ok(value)
}
pub(super) async fn hydrate_leaf<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    input: LeafInput<'_>,
    value: Vec<u8>,
) -> StorageResult<VerifiedLeaf> {
    let LeafInput {
        table,
        index,
        scope,
        node,
    } = input;
    let NodeId::Leaf(id) = node else {
        return Err(corrupt());
    };
    let key = node.key(scope);
    let target = storage_backfill::GsiBuildTarget {
        table_name: table.table_name.clone(),
        index_name: index.clone(),
        incarnation: (*scope).into(),
    };
    let mut images =
        hydrate_projections(catalog, table, &[KvEntry::new(key, value.clone())], &target).await?;
    if images.len() != 1 {
        return Err(corrupt());
    }
    let image = images.pop().ok_or_else(corrupt)?;
    let word = image_word(table, index, &image.image)?;
    if super::key_layout::projection_address(&word) != id {
        return Err(corrupt());
    }
    Ok(VerifiedLeaf { word, value, image })
}
pub(super) fn image_word(
    table: &BoundTableInfo,
    index: &IndexName,
    image: &storage_types::WireItem,
) -> StorageResult<Vec<u8>> {
    let definition = table
        .global_secondary_indexes
        .as_deref()
        .unwrap_or_default()
        .iter()
        .find(|candidate| &candidate.index_name == index)
        .ok_or_else(corrupt)?;
    let attributes = AttributeMap::try_from(image)?;
    let key = ItemKey::from_key_schema_for_index(
        table.table_name.clone(),
        &table.key_schema,
        index,
        &definition.key_schema,
        &attributes,
    )?
    .ok_or_else(corrupt)?;
    super::key_layout::ordered_projection_suffix(&key)
}
pub(super) fn require_path(path: &TreePath, word: &[u8], partition: &[u8]) -> StorageResult<()> {
    if word.get(..32) != Some(partition)
        || path
            .steps
            .iter()
            .any(|step| bit(word, step.branch.bit) != step.side)
    {
        return Err(corrupt());
    }
    Ok(())
}
pub(super) async fn load_root<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    key: Vec<u8>,
    protection: ReadProtection,
) -> StorageResult<Option<Root>> {
    if !catalog.projection_tree.rows.contains_key(&key)
        && let Some(root) = catalog.projection_tree.protected_roots.get(&key)
    {
        return Ok(root.clone());
    }
    let root = catalog
        .projection_tree
        .load(catalog.attempt, key.clone(), protection, 42)
        .await?
        .map(|bytes| Root::decode(&bytes))
        .transpose()?;
    if protection == ReadProtection::Conflict {
        catalog
            .projection_tree
            .protected_roots
            .insert(key, root.clone());
    }
    Ok(root)
}
