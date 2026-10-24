//! Compact structural changes join the caller's pending physical mutation
//! batch.
use storage_types::{BoundTableInfo, IndexName, ReadProtection, StorageResult};

use super::{
    tree_codec::{Branch, NodeId, Root, bit, corrupt, divergence, root_key},
    tree_read::{LeafInput, TreePath, load_leaf, load_root, require_path},
};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

pub(super) struct TreeWrite<'a> {
    pub table: &'a BoundTableInfo,
    pub index: &'a IndexName,
    pub scope: [u8; 6],
    pub word: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub tombstone: Option<Vec<u8>>,
}

pub(super) async fn store_tree<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    write: TreeWrite<'_>,
) -> StorageResult<()> {
    let key = root_key(&write.scope, &write.word)?;
    let root = load_root(catalog, key.clone(), ReadProtection::Conflict).await?;
    let leaf = NodeId::Leaf(super::key_layout::projection_address(&write.word));
    let child = match root.as_ref() {
        None => {
            if catalog
                .projection_tree
                .load(
                    catalog.attempt,
                    leaf.key(&write.scope),
                    ReadProtection::Snapshot,
                    stream_provider::retained_image::RetainedProjectionRecord::encoded_length_bound(
                    )? + 10,
                )
                .await?
                .is_some()
            {
                return Err(corrupt());
            }
            write.value.as_ref().map(|_| leaf)
        }
        Some(root) => change_path(catalog, &write, root.child, leaf).await?,
    };
    let value = child
        .map(|child| {
            root.as_ref()
                .map_or(Ok(1), |root| {
                    root.revision.checked_add(1).ok_or_else(corrupt)
                })
                .map(|revision| Root { revision, child }.encode())
        })
        .transpose()?;
    catalog.projection_tree.rows.insert(key, value);
    let live = leaf.key(&write.scope);
    catalog
        .projection_tree
        .rows
        .insert(live.clone(), write.value);
    catalog.projection_tree.words.insert(live, write.word);
    let NodeId::Leaf(id) = leaf else {
        return Err(corrupt());
    };
    let tombstone = [
        vec![crate::constants::GSI_TOMBSTONE_FAMILY],
        write.scope.to_vec(),
        id.to_vec(),
    ]
    .concat();
    catalog
        .projection_tree
        .rows
        .insert(tombstone, write.tombstone);
    Ok(())
}
async fn change_path<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    write: &TreeWrite<'_>,
    root: NodeId,
    leaf: NodeId,
) -> StorageResult<Option<NodeId>> {
    let path = catalog
        .projection_tree
        .descend(catalog.attempt, &write.scope, root, &write.word)
        .await?;
    let key = path.leaf.key(&write.scope);
    let word = match catalog.projection_tree.words.get(&key) {
        Some(word) => word.clone(),
        None => {
            load_leaf(
                catalog,
                LeafInput {
                    table: write.table,
                    index: write.index,
                    scope: &write.scope,
                    node: path.leaf,
                },
            )
            .await?
            .word
        }
    };
    require_path(&path, &word, &write.word[..32])?;
    if word == write.word {
        if path.leaf != leaf {
            return Err(corrupt());
        }
        return if write.value.is_some() {
            Ok(Some(root))
        } else {
            remove_leaf(catalog, write.scope, path)
        };
    }
    if catalog
        .projection_tree
        .load(
            catalog.attempt,
            leaf.key(&write.scope),
            ReadProtection::Snapshot,
            stream_provider::retained_image::RetainedProjectionRecord::encoded_length_bound()? + 10,
        )
        .await?
        .is_some()
    {
        return Err(corrupt());
    }
    if write.value.is_none() {
        return Ok(Some(root));
    }
    insert_leaf(catalog, write, (root, path), (leaf, word))
        .await
        .map(Some)
}
async fn insert_leaf<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    write: &TreeWrite<'_>,
    observed: (NodeId, TreePath),
    incoming: (NodeId, Vec<u8>),
) -> StorageResult<NodeId> {
    let (root, path) = observed;
    let (leaf, old) = incoming;
    let position = divergence(&write.word, &old)?;
    let split = path
        .steps
        .iter()
        .position(|step| step.branch.bit > position)
        .unwrap_or(path.steps.len());
    let old_child = path.steps.get(split).map_or(path.leaf, |step| step.id);
    let branch_id = NodeId::Branch(uuid::Uuid::new_v4().into_bytes());
    let key = branch_id.key(&write.scope);
    if catalog.projection_tree.rows.contains_key(&key)
        || catalog
            .projection_tree
            .load(catalog.attempt, key.clone(), ReadProtection::Conflict, 71)
            .await?
            .is_some()
    {
        return Err(corrupt());
    }
    let side = bit(&write.word, position);
    let mut children = [old_child; 2];
    children[side] = leaf;
    catalog.projection_tree.rows.insert(
        key,
        Some(
            Branch {
                bit: position,
                children,
            }
            .encode()?,
        ),
    );
    if split == 0 {
        return Ok(branch_id);
    }
    replace_parent(catalog, write.scope, &path.steps[split - 1], branch_id)?;
    Ok(root)
}
fn remove_leaf<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    scope: [u8; 6],
    mut path: TreePath,
) -> StorageResult<Option<NodeId>> {
    let Some(parent) = path.steps.pop() else {
        return Ok(None);
    };
    let sibling = parent.branch.children[1 - parent.side];
    catalog
        .projection_tree
        .rows
        .insert(parent.id.key(&scope), None);
    let Some(grandparent) = path.steps.last() else {
        return Ok(Some(sibling));
    };
    replace_parent(catalog, scope, grandparent, sibling)?;
    Ok(path.steps.first().map(|step| step.id))
}
fn replace_parent<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    scope: [u8; 6],
    parent: &super::tree_read::PathStep,
    child: NodeId,
) -> StorageResult<()> {
    let mut branch = parent.branch.clone();
    branch.children[parent.side] = child;
    catalog
        .projection_tree
        .rows
        .insert(parent.id.key(&scope), Some(branch.encode()?));
    Ok(())
}
