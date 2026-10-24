//! Protected compact destination state agrees with root-fenced tree membership.
use std::num::NonZeroUsize;

use storage_backfill::{GsiProjection, GsiProjectionKey, GsiProjectionStateReadBatch};
use storage_types::{BoundTableInfo, StorageError, StorageResult};

use super::{
    key_layout::{ProjectionKeys, projection_keys},
    read::target_index,
    tree_codec::{corrupt, root_key},
    tree_read::{LeafInput, load_leaf, load_root, require_path},
    write::{PhysicalProjection, decode_projection_state},
};
use crate::{
    KvAttempt, ReadProtection,
    catalog::{KvCatalogAttempt, KvTableIdentity},
    expiration::key_layout::encode_index_key,
};

struct ReadSlot {
    source_ordinal: usize,
    live_index: usize,
    tree: Option<ProjectionKeys>,
}

pub(crate) async fn read_projection_states<A: KvAttempt>(
    catalog: &mut KvCatalogAttempt<'_, A>,
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    batch: GsiProjectionStateReadBatch<'_>,
) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
    if identity.deleted
        || identity.table_name != batch.table.table_name
        || table.info.table_name != batch.table.table_name
    {
        return Err(StorageError::internal(
            "KV GSI destination table has changed",
        ));
    }
    if !batch.target.index_name.is_ttl_index() {
        target_index(identity, batch.target)?;
    }
    let (keys, slots) = plan_projection_reads(identity, table, &batch)?;
    let descriptor =
        stream_provider::retained_image::RetainedProjectionRecord::encoded_length_bound()?;
    let maximum = (descriptor + 10 + crate::constants::GSI_REVISION_BYTES)
        .checked_mul(slots.len())
        .ok_or_else(corrupt)?;
    let expected = keys.len();
    let mut current = Vec::new();
    current
        .try_reserve_exact(batch.sources.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    current.resize_with(batch.sources.len(), || None);
    let mut values = catalog
        .attempt
        .get_batch_bounded(
            keys,
            ReadProtection::Conflict,
            NonZeroUsize::new(maximum.max(1)).ok_or_else(corrupt)?,
        )
        .await?;
    if values.len() != expected {
        return Err(corrupt());
    }
    for slot in &slots {
        if let Some(tree) = &slot.tree {
            for (offset, key) in [&tree.live, &tree.tombstone].into_iter().enumerate() {
                if let Some(pending) = catalog.projection_tree.rows.get(key) {
                    values[slot.live_index + offset] = pending
                        .as_deref()
                        .map(super::write::copy_descriptor_bytes)
                        .transpose()?;
                }
            }
        }
    }
    for slot in slots {
        let source = batch.sources.get(slot.source_ordinal).ok_or_else(corrupt)?;
        let live = values[slot.live_index].as_deref();
        let state = if let Some(tree) = slot.tree {
            let state =
                decode_projection_state(table, live, values[slot.live_index + 1].as_deref())?;
            tree.verify_membership(catalog, table, &batch.target.index_name, &state)
                .await?;
            match state {
                Some(PhysicalProjection::Live(revision)) => Some((revision, true)),
                Some(PhysicalProjection::Tombstone(revision)) => Some((revision, false)),
                None => None,
            }
        } else {
            live.map(|_| (source.item_version(), true))
        };
        if let Some((version, live)) = state {
            current[slot.source_ordinal] =
                Some(GsiProjection::from_stored_state(source, version, live)?);
        }
    }
    Ok(current)
}

fn plan_projection_reads(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    batch: &GsiProjectionStateReadBatch<'_>,
) -> StorageResult<(Vec<Vec<u8>>, Vec<ReadSlot>)> {
    let count = batch
        .reads
        .len()
        .checked_mul(2)
        .ok_or(storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    let mut keys = Vec::new();
    let mut slots = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    slots
        .try_reserve_exact(batch.reads.len())
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    for read in batch.reads {
        if read.key.index_name != batch.target.index_name {
            return Err(StorageError::internal(
                "KV GSI destination index has changed",
            ));
        }
        let live_index = keys.len();
        let tree = if let Some(ttl) = &read.key.ttl_index_key {
            keys.push(encode_index_key(identity.table_id, ttl)?);
            None
        } else {
            let pair = projection_keys(identity, table, read.key)?;
            keys.push(super::write::copy_descriptor_bytes(&pair.live)?);
            keys.push(super::write::copy_descriptor_bytes(&pair.tombstone)?);
            Some(pair)
        };
        slots.push(ReadSlot {
            source_ordinal: read.source_ordinal,
            live_index,
            tree,
        });
    }
    Ok((keys, slots))
}

impl ProjectionKeys {
    async fn verify_membership<A: KvAttempt>(
        &self,
        catalog: &mut KvCatalogAttempt<'_, A>,
        table: &BoundTableInfo,
        index: &storage_types::IndexName,
        state: &Option<PhysicalProjection>,
    ) -> StorageResult<()> {
        let tree = self;
        let mut found = None;
        if let Some(root) = load_root(
            catalog,
            root_key(&tree.scope, &tree.word)?,
            ReadProtection::Conflict,
        )
        .await?
        {
            let path = catalog
                .projection_tree
                .descend(catalog.attempt, &tree.scope, root.child, &tree.word)
                .await?;
            let leaf = load_leaf(
                catalog,
                LeafInput {
                    table,
                    index,
                    scope: &tree.scope,
                    node: path.leaf,
                },
            )
            .await?;
            require_path(&path, &leaf.word, &tree.word[..32])?;
            if leaf.word == tree.word {
                found = Some(leaf.image.revision);
            }
        }
        match (state, found) {
            (Some(PhysicalProjection::Live(expected)), Some(actual)) if *expected == actual => {
                Ok(())
            }
            (Some(PhysicalProjection::Tombstone(_)) | None, None) => Ok(()),
            _ => Err(StorageError::internal(
                "GSI tree and compact authority differ",
            )),
        }
    }
}
