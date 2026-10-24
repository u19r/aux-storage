use storage_provider::{ItemPrimaryEffects, PrimaryRevisionRecord};
use storage_types::{BoundTableInfo, KeyAttributes, StorageResult, storage_serde};

use super::primary_tombstone_key;
use crate::{KvMutation, catalog::KvTableIdentity, item_read::primary_item_key};

pub(crate) fn tombstone_mutation(
    table: &BoundTableInfo,
    key: &KeyAttributes,
    effects: &ItemPrimaryEffects,
) -> StorageResult<KvMutation> {
    let (tombstone_key, key_json) = primary_tombstone_key(table, key)?;
    let record = PrimaryRevisionRecord {
        table_name: table.info.table_name.as_ref(),
        key_json: key_json.as_str(),
        revision: effects.revision().get(),
        deleted: true,
        provenance: effects.provenance().clone(),
    };
    Ok(KvMutation::Put {
        key: tombstone_key,
        value: storage_serde::to_bytes(&record)?,
    })
}

pub(crate) fn primary_delete_mutations(
    identity: &KvTableIdentity,
    table: &BoundTableInfo,
    key: &KeyAttributes,
    effects: &ItemPrimaryEffects,
) -> StorageResult<Vec<KvMutation>> {
    let mut changes = Vec::new();
    changes
        .try_reserve_exact(2)
        .map_err(|_| storage_types::InfrastructureResultKind::NativeCapacityRejected)?;
    changes.push(KvMutation::Delete {
        key: primary_item_key(identity, table, key)?,
    });
    changes.push(tombstone_mutation(table, key, effects)?);
    Ok(changes)
}
