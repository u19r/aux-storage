//! Protected GSI lifecycle and readiness observations.

use storage_backfill::GsiBuildTarget;
use storage_types::{StorageError, StorageResult};

use super::key_layout::index_incarnation;
use crate::catalog::{KvIndexKind, KvTableIdentity};

pub(super) fn target_index(
    identity: &KvTableIdentity,
    target: &GsiBuildTarget,
) -> StorageResult<u16> {
    let index = identity
        .indexes
        .iter()
        .find(|index| {
            index.index_kind == KvIndexKind::UserGsi && index.index_name == target.index_name
        })
        .ok_or_else(|| StorageError::internal("KV GSI target index is absent"))?;
    if identity.deleted
        || target.history_id != identity.table_id.into()
        || identity.table_name != target.table_name
        || target.incarnation.as_ref()
            != index_incarnation(identity.table_id, index.index_id).as_slice()
    {
        return Err(StorageError::internal("KV GSI target has changed"));
    }
    Ok(index.index_id)
}
