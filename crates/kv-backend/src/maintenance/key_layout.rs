//! KV keys for shared maintenance task authority.

pub(super) fn progress_key(kind: storage_types::maintenance::MaintenanceKind) -> Vec<u8> {
    let mut key = crate::constants::MAINTENANCE_PROGRESS_PREFIX.to_vec();
    key.push(kind.storage_tag());
    key
}
