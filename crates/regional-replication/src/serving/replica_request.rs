use storage_types::{StorageError, StorageResult, UpdateTableRequest};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplicaActionKind {
    Create,
    Delete,
}

pub(crate) fn validate_replica_request(
    request: &UpdateTableRequest,
) -> StorageResult<ReplicaActionKind> {
    require_separate_metadata(request)?;
    if request.aux_configuration_primary_region.is_some() {
        return Err(StorageError::validation(
            "replica changes and primary transfer require separate controls",
        ));
    }
    let updates = request
        .replica_updates
        .as_ref()
        .ok_or_else(|| StorageError::validation("ReplicaUpdates is required"))?;
    if updates.is_empty() {
        return Err(StorageError::validation("ReplicaUpdates must not be empty"));
    }
    let mut selected = None;
    for update in updates {
        if usize::from(update.create.is_some())
            + usize::from(update.update.is_some())
            + usize::from(update.delete.is_some())
            != 1
        {
            return Err(StorageError::validation(
                "ReplicaUpdates requires exactly one action per entry",
            ));
        }
        let action = if update.create.is_some() {
            ReplicaActionKind::Create
        } else if update.delete.is_some() {
            ReplicaActionKind::Delete
        } else {
            return Err(StorageError::unsupported(
                "replica update requires its regional lifecycle transition",
            ));
        };
        if selected.is_some_and(|selected| selected != action) {
            return Err(StorageError::validation(
                "replica creation and deletion require separate controls",
            ));
        }
        selected = Some(action);
    }
    selected.ok_or_else(|| StorageError::validation("ReplicaUpdates must not be empty"))
}

pub(crate) fn require_separate_metadata(request: &UpdateTableRequest) -> StorageResult<()> {
    if request.max_indexers.is_some()
        || request.deletion_protection_enabled.is_some()
        || request.global_secondary_index_updates.is_some()
        || request.stream_specification.is_some()
        || request.aux_stream_duration_hours.is_some()
        || request.aux_default_item_stream_duration_hours.is_some()
    {
        return Err(StorageError::validation(
            "regional lifecycle controls cannot be combined with table metadata changes",
        ));
    }
    Ok(())
}
