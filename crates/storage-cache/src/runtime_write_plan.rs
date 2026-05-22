use std::collections::HashMap;

use storage_types::{
    AttributeValue, BatchWriteItemEncodeRequest, BatchWriteItemRequest, KeyAttributes,
    KeySchemaElement, StorageError, StorageResult, StoredTableInfo, TableName, TransactEncodeItem,
    TransactWriteItem, WireItem,
};

#[derive(Debug, Clone)]
pub enum RuntimePointReadMutation {
    Put {
        table_name: TableName,
        key: KeyAttributes,
        item: Box<WireItem>,
    },
    Delete {
        table_name: TableName,
        key: KeyAttributes,
    },
    Invalidate {
        table_name: TableName,
        key: KeyAttributes,
    },
}

#[derive(Debug, Clone)]
pub enum RuntimeBaseWrite {
    Put {
        table_name: TableName,
        table_info: StoredTableInfo,
        item: HashMap<String, AttributeValue>,
    },
    Delete {
        table_name: TableName,
        table_info: StoredTableInfo,
        key: KeyAttributes,
    },
    InvalidateCoverage {
        table_name: TableName,
        table_info: StoredTableInfo,
        key: KeyAttributes,
    },
}

#[derive(Debug, Clone)]
pub struct RuntimeIndexTransition {
    pub table_name: TableName,
    pub table_info: StoredTableInfo,
    pub old_item: Option<HashMap<String, AttributeValue>>,
    pub new_item: Option<HashMap<String, AttributeValue>>,
}

#[derive(Debug, Clone)]
pub struct RuntimePreparedIndexPrewrite {
    pub table_info: StoredTableInfo,
    pub old_item: Option<HashMap<String, AttributeValue>>,
}

pub fn table_requires_index_tracking(table_info: &StoredTableInfo) -> bool {
    table_info
        .global_secondary_indexes
        .as_ref()
        .is_some_and(|indexes| !indexes.is_empty())
}

pub fn maybe_indexed_table_info(
    query_proof_enabled: bool,
    table_info: StoredTableInfo,
) -> Option<StoredTableInfo> {
    if query_proof_enabled && table_requires_index_tracking(&table_info) {
        Some(table_info)
    } else {
        None
    }
}

pub fn maybe_prepare_index_prewrite(
    query_proof_enabled: bool,
    table_info: StoredTableInfo,
    old_item: Option<HashMap<String, AttributeValue>>,
) -> Option<RuntimePreparedIndexPrewrite> {
    maybe_indexed_table_info(query_proof_enabled, table_info).map(|table_info| {
        RuntimePreparedIndexPrewrite {
            table_info,
            old_item,
        }
    })
}

#[derive(Debug, Clone)]
pub struct RuntimePreparedUpdateCacheWrite {
    pub table_name: TableName,
    pub table_info: StoredTableInfo,
    pub key: KeyAttributes,
    pub query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
}

#[derive(Debug, Clone)]
pub enum RuntimePendingIndexTransitionKind {
    Put {
        new_item: HashMap<String, AttributeValue>,
    },
    Update {
        key: KeyAttributes,
    },
    Delete,
}

#[derive(Debug, Clone)]
pub struct RuntimePendingIndexTransition {
    pub table_name: TableName,
    pub table_info: StoredTableInfo,
    pub old_item: Option<HashMap<String, AttributeValue>>,
    pub kind: RuntimePendingIndexTransitionKind,
}

impl RuntimePendingIndexTransition {
    pub fn update_lookup(&self) -> Option<(TableName, KeyAttributes)> {
        match &self.kind {
            RuntimePendingIndexTransitionKind::Update { key } => {
                Some((self.table_name.clone(), key.clone()))
            }
            _ => None,
        }
    }

    pub fn finalize(
        self,
        resolved_update_item: Option<HashMap<String, AttributeValue>>,
    ) -> RuntimeIndexTransition {
        let new_item = match self.kind {
            RuntimePendingIndexTransitionKind::Put { new_item } => Some(new_item),
            RuntimePendingIndexTransitionKind::Update { .. } => resolved_update_item,
            RuntimePendingIndexTransitionKind::Delete => None,
        };
        RuntimeIndexTransition {
            table_name: self.table_name,
            table_info: self.table_info,
            old_item: self.old_item,
            new_item,
        }
    }
}

pub fn collect_pending_index_transition_update_lookups(
    transitions: &[RuntimePendingIndexTransition],
) -> Vec<Option<(TableName, KeyAttributes)>> {
    transitions
        .iter()
        .map(RuntimePendingIndexTransition::update_lookup)
        .collect()
}

pub fn finalize_pending_index_transitions(
    transitions: Vec<RuntimePendingIndexTransition>,
    resolved_update_items: Vec<Option<HashMap<String, AttributeValue>>>,
) -> StorageResult<Vec<RuntimeIndexTransition>> {
    if transitions.len() != resolved_update_items.len() {
        return Err(StorageError::validation(
            "pending transition resolution count mismatch",
        ));
    }

    Ok(transitions
        .into_iter()
        .zip(resolved_update_items)
        .map(|(transition, new_item)| transition.finalize(new_item))
        .collect())
}

pub fn build_index_transition(
    table_name: &TableName,
    table_info: StoredTableInfo,
    old_item: Option<HashMap<String, AttributeValue>>,
    new_item: Option<HashMap<String, AttributeValue>>,
) -> RuntimeIndexTransition {
    RuntimeIndexTransition {
        table_name: table_name.clone(),
        table_info,
        old_item,
        new_item,
    }
}

pub fn build_pending_put_index_transition(
    table_name: &TableName,
    table_info: StoredTableInfo,
    old_item: Option<HashMap<String, AttributeValue>>,
    new_item: HashMap<String, AttributeValue>,
) -> RuntimePendingIndexTransition {
    RuntimePendingIndexTransition {
        table_name: table_name.clone(),
        table_info,
        old_item,
        kind: RuntimePendingIndexTransitionKind::Put { new_item },
    }
}

pub fn build_pending_update_index_transition(
    table_name: &TableName,
    table_info: StoredTableInfo,
    old_item: Option<HashMap<String, AttributeValue>>,
    key: KeyAttributes,
) -> RuntimePendingIndexTransition {
    RuntimePendingIndexTransition {
        table_name: table_name.clone(),
        table_info,
        old_item,
        kind: RuntimePendingIndexTransitionKind::Update { key },
    }
}

pub fn build_pending_delete_index_transition(
    table_name: &TableName,
    table_info: StoredTableInfo,
    old_item: Option<HashMap<String, AttributeValue>>,
) -> RuntimePendingIndexTransition {
    RuntimePendingIndexTransition {
        table_name: table_name.clone(),
        table_info,
        old_item,
        kind: RuntimePendingIndexTransitionKind::Delete,
    }
}

#[derive(Debug, Clone)]
pub enum RuntimeIndexTransitionTargetKind {
    Put {
        new_item: HashMap<String, AttributeValue>,
    },
    Delete,
}

#[derive(Debug, Clone)]
pub struct RuntimeIndexTransitionTarget {
    pub table_name: TableName,
    pub table_info: StoredTableInfo,
    pub old_item_lookup_key: KeyAttributes,
    pub kind: RuntimeIndexTransitionTargetKind,
}

impl RuntimeIndexTransitionTarget {
    pub fn build(
        self,
        old_item: Option<HashMap<String, AttributeValue>>,
    ) -> RuntimeIndexTransition {
        let new_item = match self.kind {
            RuntimeIndexTransitionTargetKind::Put { new_item } => Some(new_item),
            RuntimeIndexTransitionTargetKind::Delete => None,
        };
        build_index_transition(&self.table_name, self.table_info, old_item, new_item)
    }
}

#[derive(Debug, Clone)]
pub enum RuntimePendingIndexTransitionTargetKind {
    Put {
        new_item: HashMap<String, AttributeValue>,
    },
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct RuntimePendingIndexTransitionTarget {
    pub table_name: TableName,
    pub table_info: StoredTableInfo,
    pub old_item_lookup_key: KeyAttributes,
    pub kind: RuntimePendingIndexTransitionTargetKind,
}

impl RuntimePendingIndexTransitionTarget {
    pub fn build(
        self,
        old_item: Option<HashMap<String, AttributeValue>>,
    ) -> RuntimePendingIndexTransition {
        match self.kind {
            RuntimePendingIndexTransitionTargetKind::Put { new_item } => {
                build_pending_put_index_transition(
                    &self.table_name,
                    self.table_info,
                    old_item,
                    new_item,
                )
            }
            RuntimePendingIndexTransitionTargetKind::Update => {
                build_pending_update_index_transition(
                    &self.table_name,
                    self.table_info,
                    old_item,
                    self.old_item_lookup_key,
                )
            }
            RuntimePendingIndexTransitionTargetKind::Delete => {
                build_pending_delete_index_transition(&self.table_name, self.table_info, old_item)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum RuntimeQueryProofMutation {
    RecordBasePut {
        table_name: TableName,
        table_info: StoredTableInfo,
        item: HashMap<String, AttributeValue>,
    },
    RecordBaseDelete {
        table_name: TableName,
        table_info: StoredTableInfo,
        key: KeyAttributes,
    },
    InvalidateBaseCoverage {
        table_name: TableName,
        table_info: StoredTableInfo,
        key: KeyAttributes,
    },
    RecordIndexTransition {
        table_name: TableName,
        table_info: StoredTableInfo,
        old_item: Option<HashMap<String, AttributeValue>>,
        new_item: Option<HashMap<String, AttributeValue>>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeWriteEffects {
    pub point_read: Vec<RuntimePointReadMutation>,
    pub query_proof: Vec<RuntimeQueryProofMutation>,
}

pub fn extract_primary_key_from_item(
    key_schema: &[KeySchemaElement],
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<KeyAttributes> {
    let mut key = KeyAttributes::with_capacity(key_schema.len());
    for key_schema in key_schema {
        let value = item
            .get(&key_schema.attribute_name)
            .cloned()
            .ok_or_else(StorageError::invalid_or_missing_key)?;
        key.insert(key_schema.attribute_name.clone(), value);
    }
    Ok(key)
}

pub fn point_read_put_from_item(
    table_name: &TableName,
    key_schema: &[KeySchemaElement],
    item: &HashMap<String, AttributeValue>,
) -> StorageResult<RuntimePointReadMutation> {
    Ok(RuntimePointReadMutation::Put {
        table_name: table_name.clone(),
        key: extract_primary_key_from_item(key_schema, item)?,
        item: Box::new(WireItem::from_attribute_map(item)?),
    })
}

pub fn point_read_put_from_wire_item(
    table_name: &TableName,
    key: KeyAttributes,
    item: WireItem,
) -> RuntimePointReadMutation {
    RuntimePointReadMutation::Put {
        table_name: table_name.clone(),
        key,
        item: Box::new(item),
    }
}

pub fn point_read_delete(table_name: &TableName, key: &KeyAttributes) -> RuntimePointReadMutation {
    RuntimePointReadMutation::Delete {
        table_name: table_name.clone(),
        key: key.clone(),
    }
}

pub fn point_read_invalidate(
    table_name: &TableName,
    key: &KeyAttributes,
) -> RuntimePointReadMutation {
    RuntimePointReadMutation::Invalidate {
        table_name: table_name.clone(),
        key: key.clone(),
    }
}

pub fn build_put_item_cache_effects(
    table_name: &TableName,
    table_info: StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
    query_proof_enabled: bool,
) -> StorageResult<RuntimeWriteEffects> {
    let point_read_mutation = point_read_put_from_item(table_name, &table_info.key_schema, item)?;
    Ok(compose_put_item_effects(
        point_read_mutation,
        table_name,
        table_info,
        item,
        query_proof_prewrite,
        query_proof_enabled,
    ))
}

pub fn build_delete_item_cache_effects(
    table_name: &TableName,
    table_info: StoredTableInfo,
    key: &KeyAttributes,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
    query_proof_enabled: bool,
) -> RuntimeWriteEffects {
    compose_delete_item_effects(
        point_read_delete(table_name, key),
        table_name,
        table_info,
        key,
        query_proof_prewrite,
        query_proof_enabled,
    )
}

pub fn prepare_update_cache_write(
    table_name: &TableName,
    table_info: StoredTableInfo,
    key: &KeyAttributes,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
) -> RuntimePreparedUpdateCacheWrite {
    RuntimePreparedUpdateCacheWrite {
        table_name: table_name.clone(),
        table_info,
        key: key.clone(),
        query_proof_prewrite,
    }
}

pub fn finalize_update_cache_effects(
    prepared: RuntimePreparedUpdateCacheWrite,
    post_image: Option<WireItem>,
    query_proof_enabled: bool,
) -> StorageResult<RuntimeWriteEffects> {
    let query_proof_new_item_map = post_image
        .as_ref()
        .map(WireItem::to_attribute_map)
        .transpose()?;
    let point_read_mutation = match post_image {
        Some(item) => RuntimePointReadMutation::Put {
            table_name: prepared.table_name.clone(),
            key: prepared.key.clone(),
            item: Box::new(item),
        },
        None => point_read_invalidate(&prepared.table_name, &prepared.key),
    };
    Ok(compose_update_item_effects(
        point_read_mutation,
        &prepared.table_name,
        prepared.table_info,
        &prepared.key,
        prepared.query_proof_prewrite,
        query_proof_new_item_map,
        query_proof_enabled,
    ))
}

pub fn compose_put_item_effects(
    point_read_mutation: RuntimePointReadMutation,
    table_name: &TableName,
    table_info: StoredTableInfo,
    item: &HashMap<String, AttributeValue>,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
    query_proof_enabled: bool,
) -> RuntimeWriteEffects {
    compose_write_effects(
        vec![point_read_mutation],
        vec![RuntimeBaseWrite::Put {
            table_name: table_name.clone(),
            table_info,
            item: item.clone(),
        }],
        query_proof_prewrite
            .into_iter()
            .map(|query_proof_prewrite| RuntimeIndexTransition {
                table_name: table_name.clone(),
                table_info: query_proof_prewrite.table_info,
                old_item: query_proof_prewrite.old_item,
                new_item: Some(item.clone()),
            })
            .collect(),
        query_proof_enabled,
    )
}

pub fn compose_delete_item_effects(
    point_read_mutation: RuntimePointReadMutation,
    table_name: &TableName,
    table_info: StoredTableInfo,
    key: &KeyAttributes,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
    query_proof_enabled: bool,
) -> RuntimeWriteEffects {
    compose_write_effects(
        vec![point_read_mutation],
        vec![RuntimeBaseWrite::Delete {
            table_name: table_name.clone(),
            table_info,
            key: key.clone(),
        }],
        query_proof_prewrite
            .into_iter()
            .map(|query_proof_prewrite| RuntimeIndexTransition {
                table_name: table_name.clone(),
                table_info: query_proof_prewrite.table_info,
                old_item: query_proof_prewrite.old_item,
                new_item: None,
            })
            .collect(),
        query_proof_enabled,
    )
}

pub fn compose_update_item_effects(
    point_read_mutation: RuntimePointReadMutation,
    table_name: &TableName,
    table_info: StoredTableInfo,
    key: &KeyAttributes,
    query_proof_prewrite: Option<RuntimePreparedIndexPrewrite>,
    new_item: Option<HashMap<String, AttributeValue>>,
    query_proof_enabled: bool,
) -> RuntimeWriteEffects {
    compose_write_effects(
        vec![point_read_mutation],
        vec![RuntimeBaseWrite::InvalidateCoverage {
            table_name: table_name.clone(),
            table_info,
            key: key.clone(),
        }],
        query_proof_prewrite
            .into_iter()
            .map(|query_proof_prewrite| RuntimeIndexTransition {
                table_name: table_name.clone(),
                table_info: query_proof_prewrite.table_info,
                old_item: query_proof_prewrite.old_item,
                new_item: new_item.clone(),
            })
            .collect(),
        query_proof_enabled,
    )
}

pub fn collect_base_writes_for_batch_write(
    request: &BatchWriteItemRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> Vec<RuntimeBaseWrite> {
    let mut base_writes = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name).cloned() else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                base_writes.push(RuntimeBaseWrite::Put {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    item: put_request.item.clone(),
                });
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                base_writes.push(RuntimeBaseWrite::Delete {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    key: delete_request.key.clone(),
                });
            }
        }
    }
    base_writes
}

pub fn collect_point_read_mutations_for_batch_write(
    request: &BatchWriteItemRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePointReadMutation>> {
    let mut mutations = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name) else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                mutations.push(point_read_put_from_item(
                    table_name,
                    &table_info.key_schema,
                    &put_request.item,
                )?);
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                mutations.push(point_read_delete(table_name, &delete_request.key));
            }
        }
    }
    Ok(mutations)
}

pub fn collect_query_proof_targets_for_batch_write(
    request: &BatchWriteItemRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimeIndexTransitionTarget>> {
    let mut targets = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name).cloned() else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                targets.push(RuntimeIndexTransitionTarget {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    old_item_lookup_key: extract_primary_key_from_item(
                        &table_info.key_schema,
                        &put_request.item,
                    )?,
                    kind: RuntimeIndexTransitionTargetKind::Put {
                        new_item: put_request.item.clone(),
                    },
                });
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                targets.push(RuntimeIndexTransitionTarget {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    old_item_lookup_key: delete_request.key.clone(),
                    kind: RuntimeIndexTransitionTargetKind::Delete,
                });
            }
        }
    }
    Ok(targets)
}

pub fn collect_base_writes_for_batch_write_encode(
    request: &BatchWriteItemEncodeRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimeBaseWrite>> {
    let mut base_writes = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name).cloned() else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                base_writes.push(RuntimeBaseWrite::Put {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    item: put_request.item.to_attribute_map()?,
                });
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                base_writes.push(RuntimeBaseWrite::Delete {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    key: delete_request.key.clone(),
                });
            }
        }
    }
    Ok(base_writes)
}

pub fn collect_point_read_mutations_for_batch_write_encode(
    request: &BatchWriteItemEncodeRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePointReadMutation>> {
    let mut mutations = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name) else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                let item = put_request.item.to_attribute_map()?;
                mutations.push(point_read_put_from_wire_item(
                    table_name,
                    extract_primary_key_from_item(&table_info.key_schema, &item)?,
                    put_request.item.clone(),
                ));
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                mutations.push(point_read_delete(table_name, &delete_request.key));
            }
        }
    }
    Ok(mutations)
}

pub fn collect_query_proof_targets_for_batch_write_encode(
    request: &BatchWriteItemEncodeRequest,
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimeIndexTransitionTarget>> {
    let mut targets = Vec::new();
    for (table_name, write_requests) in &request.request_items {
        let Some(table_info) = table_infos.get(table_name).cloned() else {
            continue;
        };
        for write_request in write_requests {
            if let Some(put_request) = write_request.put_request.as_ref() {
                let new_item = put_request.item.to_attribute_map()?;
                targets.push(RuntimeIndexTransitionTarget {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    old_item_lookup_key: extract_primary_key_from_item(
                        &table_info.key_schema,
                        &new_item,
                    )?,
                    kind: RuntimeIndexTransitionTargetKind::Put { new_item },
                });
            }
            if let Some(delete_request) = write_request.delete_request.as_ref() {
                targets.push(RuntimeIndexTransitionTarget {
                    table_name: table_name.clone(),
                    table_info: table_info.clone(),
                    old_item_lookup_key: delete_request.key.clone(),
                    kind: RuntimeIndexTransitionTargetKind::Delete,
                });
            }
        }
    }
    Ok(targets)
}

pub fn collect_base_writes_for_transact_write_items(
    transact_items: &[TransactWriteItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> Vec<RuntimeBaseWrite> {
    let mut base_writes = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::Put {
                table_name: put.table_name.clone(),
                table_info,
                item: put.item.clone(),
            });
        }
        if let Some(update) = item.update.as_ref()
            && let Some(table_info) = table_infos.get(&update.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::InvalidateCoverage {
                table_name: update.table_name.clone(),
                table_info,
                key: update.key.clone(),
            });
        }
        if let Some(delete) = item.delete.as_ref()
            && let Some(table_info) = table_infos.get(&delete.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::Delete {
                table_name: delete.table_name.clone(),
                table_info,
                key: delete.key.clone(),
            });
        }
    }
    base_writes
}

pub fn collect_transact_write_table_names(transact_items: &[TransactWriteItem]) -> Vec<TableName> {
    let mut table_names = Vec::new();
    for item in transact_items {
        for table_name in [
            item.put.as_ref().map(|put| &put.table_name),
            item.update.as_ref().map(|update| &update.table_name),
            item.delete.as_ref().map(|delete| &delete.table_name),
        ]
        .into_iter()
        .flatten()
        {
            push_unique_table_name(&mut table_names, table_name);
        }
    }
    table_names
}

pub fn collect_point_read_mutations_for_transact_write_items(
    transact_items: &[TransactWriteItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePointReadMutation>> {
    let mut mutations = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name)
        {
            mutations.push(point_read_put_from_item(
                &put.table_name,
                &table_info.key_schema,
                &put.item,
            )?);
        }
        if let Some(update) = item.update.as_ref() {
            mutations.push(point_read_invalidate(&update.table_name, &update.key));
        }
        if let Some(delete) = item.delete.as_ref() {
            mutations.push(point_read_delete(&delete.table_name, &delete.key));
        }
    }
    Ok(mutations)
}

pub fn collect_pending_query_proof_targets_for_transact_write_items(
    transact_items: &[TransactWriteItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePendingIndexTransitionTarget>> {
    let mut targets = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name).cloned()
        {
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: put.table_name.clone(),
                table_info: table_info.clone(),
                old_item_lookup_key: extract_primary_key_from_item(
                    &table_info.key_schema,
                    &put.item,
                )?,
                kind: RuntimePendingIndexTransitionTargetKind::Put {
                    new_item: put.item.clone(),
                },
            });
        }
        if let Some(update) = item.update.as_ref()
            && let Some(table_info) = table_infos.get(&update.table_name).cloned()
        {
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: update.table_name.clone(),
                table_info,
                old_item_lookup_key: update.key.clone(),
                kind: RuntimePendingIndexTransitionTargetKind::Update,
            });
        }
        if let Some(delete) = item.delete.as_ref()
            && let Some(table_info) = table_infos.get(&delete.table_name).cloned()
        {
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: delete.table_name.clone(),
                table_info,
                old_item_lookup_key: delete.key.clone(),
                kind: RuntimePendingIndexTransitionTargetKind::Delete,
            });
        }
    }
    Ok(targets)
}

pub fn collect_base_writes_for_transact_write_items_encode(
    transact_items: &[TransactEncodeItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimeBaseWrite>> {
    let mut base_writes = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::Put {
                table_name: put.table_name.clone(),
                table_info,
                item: put.item.to_attribute_map()?,
            });
        }
        if let Some(update) = item.update.as_ref()
            && let Some(table_info) = table_infos.get(&update.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::InvalidateCoverage {
                table_name: update.table_name.clone(),
                table_info,
                key: update.key.clone(),
            });
        }
        if let Some(delete) = item.delete.as_ref()
            && let Some(table_info) = table_infos.get(&delete.table_name).cloned()
        {
            base_writes.push(RuntimeBaseWrite::Delete {
                table_name: delete.table_name.clone(),
                table_info,
                key: delete.key.clone(),
            });
        }
    }
    Ok(base_writes)
}

pub fn collect_transact_write_encode_table_names(
    transact_items: &[TransactEncodeItem],
) -> Vec<TableName> {
    let mut table_names = Vec::new();
    for item in transact_items {
        for table_name in [
            item.put.as_ref().map(|put| &put.table_name),
            item.update.as_ref().map(|update| &update.table_name),
            item.delete.as_ref().map(|delete| &delete.table_name),
        ]
        .into_iter()
        .flatten()
        {
            push_unique_table_name(&mut table_names, table_name);
        }
    }
    table_names
}

pub fn collect_point_read_mutations_for_transact_write_items_encode(
    transact_items: &[TransactEncodeItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePointReadMutation>> {
    let mut mutations = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name)
        {
            let item_map = put.item.to_attribute_map()?;
            mutations.push(point_read_put_from_wire_item(
                &put.table_name,
                extract_primary_key_from_item(&table_info.key_schema, &item_map)?,
                put.item.clone(),
            ));
        }
        if let Some(update) = item.update.as_ref() {
            mutations.push(point_read_invalidate(&update.table_name, &update.key));
        }
        if let Some(delete) = item.delete.as_ref() {
            mutations.push(point_read_delete(&delete.table_name, &delete.key));
        }
    }
    Ok(mutations)
}

pub fn collect_pending_query_proof_targets_for_transact_write_items_encode(
    transact_items: &[TransactEncodeItem],
    table_infos: &HashMap<TableName, StoredTableInfo>,
) -> StorageResult<Vec<RuntimePendingIndexTransitionTarget>> {
    let mut targets = Vec::new();
    for item in transact_items {
        if let Some(put) = item.put.as_ref()
            && let Some(table_info) = table_infos.get(&put.table_name).cloned()
        {
            let new_item = put.item.to_attribute_map()?;
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: put.table_name.clone(),
                table_info: table_info.clone(),
                old_item_lookup_key: extract_primary_key_from_item(
                    &table_info.key_schema,
                    &new_item,
                )?,
                kind: RuntimePendingIndexTransitionTargetKind::Put { new_item },
            });
        }
        if let Some(update) = item.update.as_ref()
            && let Some(table_info) = table_infos.get(&update.table_name).cloned()
        {
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: update.table_name.clone(),
                table_info,
                old_item_lookup_key: update.key.clone(),
                kind: RuntimePendingIndexTransitionTargetKind::Update,
            });
        }
        if let Some(delete) = item.delete.as_ref()
            && let Some(table_info) = table_infos.get(&delete.table_name).cloned()
        {
            targets.push(RuntimePendingIndexTransitionTarget {
                table_name: delete.table_name.clone(),
                table_info,
                old_item_lookup_key: delete.key.clone(),
                kind: RuntimePendingIndexTransitionTargetKind::Delete,
            });
        }
    }
    Ok(targets)
}

pub fn compose_write_effects(
    point_read: Vec<RuntimePointReadMutation>,
    base_writes: Vec<RuntimeBaseWrite>,
    index_transitions: Vec<RuntimeIndexTransition>,
    query_proof_enabled: bool,
) -> RuntimeWriteEffects {
    let mut effects = RuntimeWriteEffects {
        point_read,
        query_proof: Vec::new(),
    };
    if !query_proof_enabled {
        return effects;
    }

    effects.query_proof.extend(
        base_writes
            .into_iter()
            .map(runtime_query_proof_mutation_from_base_write),
    );
    effects.query_proof.extend(
        index_transitions
            .into_iter()
            .map(runtime_query_proof_mutation_from_index_transition),
    );
    effects
}

fn runtime_query_proof_mutation_from_base_write(
    base_write: RuntimeBaseWrite,
) -> RuntimeQueryProofMutation {
    match base_write {
        RuntimeBaseWrite::Put {
            table_name,
            table_info,
            item,
        } => RuntimeQueryProofMutation::RecordBasePut {
            table_name,
            table_info,
            item,
        },
        RuntimeBaseWrite::Delete {
            table_name,
            table_info,
            key,
        } => RuntimeQueryProofMutation::RecordBaseDelete {
            table_name,
            table_info,
            key,
        },
        RuntimeBaseWrite::InvalidateCoverage {
            table_name,
            table_info,
            key,
        } => RuntimeQueryProofMutation::InvalidateBaseCoverage {
            table_name,
            table_info,
            key,
        },
    }
}

fn runtime_query_proof_mutation_from_index_transition(
    transition: RuntimeIndexTransition,
) -> RuntimeQueryProofMutation {
    RuntimeQueryProofMutation::RecordIndexTransition {
        table_name: transition.table_name,
        table_info: transition.table_info,
        old_item: transition.old_item,
        new_item: transition.new_item,
    }
}

fn push_unique_table_name(table_names: &mut Vec<TableName>, table_name: &TableName) {
    if !table_names.iter().any(|existing| existing == table_name) {
        table_names.push(table_name.clone());
    }
}
