//! Provider-neutral ordinary BatchGetItem plan (phase 6, §4).
//!
//! `BatchGetPlan` moves each nonempty table entry of the request into one
//! `BatchGetTablePlan` in the request map's encountered order, without a
//! second key/member map. Per-table binding maps metadata outcomes to
//! fatal/unprocessed/ready exactly once; `finish_read` preserves request-key
//! order and missing slots; `finish` consumes the plan and builds the
//! existing final response/unprocessed maps with the original per-table
//! options and no cloned request.

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    sync::Arc,
};

use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetItemResponse, BatchGetWireItemResponse,
    BatchProofSelection, DurableBatchPointReadProof, KeysAndAttributes, StorageEnum, StorageError,
    StorageResult, StoredTableInfo, TableName, VecMap, WireItem, canonical_dynamodb_number,
    project_attribute_map, validate_transact_key,
};

use super::table::PointReadTable;
use crate::constants::MAX_BATCH_GET_RESPONSE_ITEM_BYTES;

/// Owned ordinary BatchGetItem request: one table plan per nonempty request
/// entry, in encountered order.
pub struct BatchGetPlan {
    pub tables: Vec<BatchGetTablePlan>,
}

impl From<BatchGetItemRequest> for BatchGetPlan {
    fn from(request: BatchGetItemRequest) -> Self {
        let mut tables = Vec::with_capacity(request.request_items.len());
        for (table_name, keys_and_attributes) in request.request_items {
            if keys_and_attributes.keys.is_empty() {
                continue;
            }
            tables.push(BatchGetTablePlan {
                table_name,
                keys: keys_and_attributes,
            });
        }
        Self { tables }
    }
}

impl BatchGetPlan {
    /// Complete the local Dynamo operation after one coherent native read
    /// attempt. The API response includes empty requested table groups.
    pub fn finish_api_response(
        self,
        tables: Vec<BatchGetTableResult>,
    ) -> StorageResult<BatchGetItemResponse> {
        if tables.len() != self.tables.len() {
            return Err(StorageError::internal(
                "batch-get table result count must match requested tables",
            ));
        }
        let mut responses = storage_types::VecMap::with_capacity(tables.len());
        let mut unprocessed_keys = storage_types::VecMap::new();
        let mut remaining_bytes = MAX_BATCH_GET_RESPONSE_ITEM_BYTES;
        for (plan, result) in self.tables.into_iter().zip(tables) {
            match result {
                BatchGetTableResult::Items(items) => {
                    let (projected, deferred) =
                        plan.project_items_with_budget(items, &mut remaining_bytes)?;
                    responses.insert(plan.table_name.clone(), projected);
                    if let Some(deferred) = deferred {
                        unprocessed_keys.insert(plan.table_name, deferred);
                    }
                }
                BatchGetTableResult::Unprocessed => {
                    unprocessed_keys.insert(plan.table_name, plan.keys);
                }
            }
        }
        Ok(BatchGetItemResponse {
            responses: Some(responses),
            unprocessed_keys: Some(unprocessed_keys),
            consumed_capacity: None,
        })
    }

    /// Build the final wire response from exactly one table result per
    /// [`BatchGetPlan::tables`] entry, in that order. Missing items are
    /// omitted; empty provider response groups are omitted; unprocessed
    /// tables keep their original keys and options.
    /// Finish one provider execution while keeping proof association at the
    /// shared response boundary.  Providers must use this method whenever
    /// they return durable proof material: a caller cannot publish a response
    /// or warm its cache until every proof has been checked against the exact
    /// request-key slots and unprocessed table outcome produced by this
    /// attempt.
    pub fn finish_with_proofs(
        self,
        tables: Vec<BatchGetTableResult>,
        mut proofs: DurableBatchPointReadProof,
        selection: BatchProofSelection,
    ) -> StorageResult<storage_types::BatchGetExecutionResult> {
        if tables.len() != self.tables.len() {
            return Err(StorageError::internal(
                "batch-get table result count must match requested tables",
            ));
        }
        let mut accepted = Vec::with_capacity(self.tables.len());
        for (plan, result) in self.tables.into_iter().zip(tables) {
            match result {
                BatchGetTableResult::Items(items) => {
                    let proof = proofs.responses.remove(&plan.table_name);
                    accepted.push(storage_types::BatchGetExecutionTable::items(
                        plan.table_name,
                        plan.keys,
                        items,
                        proof,
                    ))
                }
                BatchGetTableResult::Unprocessed => {
                    let proof = proofs.unprocessed_keys.remove(&plan.table_name);
                    accepted.push(storage_types::BatchGetExecutionTable::unprocessed(
                        plan.table_name,
                        plan.keys,
                        proof,
                    ))
                }
            }
        }
        if !proofs.responses.is_empty() || !proofs.unprocessed_keys.is_empty() {
            return Err(StorageError::internal(
                "batch-get proof contains an unknown table",
            ));
        }
        storage_types::BatchGetExecutionResult::from_tables(accepted, selection)
    }

    pub fn finish(
        self,
        tables: Vec<BatchGetTableResult>,
    ) -> StorageResult<BatchGetWireItemResponse> {
        if tables.len() != self.tables.len() {
            return Err(StorageError::internal(
                "batch-get table result count must match requested tables",
            ));
        }
        let mut responses = VecMap::with_capacity(tables.len());
        let mut unprocessed_keys = VecMap::with_capacity(tables.len());
        for (plan, result) in self.tables.into_iter().zip(tables) {
            plan.finish_table(result, &mut responses, &mut unprocessed_keys);
        }
        Ok(BatchGetWireItemResponse {
            responses: (!responses.is_empty()).then_some(responses),
            unprocessed_keys: (!unprocessed_keys.is_empty()).then_some(unprocessed_keys),
            consumed_capacity: None,
        })
    }
}

/// Owned BatchGet keys for one table: table identity plus the original
/// `KeysAndAttributes`, including its projection and capacity options.
pub struct BatchGetTablePlan {
    pub table_name: TableName,
    pub keys: KeysAndAttributes,
}

impl BatchGetTablePlan {
    fn project_items_with_budget(
        &self,
        items: Vec<Option<WireItem>>,
        remaining_bytes: &mut usize,
    ) -> StorageResult<(Vec<WireItem>, Option<KeysAndAttributes>)> {
        let mut projected = Vec::with_capacity(items.len());
        let mut deferred = Vec::new();
        for (key, item) in self.keys.keys.iter().zip(items) {
            let Some(item) = item else { continue };
            let bytes = item.payload_len();
            if bytes > *remaining_bytes {
                deferred.push(key.clone());
                continue;
            }
            *remaining_bytes -= bytes;
            let item = if self.keys.projection_expression.is_some()
                || self.keys.attributes_to_get.is_some()
            {
                WireItem::try_from(project_attribute_map(
                    item.try_into()?,
                    self.keys.projection_expression.as_deref(),
                    self.keys.attributes_to_get.as_deref(),
                    self.keys.expression_attribute_names.as_ref(),
                ))?
            } else {
                item
            };
            projected.push(item);
        }
        let deferred = (!deferred.is_empty()).then(|| {
            let mut request = self.keys.clone();
            request.keys = deferred;
            request
        });
        Ok((projected, deferred))
    }

    /// Bind one metadata outcome: `TableNotFound` fails the whole request,
    /// any other metadata/access error or an empty key schema leaves the
    /// whole table unprocessed, otherwise the table is ready to read.
    pub fn bind(
        &self,
        metadata: StorageResult<Arc<StoredTableInfo>>,
    ) -> StorageResult<Option<PointReadTable<'_>>> {
        match metadata {
            Err(error) if matches!(error.as_ref(), StorageEnum::TableNotFound { .. }) => Err(error),
            Err(_) => Ok(None),
            Ok(table) => self.bind_table(table),
        }
    }

    /// Fold one table read outcome: a physical read error discards that
    /// table's partial items into unprocessed form; a successful vector must
    /// carry exactly one slot per requested key, preserving missing slots
    /// until association is checked by [`BatchGetPlan::finish`].
    pub fn finish_read(
        &self,
        rows: StorageResult<Vec<Option<WireItem>>>,
    ) -> StorageResult<BatchGetTableResult> {
        match rows {
            Err(_) => Ok(BatchGetTableResult::Unprocessed),
            Ok(items) => {
                if items.len() != self.keys.keys.len() {
                    return Err(StorageError::internal(
                        "batch-get observation count must match requested keys",
                    ));
                }
                Ok(BatchGetTableResult::Items(items))
            }
        }
    }

    fn bind_table(&self, table: Arc<StoredTableInfo>) -> StorageResult<Option<PointReadTable<'_>>> {
        if table.table_name != self.table_name {
            return Err(StorageError::internal(
                "point-read metadata identity must match the requested table",
            ));
        }
        if table.key_schema.is_empty() {
            return Ok(None);
        }
        for key in &self.keys.keys {
            if key.len() != table.key_schema.len() {
                return Err(StorageError::validation(
                    "The provided key element does not match the schema",
                ));
            }
            validate_transact_key(&table, key)?;
        }
        let mut seen = HashSet::with_capacity(self.keys.keys.len());
        for key in &self.keys.keys {
            if !seen.insert(BatchGetKeyIdentity {
                key_schema: &table.key_schema,
                key,
            }) {
                return Err(StorageError::validation(
                    "Provided list of item keys contains duplicates",
                ));
            }
        }
        Ok(Some(PointReadTable::bound(table, &self.keys.keys)))
    }

    fn finish_table(
        self,
        result: BatchGetTableResult,
        responses: &mut VecMap<TableName, Vec<WireItem>>,
        unprocessed_keys: &mut VecMap<TableName, KeysAndAttributes>,
    ) {
        match result {
            BatchGetTableResult::Items(items) => {
                let present = items.into_iter().flatten().collect::<Vec<_>>();
                if !present.is_empty() {
                    responses.insert(self.table_name, present);
                }
            }
            BatchGetTableResult::Unprocessed => {
                unprocessed_keys.insert(self.table_name, self.keys);
            }
        }
    }
}

/// One table's read outcome in request-key order. `Items` preserves missing
/// slots (`None`) until the shared `finish` removes them.
#[derive(Debug)]
pub enum BatchGetTableResult {
    Items(Vec<Option<WireItem>>),
    Unprocessed,
}

/// Schema-bound identity used while binding one table's request keys. Numeric
/// key values compare by their DynamoDB canonical number, so `1` and `1.0`
/// cannot enter one native read as duplicate slots.
pub struct BatchGetKeyIdentity<'a> {
    key_schema: &'a [storage_types::KeySchemaElement],
    key: &'a storage_types::KeyAttributes,
}

impl<'a> BatchGetKeyIdentity<'a> {
    #[must_use]
    pub fn new(
        key_schema: &'a [storage_types::KeySchemaElement],
        key: &'a storage_types::KeyAttributes,
    ) -> Self {
        Self { key_schema, key }
    }
}

impl PartialEq for BatchGetKeyIdentity<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key_schema.len() == other.key_schema.len()
            && self
                .key_schema
                .iter()
                .zip(other.key_schema)
                .all(|(left_schema, right_schema)| {
                    left_schema.attribute_name == right_schema.attribute_name
                        && match (
                            self.key.get(&left_schema.attribute_name),
                            other.key.get(&right_schema.attribute_name),
                        ) {
                            (Some(left), Some(right)) => canonical_key_value_eq(left, right),
                            (None, None) => true,
                            _ => false,
                        }
                })
    }
}

impl Eq for BatchGetKeyIdentity<'_> {}

impl Hash for BatchGetKeyIdentity<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key_schema.len().hash(state);
        for schema in self.key_schema {
            schema.attribute_name.hash(state);
            if let Some(value) = self.key.get(&schema.attribute_name) {
                hash_canonical_key_value(value, state);
            }
        }
    }
}

fn canonical_key_value_eq(left: &AttributeValue, right: &AttributeValue) -> bool {
    match (left, right) {
        (AttributeValue::N(left), AttributeValue::N(right)) => match (
            canonical_dynamodb_number(left),
            canonical_dynamodb_number(right),
        ) {
            (Ok(left), Ok(right)) => left == right,
            _ => left == right,
        },
        _ => left == right,
    }
}

fn hash_canonical_key_value<H: Hasher>(value: &AttributeValue, state: &mut H) {
    match value {
        AttributeValue::S(value) => {
            0_u8.hash(state);
            value.hash(state);
        }
        AttributeValue::N(value) => match canonical_dynamodb_number(value) {
            Ok(value) => {
                1_u8.hash(state);
                value.hash(state);
            }
            Err(_) => {
                4_u8.hash(state);
                value.hash(state);
            }
        },
        AttributeValue::B(value) => {
            2_u8.hash(state);
            value.hash(state);
        }
        value => {
            3_u8.hash(state);
            std::mem::discriminant(value).hash(state);
        }
    }
}
