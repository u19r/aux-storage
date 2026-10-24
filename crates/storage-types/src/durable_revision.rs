use std::collections::HashMap;

use serde::Serialize;

use crate::{
    AllOld, AttributeMap, AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse,
    KeyAttributes, KeysAndAttributes, StorageError, StorageResult, TableName,
    TransactWriteItemsRequest, UpdateItemRequest, WireItem, canonical_dynamodb_number,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DurableItemRevision(Vec<u8>);

impl DurableItemRevision {
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DurableAbsenceProof(Vec<u8>);

impl DurableAbsenceProof {
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum DurablePointReadProof {
    Present {
        item: Box<WireItem>,
        indexers: Vec<String>,
        revision: DurableItemRevision,
    },
    Absent {
        proof: DurableAbsenceProof,
    },
}

#[derive(Debug, Clone)]
pub enum DurablePointReadGuard {
    Present(DurableItemRevision),
    Absent(DurableAbsenceProof),
}

#[derive(Debug, Clone)]
pub struct GuardedPutItemRequest {
    pub table_name: TableName,
    // Legacy guarded writes still use the old SQL/KV provider contract. This
    // proof path is removed at the Dynamo cutover rather than migrated again.
    pub item: HashMap<String, AttributeValue>,
    pub indexers: Vec<String>,
    pub guard: DurablePointReadGuard,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,
    pub expression_attribute_values: Option<AttributeMap>,
    pub return_values: Option<AllOld>,
}

#[derive(Debug, Clone)]
pub struct GuardedDeleteItemRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub guard: DurablePointReadGuard,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<crate::VecMap<String, String>>,
    pub expression_attribute_values: Option<AttributeMap>,
}

#[derive(Debug, Clone)]
pub struct GuardedUpdateItemRequest {
    pub request: UpdateItemRequest,
    pub guard: DurablePointReadGuard,
}

#[derive(Debug, Clone)]
pub struct DurableTransactWriteGuard {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub guard: DurablePointReadGuard,
}

#[derive(Debug, Clone)]
pub struct GuardedTransactWriteItemsRequest {
    pub request: TransactWriteItemsRequest,
    pub guards: Vec<DurableTransactWriteGuard>,
}

#[derive(Debug, Clone)]
pub struct DurablePointReadRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub consistent_read: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct DurableBatchPointReadProofEntry {
    pub key: KeyAttributes,
    pub proof: DurablePointReadProof,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct DurableBatchPointReadProof {
    // Legacy proof producers construct these maps directly; the proof contract
    // is deleted with the point-read cache path at the Dynamo cutover.
    pub responses: HashMap<TableName, Vec<DurableBatchPointReadProofEntry>>,
    pub unprocessed_keys: HashMap<TableName, KeysAndAttributes>,
}

/// Selects the evidence a native BatchGet execution must retain from its
/// one attempt snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BatchProofSelection {
    /// Return item data only.
    #[default]
    None,
    /// Return one durable proof for every requested key.
    ConsistentTables,
}

/// One table's request-order BatchGet observations at the proof boundary.
/// Missing slots stay aligned until the result owner performs the single
/// association check and shapes the public response.
#[derive(Debug)]
pub struct BatchGetExecutionTable {
    pub table_name: TableName,
    pub keys: KeysAndAttributes,
    pub outcome: BatchGetExecutionTableOutcome,
}

#[derive(Debug)]
pub enum BatchGetExecutionTableOutcome {
    Items {
        items: Vec<Option<WireItem>>,
        proofs: Option<Vec<DurableBatchPointReadProofEntry>>,
    },
    Unprocessed {
        proof: Option<Box<KeysAndAttributes>>,
    },
}

impl BatchGetExecutionTable {
    #[must_use]
    pub fn items(
        table_name: TableName,
        keys: KeysAndAttributes,
        items: Vec<Option<WireItem>>,
        proofs: Option<Vec<DurableBatchPointReadProofEntry>>,
    ) -> Self {
        Self {
            table_name,
            keys,
            outcome: BatchGetExecutionTableOutcome::Items { items, proofs },
        }
    }

    #[must_use]
    pub fn unprocessed(
        table_name: TableName,
        keys: KeysAndAttributes,
        proof: Option<KeysAndAttributes>,
    ) -> Self {
        Self {
            table_name,
            keys,
            outcome: BatchGetExecutionTableOutcome::Unprocessed {
                proof: proof.map(Box::new),
            },
        }
    }
}

/// Shared BatchGet execution request. The request owns the wire shape and
/// the evidence choice so providers do not infer proof work from cache state.
#[derive(Debug, Clone)]
pub struct BatchGetExecutionRequest {
    pub request: BatchGetItemRequest,
    pub proof_selection: BatchProofSelection,
}

/// Shared BatchGet execution result. Item data and optional evidence come
/// from the same provider attempt and snapshot.
///
/// The fields stay private so callers cannot publish provider-supplied proof
/// material without going through the shared BatchGet plan's association
/// gate. Providers that do not implement the execution path use
/// [`Self::without_proofs`]; the proof-bearing constructor is used only by
/// `BatchGetPlan::finish_with_proofs` after it has validated every ordinal
/// slot and unprocessed outcome.
#[derive(Debug, Clone, Serialize)]
pub struct BatchGetExecutionResult {
    response: BatchGetWireItemResponse,
    proofs: DurableBatchPointReadProof,
}

impl BatchGetExecutionResult {
    /// Build a result for an execution that did not request durable proofs.
    pub fn without_proofs(response: BatchGetWireItemResponse) -> Self {
        Self {
            response,
            proofs: DurableBatchPointReadProof::default(),
        }
    }

    /// Build the response and proof result from one request-order carrier.
    /// Association, missing-slot handling, and public response shaping happen
    /// here once; providers do not reconstruct a request or flatten a
    /// response for a second validation pass.
    pub fn from_tables(
        tables: Vec<BatchGetExecutionTable>,
        selection: BatchProofSelection,
    ) -> StorageResult<Self> {
        let mut seen_tables = crate::VecMap::with_capacity(tables.len());
        let mut responses = crate::VecMap::with_capacity(tables.len());
        let mut unprocessed_keys = crate::VecMap::with_capacity(tables.len());
        let mut proof_responses = HashMap::new();
        let mut proof_unprocessed = HashMap::new();

        for table in tables {
            if seen_tables.insert(table.table_name.clone(), ()).is_some() {
                return Err(StorageError::internal(
                    "batch-get result contains a duplicate table",
                ));
            }
            let requires_proof = selection == BatchProofSelection::ConsistentTables
                && table.keys.consistent_read.unwrap_or(false);
            match table.outcome {
                BatchGetExecutionTableOutcome::Items { items, proofs } => {
                    if items.len() != table.keys.keys.len() {
                        return Err(StorageError::internal(
                            "batch-get observation count must match requested keys",
                        ));
                    }
                    if requires_proof {
                        let entries = proofs.ok_or_else(|| {
                            StorageError::internal("batch-get item result has no matching proof")
                        })?;
                        validate_batch_get_slots(&table.keys.keys, &items, &entries)?;
                        proof_responses.insert(table.table_name.clone(), entries);
                    } else if proofs.is_some() {
                        return Err(StorageError::internal(
                            "batch-get proof was returned for an eventual-read table",
                        ));
                    }
                    let present = items.into_iter().flatten().collect::<Vec<_>>();
                    if !present.is_empty() {
                        responses.insert(table.table_name, present);
                    }
                }
                BatchGetExecutionTableOutcome::Unprocessed { proof } => {
                    if requires_proof {
                        let proof = proof.ok_or_else(|| {
                            StorageError::internal(
                                "batch-get unprocessed result has no matching proof",
                            )
                        })?;
                        if !keys_and_attributes_match(&table.keys, &proof) {
                            return Err(StorageError::internal(
                                "batch-get proof unprocessed keys do not match the request",
                            ));
                        }
                        proof_unprocessed.insert(table.table_name.clone(), *proof);
                    } else if proof.is_some() {
                        return Err(StorageError::internal(
                            "batch-get proof was returned for an eventual-read table",
                        ));
                    }
                    unprocessed_keys.insert(table.table_name, table.keys);
                }
            }
        }

        Ok(Self {
            response: BatchGetWireItemResponse {
                responses: (!responses.is_empty()).then_some(responses),
                unprocessed_keys: (!unprocessed_keys.is_empty()).then_some(unprocessed_keys),
                consumed_capacity: None,
            },
            proofs: DurableBatchPointReadProof {
                responses: proof_responses,
                unprocessed_keys: proof_unprocessed,
            },
        })
    }

    pub fn response(&self) -> &BatchGetWireItemResponse {
        &self.response
    }

    pub fn proofs(&self) -> &DurableBatchPointReadProof {
        &self.proofs
    }

    pub fn into_parts(self) -> (BatchGetWireItemResponse, DurableBatchPointReadProof) {
        (self.response, self.proofs)
    }

    pub fn into_response(self) -> BatchGetWireItemResponse {
        self.response
    }
}

fn validate_batch_get_slots(
    requested_keys: &[KeyAttributes],
    items: &[Option<WireItem>],
    entries: &[DurableBatchPointReadProofEntry],
) -> StorageResult<()> {
    if requested_keys.len() != items.len() || requested_keys.len() != entries.len() {
        return Err(StorageError::internal(
            "batch-get proof slots do not match requested keys",
        ));
    }
    for ((requested, item), entry) in requested_keys.iter().zip(items).zip(entries) {
        if !keys_have_same_identity(requested, &entry.key) {
            return Err(StorageError::internal(
                "batch-get proof key does not match the request ordinal",
            ));
        }
        match (&entry.proof, item) {
            (
                DurablePointReadProof::Present {
                    item: proof_item, ..
                },
                Some(item),
            ) => {
                let proof_map = proof_item.to_attribute_map()?;
                if !wire_item_has_key(&proof_map, requested) {
                    return Err(StorageError::internal(
                        "batch-get present proof item has the wrong key",
                    ));
                }
                if proof_map != item.to_attribute_map()? {
                    return Err(StorageError::internal(
                        "batch-get present proof payload does not match its item slot",
                    ));
                }
            }
            (DurablePointReadProof::Absent { .. }, None) => {}
            (DurablePointReadProof::Present { .. }, None) => {
                return Err(StorageError::internal(
                    "batch-get present proof has a missing item slot",
                ));
            }
            (DurablePointReadProof::Absent { .. }, Some(_)) => {
                return Err(StorageError::internal(
                    "batch-get absence proof has an item slot",
                ));
            }
        }
    }
    Ok(())
}

fn keys_and_attributes_match(left: &KeysAndAttributes, right: &KeysAndAttributes) -> bool {
    left.keys.len() == right.keys.len()
        && left
            .keys
            .iter()
            .zip(&right.keys)
            .all(|(left, right)| keys_have_same_identity(left, right))
        && left.attributes_to_get == right.attributes_to_get
        && left.projection_expression == right.projection_expression
        && left.expression_attribute_names == right.expression_attribute_names
        && left.consistent_read == right.consistent_read
}

fn keys_have_same_identity(left: &KeyAttributes, right: &KeyAttributes) -> bool {
    left.len() == right.len()
        && left.iter().all(|(name, left_value)| {
            right
                .get(name)
                .is_some_and(|right_value| key_values_equal(left_value, right_value))
        })
}

fn key_values_equal(left: &AttributeValue, right: &AttributeValue) -> bool {
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

fn wire_item_has_key(item: &AttributeMap, key: &KeyAttributes) -> bool {
    key.iter().all(|(name, expected)| {
        item.get(name)
            .is_some_and(|actual| key_values_equal(actual, expected))
    })
}
