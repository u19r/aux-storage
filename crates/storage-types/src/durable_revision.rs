use std::collections::HashMap;

use serde::Serialize;

use crate::{
    AllOld, AttributeValue, KeyAttributes, KeysAndAttributes, TableName, TransactWriteItemsRequest,
    UpdateItemRequest, WireItem,
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
    pub item: HashMap<String, AttributeValue>,
    pub guard: DurablePointReadGuard,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    pub return_values: Option<AllOld>,
}

#[derive(Debug, Clone)]
pub struct GuardedDeleteItemRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub guard: DurablePointReadGuard,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<HashMap<String, String>>,
    pub expression_attribute_values: Option<HashMap<String, AttributeValue>>,
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

#[derive(Debug, Clone)]
pub struct DurableBatchPointReadRequest {
    pub request_items: HashMap<TableName, KeysAndAttributes>,
}

#[derive(Debug, Clone)]
pub struct DurableBatchPointReadProofEntry {
    pub key: KeyAttributes,
    pub proof: DurablePointReadProof,
}

#[derive(Debug, Clone, Default)]
pub struct DurableBatchPointReadProof {
    pub responses: HashMap<TableName, Vec<DurableBatchPointReadProofEntry>>,
    pub unprocessed_keys: HashMap<TableName, KeysAndAttributes>,
}
