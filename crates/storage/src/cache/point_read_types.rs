use std::{collections::HashMap, time::Duration};

pub use storage_types::{DurableAbsenceProof, DurableItemRevision};
use storage_types::{KeyAttributes, KeysAndAttributes, TableName, WireItem};

#[derive(Debug, Clone, PartialEq)]
pub struct PointReadGetRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
}

#[derive(Debug, Clone)]
pub enum PointReadGetResult {
    Hit(Box<Option<WireItem>>),
    Miss,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthoritativePointReadPurpose {
    StrongGet,
    StrongBatchGet,
    UpdatePreImage,
    ConditionalPutPreImage,
    ConditionalDeletePreImage,
    TransactionPreImage,
    QueryProofPrewriteImage,
}

#[derive(Debug, Clone)]
pub enum AuthoritativePointReadHit {
    Present {
        item: Box<WireItem>,
        revision: Option<DurableItemRevision>,
    },
    Absent {
        proof: Option<DurableAbsenceProof>,
    },
}

#[derive(Debug, Clone)]
pub enum AuthoritativePointReadResult {
    Hit(Box<AuthoritativePointReadHit>),
    Miss,
}

#[derive(Debug, Clone)]
pub struct PointReadBatchGetResult {
    pub responses: HashMap<TableName, Vec<WireItem>>,
    pub unresolved_request_items: HashMap<TableName, KeysAndAttributes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointReadCacheEvictionPolicy {
    Lru,
    TwoQueue,
}

#[derive(Debug, Clone, Copy)]
pub struct InMemoryPointReadCacheConfig {
    pub capacity: usize,
    pub max_bytes: usize,
    pub ttl: Duration,
    pub eviction_policy: PointReadCacheEvictionPolicy,
}

impl Default for InMemoryPointReadCacheConfig {
    fn default() -> Self {
        Self {
            capacity: 10_000,
            max_bytes: 64 * 1024 * 1024,
            ttl: Duration::from_secs(300),
            eviction_policy: PointReadCacheEvictionPolicy::TwoQueue,
        }
    }
}
