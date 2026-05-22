//! Unified read path scaffolding shared across backends.
//! Backends translate a higher-level request (query/scan) into a ReadPlan.
//! This layer stays pure: it does not hit storage, only organizes parameters.
use storage_types::{IndexName, KeySchemaElement, TableName};

#[derive(Debug, Clone, PartialEq)]
pub enum ReadOrigin {
    Primary,
    Gsi(IndexName),
}

#[derive(Debug, Clone)]
pub struct ReadPlan {
    pub table: TableName,
    pub origin: ReadOrigin,
    pub key_schema: Vec<KeySchemaElement>,
    pub limit: u32,
    pub exclusive_start_key: Option<String>,
    pub filter_expression: Option<String>, // placeholder for future typed filter AST
}

impl ReadPlan {
    pub fn new(
        table: TableName,
        origin: ReadOrigin,
        key_schema: Vec<KeySchemaElement>,
        limit: u32,
        exclusive_start_key: Option<String>,
    ) -> Self {
        Self {
            table,
            origin,
            key_schema,
            limit,
            exclusive_start_key,
            filter_expression: None,
        }
    }
}
