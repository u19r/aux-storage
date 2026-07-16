mod storage_provider_trait;

use std::{collections::HashMap, sync::Arc};

use storage_types::{AttributeValue, KeyAttributes, StorageResult, TableName};

pub enum AtomicItemWriteDecision {
    NoWrite { output: Vec<u8> },
    Write {
        item: HashMap<String, AttributeValue>,
        additional_items: Vec<HashMap<String, AttributeValue>>,
        output: Vec<u8>,
    },
}

pub type AtomicItemTransform = Arc<
    dyn Fn(Option<&HashMap<String, AttributeValue>>) -> StorageResult<AtomicItemWriteDecision>
        + Send
        + Sync,
>;

pub struct AtomicItemReadModifyWriteRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub transform: AtomicItemTransform,
}

pub use storage_provider_trait::{
    StorageProvider, StorageProviderReadContext, split_item_into_key_and_attributes_sync,
};

#[cfg(test)]
mod provider_tests;
