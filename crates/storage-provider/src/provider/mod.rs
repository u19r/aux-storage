mod storage_provider_trait;

pub use storage_provider_trait::{
    StorageProvider, StorageProviderReadContext, split_item_into_key_and_attributes_sync,
};

#[cfg(test)]
mod provider_tests;
