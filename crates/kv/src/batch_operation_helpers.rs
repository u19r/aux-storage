use std::collections::HashMap;

use storage_types::{StorageResult, TableName, WriteRequest};

use crate::{SortedKvDbStorageProvider, sorted_kv_store::SortedKvStore};

impl<S: SortedKvStore> SortedKvDbStorageProvider<S> {
    pub fn handle_batch_write_error(
        &self,
        table_name: &TableName,
        write_requests: &[WriteRequest],
        unprocessed_items: &mut HashMap<TableName, Vec<WriteRequest>>,
    ) -> StorageResult<()> {
        if !write_requests.is_empty() {
            unprocessed_items.insert(table_name.clone(), write_requests.to_vec());
        }
        Ok(())
    }

    pub fn collect_unprocessed_batch_items(
        unprocessed_table_items: Vec<WriteRequest>,
        table_name: &TableName,
        unprocessed_items: &mut HashMap<TableName, Vec<WriteRequest>>,
    ) {
        if !unprocessed_table_items.is_empty() {
            unprocessed_items.insert(table_name.clone(), unprocessed_table_items);
        }
    }
}
