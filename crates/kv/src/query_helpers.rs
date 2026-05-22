use storage_types::{AttributeValue, IndexName, ItemKey, TableName};

use crate::{SortedKvDbStorageProvider, sorted_kv_store::SortedKvStore};

impl<S: SortedKvStore> SortedKvDbStorageProvider<S> {
    #[must_use]
    pub fn build_hash_key_prefix(
        table_name: TableName,
        index_id: &Option<IndexName>,
        hash_value: &AttributeValue,
    ) -> ItemKey {
        match index_id {
            Some(index_id) => {
                ItemKey::index_prefix(table_name, index_id.clone(), hash_value.clone(), None)
            }
            None => ItemKey::table_key(table_name, hash_value.clone(), None),
        }
    }

    #[must_use]
    pub fn build_full_key(
        table_name: TableName,
        index_id: &Option<IndexName>,
        hash_value: &AttributeValue,
        range_value: &AttributeValue,
    ) -> ItemKey {
        match index_id {
            Some(index_id) => ItemKey::index_prefix(
                table_name,
                index_id.clone(),
                hash_value.clone(),
                Some(range_value.clone()),
            ),
            None => ItemKey::table_key(table_name, hash_value.clone(), Some(range_value.clone())),
        }
    }
}
