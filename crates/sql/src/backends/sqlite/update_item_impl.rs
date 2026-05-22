use std::collections::HashMap;

use storage_condition::Condition;
use storage_provider::BoundUpdateOperation;
use storage_types::{AttributeValue, KeyAttributes, StorageResult, TableName};

use crate::{
    SQLiteStorageProvider, provider_core::write::plan_update_from_existing_item, utils::SqliteConn,
};

impl SQLiteStorageProvider {
    pub fn do_update_item(
        operations: &[BoundUpdateOperation<'_>],
        condition: &Option<Condition>,
        table_name: &TableName,
        key: &KeyAttributes,
        sqlite: &SqliteConn<'_>,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<(
        HashMap<String, AttributeValue>,
        HashMap<String, AttributeValue>,
    )> {
        // First, get the item to return it if it exists
        let existing_item = Self::do_get_item(table_name, key, sqlite)?;
        let (item_to_update, updated_item) =
            plan_update_from_existing_item(existing_item, key, operations, condition.as_ref())?;

        // Try to put the updated item
        let _ = Self::do_put_item(
            table_name,
            &updated_item,
            &None,
            sqlite,
            immediate_gsi_consistency,
            None,
        )?;

        Ok((item_to_update, updated_item))
    }
}
