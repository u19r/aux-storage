use std::collections::HashMap;

use storage_condition::Condition;
use storage_provider::BoundUpdateOperation;
use storage_types::{
    AttributeValue, KeyAttributes, StorageResult, StreamRetentionDuration, TableName,
};

use crate::{
    SQLiteStorageProvider, provider_core::write::plan_update_from_existing_item, utils::SqliteConn,
};

pub(crate) struct UpdateItemInput<'a> {
    pub(crate) operations: &'a [BoundUpdateOperation<'a>],
    pub(crate) condition: &'a Option<Condition>,
    pub(crate) table_name: &'a TableName,
    pub(crate) key: &'a KeyAttributes,
    pub(crate) immediate_gsi_consistency: bool,
    pub(crate) return_old_on_condition_failure: bool,
    pub(crate) item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl SQLiteStorageProvider {
    pub(crate) fn do_update_item(
        sqlite: &SqliteConn<'_>,
        input: UpdateItemInput<'_>,
    ) -> StorageResult<(
        HashMap<String, AttributeValue>,
        HashMap<String, AttributeValue>,
    )> {
        let UpdateItemInput {
            operations,
            condition,
            table_name,
            key,
            immediate_gsi_consistency,
            return_old_on_condition_failure,
            item_stream_ttl_hours,
        } = input;
        // First, get the item to return it if it exists
        let existing_item = Self::do_get_item(table_name, key, sqlite)?;
        let (item_to_update, updated_item) = plan_update_from_existing_item(
            existing_item,
            key,
            operations,
            condition.as_ref(),
            return_old_on_condition_failure,
        )?;

        // Try to put the updated item
        let _ = Self::do_put_item(
            sqlite,
            crate::backends::sqlite::put_item_impl::PutItemInput {
                table_name,
                item: &updated_item,
                condition: &None,
                immediate_gsi_consistency,
                return_old_on_condition_failure: false,
                replication: None,
                item_stream_ttl_hours,
            },
        )?;

        Ok((item_to_update, updated_item))
    }
}
