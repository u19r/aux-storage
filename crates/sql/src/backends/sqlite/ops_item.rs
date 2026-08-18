use std::collections::HashMap;

use storage_provider::{
    before_update_item_optional, return_values_need_updated_fields, update_item_response,
};
use storage_types::{
    AllOld, AttributeValue, DeleteItemRequest, KeyAttributes, PutItemEncodeRequest, PutItemRequest,
    StorageResult, TableName, UpdateItemRequest, WireItem,
};

use crate::{
    SQLiteStorageProvider,
    backends::sqlite::{
        delete_item_impl::DeleteItemInput, put_item_impl::PutItemInput,
        update_item_impl::UpdateItemInput,
    },
    storage_provider::parse_optional_condition,
    transaction_manager::with_transaction,
    utils::call_sqlite,
}; // now pub(crate)

impl SQLiteStorageProvider {
    pub async fn put_item_internal(
        &self,
        request: PutItemRequest,
    ) -> StorageResult<storage_types::PutItemResponse> {
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            );
        let PutItemRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let condition = parse_optional_condition(
            condition_expression,
            &expression_attribute_names,
            &expression_attribute_values,
        )?;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let old_value = with_transaction(&self.connection, move |sqlite| {
            crate::SQLiteStorageProvider::do_put_item(
                sqlite,
                PutItemInput {
                    table_name: &table_name,
                    item: &item,
                    condition: &condition,
                    indexers: indexers.as_deref(),
                    immediate_gsi_consistency,
                    return_old_on_condition_failure,
                    replication: None,
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                },
            )
        })
        .await?;
        let attributes = if matches!(return_values, Some(AllOld::AllOld)) {
            old_value
        } else {
            None
        };
        Ok(storage_types::PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    pub async fn put_item_wire_internal(
        &self,
        request: PutItemEncodeRequest,
    ) -> StorageResult<storage_types::PutItemResponse> {
        let PutItemEncodeRequest {
            table_name,
            item,
            indexers,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure,
            aux_item_stream_ttl_hours,
        } = request;
        let condition = parse_optional_condition(
            condition_expression,
            &expression_attribute_names,
            &expression_attribute_values,
        )?;
        let should_return_old = matches!(return_values, Some(AllOld::AllOld));
        let item = item.into_attribute_map()?;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let old_value = with_transaction(&self.connection, move |sqlite| {
            crate::SQLiteStorageProvider::do_put_item(
                sqlite,
                PutItemInput {
                    table_name: &table_name,
                    item: &item,
                    condition: &condition,
                    indexers: indexers.as_deref(),
                    immediate_gsi_consistency,
                    return_old_on_condition_failure,
                    replication: None,
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                },
            )
        })
        .await?;
        let attributes = if should_return_old { old_value } else { None };
        Ok(storage_types::PutItemResponse {
            attributes: attributes.map(Into::into),
        })
    }

    pub async fn get_item_internal(
        &self,
        table_name: TableName,
        key: KeyAttributes,
    ) -> StorageResult<Option<WireItem>> {
        call_sqlite(&self.connection, move |conn| {
            let sqlite = crate::utils::SqliteConn::Connection(conn);
            crate::SQLiteStorageProvider::do_get_wire_item(&table_name, &key, &sqlite)
        })
        .await
    }

    pub async fn delete_item_internal(
        &self,
        request: DeleteItemRequest,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            );
        let DeleteItemRequest {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let condition = parse_optional_condition(
            condition_expression,
            &expression_attribute_names,
            &expression_attribute_values,
        )?;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        with_transaction(&self.connection, move |sqlite| {
            crate::SQLiteStorageProvider::do_delete_item(
                sqlite,
                DeleteItemInput {
                    table_name: &table_name,
                    key: &key,
                    condition: &condition,
                    immediate_gsi_consistency,
                    return_old_on_condition_failure,
                    replication: None,
                    old_indexers: None,
                    item_stream_ttl_hours: aux_item_stream_ttl_hours,
                },
            )
        })
        .await
    }

    pub async fn update_item_internal(
        &self,
        request: UpdateItemRequest,
    ) -> StorageResult<storage_types::UpdateItemResponse> {
        let UpdateItemRequest {
            table_name,
            key,
            indexers,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            aux_item_stream_ttl_hours,
            ..
        } = request;
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let collect_response_fields = return_values_need_updated_fields(return_values.as_ref());
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                return_values_on_condition_check_failure.as_ref(),
            );
        let (old_item, new_item, response_fields) =
            with_transaction(&self.connection, move |sqlite| {
                let (operations, condition) = before_update_item_optional(
                    update_expression.as_deref(),
                    condition_expression.as_deref(),
                    expression_attribute_names.as_ref(),
                    expression_attribute_values.as_ref(),
                )?;
                let response_fields = if collect_response_fields {
                    {
                        operations
                            .iter()
                            .map(|operation| operation.field_name_arc())
                            .collect::<Vec<_>>()
                    }
                } else {
                    Default::default()
                };
                crate::SQLiteStorageProvider::do_update_item(
                    sqlite,
                    UpdateItemInput {
                        operations: &operations,
                        condition: &condition,
                        table_name: &table_name,
                        key: &key,
                        indexers: indexers.as_deref(),
                        immediate_gsi_consistency,
                        return_old_on_condition_failure,
                        item_stream_ttl_hours: aux_item_stream_ttl_hours,
                    },
                )
                .map(|(old_item, new_item)| (old_item, new_item, response_fields))
            })
            .await?;

        update_item_response(
            &response_fields,
            Some(old_item),
            Some(new_item),
            return_values.as_ref(),
        )
    }
}
