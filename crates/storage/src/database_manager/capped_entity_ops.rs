use storage_types::{
    AttributeValue, StorageEnum, StorageError, StorageResult, TableName, TransactEncodeItem,
    TransactUpdateRequest, TransactWriteItemsEncodeRequest, context::WrappedError as _,
};

use crate::database_manager::{
    CAPPED_ENTITY_COUNTER_CREATE_CONDITION, CAPPED_ENTITY_COUNTER_DELETE_CONDITION,
    CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION, CappedStorageError, CreateCappedEntityInput,
    DatabaseManager, DeleteCappedEntityInput, ENTITY_ABSENT_CONDITION, ENTITY_EXISTS_CONDITION,
    capped_entity_counter_expression_names, capped_entity_counter_expression_values,
    capped_entity_counter_key, is_conditional_failure, transaction_canceled_reason_is_conditional,
};

impl DatabaseManager {
    pub async fn create_capped_entity<T>(
        &self,
        input: CreateCappedEntityInput<'_, T>,
    ) -> Result<(), CappedStorageError>
    where
        T: storage_types::single_table_entity::SingleTableEntity + serde::Serialize,
    {
        let CreateCappedEntityInput {
            table_name,
            item,
            counted_entity_type,
            max_value,
            additional_transact_items,
        } = input;
        let counter_index = 1 + additional_transact_items.len();
        let item_key = item.table_key_map();
        let wire_item = storage_types::single_table_entity::to_wire_item(item)
            .map_err(|err| StorageError::internal(&err.to_string()))?;

        let mut transact_items = Vec::with_capacity(counter_index + 1);
        transact_items.push(TransactEncodeItem {
            put: Some(storage_types::TransactEncodePutRequest {
                table_name: table_name.clone(),
                item: wire_item,
                condition_expression: Some(ENTITY_ABSENT_CONDITION.to_string()),
                expression_attribute_names: None,
                expression_attribute_values: None,
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        });
        transact_items.extend(additional_transact_items);
        transact_items.push(TransactEncodeItem {
            update: Some(TransactUpdateRequest {
                table_name: table_name.clone(),
                key: capped_entity_counter_key(&counted_entity_type).into(),
                update_expression: CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION.to_string(),
                condition_expression: Some(CAPPED_ENTITY_COUNTER_CREATE_CONDITION.to_string()),
                expression_attribute_names: Some(capped_entity_counter_expression_names()),
                expression_attribute_values: Some(capped_entity_counter_expression_values(
                    1,
                    &counted_entity_type,
                    Some(max_value),
                    None,
                )),
                return_values_on_condition_check_failure: None,
                aux_item_stream_ttl_hours: None,
            }),
            ..Default::default()
        });

        match self
            .transact_write_items_encode(TransactWriteItemsEncodeRequest {
                transact_items,
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(self
                .classify_create_capped_entity_error(
                    error,
                    &table_name,
                    item_key.into(),
                    counted_entity_type.clone(),
                    max_value,
                    counter_index,
                )
                .await),
        }
    }

    pub async fn delete_capped_entity(
        &self,
        input: DeleteCappedEntityInput,
    ) -> Result<(), CappedStorageError> {
        let DeleteCappedEntityInput {
            table_name,
            key,
            counted_entity_type,
        } = input;
        let item_key = key.clone();
        let transact_items = vec![
            TransactEncodeItem {
                delete: Some(storage_types::TransactDeleteRequest {
                    table_name: table_name.clone(),
                    key,
                    condition_expression: Some(ENTITY_EXISTS_CONDITION.to_string()),
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..Default::default()
            },
            TransactEncodeItem {
                update: Some(TransactUpdateRequest {
                    table_name: table_name.clone(),
                    key: capped_entity_counter_key(&counted_entity_type).into(),
                    update_expression: CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION.to_string(),
                    condition_expression: Some(CAPPED_ENTITY_COUNTER_DELETE_CONDITION.to_string()),
                    expression_attribute_names: Some(capped_entity_counter_expression_names()),
                    expression_attribute_values: Some(capped_entity_counter_expression_values(
                        -1,
                        &counted_entity_type,
                        None,
                        Some(0),
                    )),
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..Default::default()
            },
        ];

        match self
            .transact_write_items_encode(TransactWriteItemsEncodeRequest {
                transact_items,
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(self
                .classify_delete_capped_entity_error(error, &table_name, item_key, 0)
                .await),
        }
    }

    async fn classify_create_capped_entity_error(
        &self,
        error: StorageError,
        table_name: &TableName,
        item_key: storage_types::KeyAttributes,
        counted_entity_type: String,
        max_value: u64,
        counter_index: usize,
    ) -> CappedStorageError {
        if let StorageEnum::TransactionCanceled { reasons } = error.to_enum() {
            if transaction_canceled_reason_is_conditional(reasons, 0) {
                return CappedStorageError::ItemExistError;
            }
            if transaction_canceled_reason_is_conditional(reasons, counter_index) {
                return CappedStorageError::CapacityExceededError;
            }
        }

        if !is_conditional_failure(&error) {
            return CappedStorageError::StorageError(error);
        }

        match self
            .get_item_map_with_consistent_read(table_name.clone(), item_key, true)
            .await
        {
            Ok(Some(_)) => CappedStorageError::ItemExistError,
            Ok(None) => match self
                .load_capped_entity_counter_value(table_name, &counted_entity_type)
                .await
            {
                Ok(Some(value)) if value >= max_value => CappedStorageError::CapacityExceededError,
                Ok(_) => CappedStorageError::StorageError(error),
                Err(counter_error) => CappedStorageError::StorageError(counter_error),
            },
            Err(read_error) => CappedStorageError::StorageError(read_error),
        }
    }

    async fn classify_delete_capped_entity_error(
        &self,
        error: StorageError,
        table_name: &TableName,
        item_key: storage_types::KeyAttributes,
        delete_index: usize,
    ) -> CappedStorageError {
        if let StorageEnum::TransactionCanceled { reasons } = error.to_enum()
            && transaction_canceled_reason_is_conditional(reasons, delete_index)
        {
            return CappedStorageError::ItemNotExistsError;
        }

        if !is_conditional_failure(&error) {
            return CappedStorageError::StorageError(error);
        }

        match self
            .get_item_map_with_consistent_read(table_name.clone(), item_key, true)
            .await
        {
            Ok(None) => CappedStorageError::ItemNotExistsError,
            Ok(Some(_)) => CappedStorageError::StorageError(error),
            Err(read_error) => CappedStorageError::StorageError(read_error),
        }
    }

    async fn load_capped_entity_counter_value(
        &self,
        table_name: &TableName,
        counted_entity_type: &str,
    ) -> StorageResult<Option<u64>> {
        let item = self
            .get_item_map_with_consistent_read(
                table_name.clone(),
                capped_entity_counter_key(counted_entity_type),
                true,
            )
            .await?;
        let Some(item) = item else {
            return Ok(None);
        };
        let Some(value) = item.get(crate::database_manager::CAPPED_ENTITY_COUNTER_VALUE_ATTR)
        else {
            return Ok(None);
        };
        match value {
            AttributeValue::N(raw) => raw.parse::<u64>().map(Some).map_err(|err| {
                StorageError::internal(&format!(
                    "invalid capped entity counter value for {counted_entity_type}: {err}"
                ))
            }),
            other => Err(StorageError::internal(&format!(
                "capped entity counter value for {counted_entity_type} must be numeric, got \
                 {other:?}"
            ))),
        }
    }
}
