use std::{collections::HashMap, sync::LazyLock};

use storage_condition::{evaluate_condition, parse_condition_expression};
use storage_provider::before_update_item;
use storage_types::{
    AttributeValue, GuardedTransactWriteItemsRequest, StorageEnum, StorageError, StorageResult,
    TransactWriteItemsEncodeRequest, context::WrappedError as _,
};
use tracing::warn;

use crate::{
    SQLiteStorageProvider,
    provider_core::transaction::{
        TransactionKeyPreflight, all_old, conditional_check_failed_reason,
        preflight_transact_item_key_with_table_info, transact_item_table_name,
        transaction_canceled_for_indexed_reasons, transaction_canceled_for_item_error_with_len,
        transaction_canceled_for_preflights, transaction_canceled_for_reason,
        transaction_cancellation_reason_at, validate_no_duplicate_transact_item_keys,
        validate_transact_key, validate_transact_put_item_key,
    },
    transaction_manager::with_transaction,
    utils::SqliteConn,
};

pub(crate) fn condition_item_ref(
    old_item: Option<&HashMap<String, AttributeValue>>,
) -> &HashMap<String, AttributeValue> {
    static EMPTY_ITEM: LazyLock<HashMap<String, AttributeValue>> = LazyLock::new(HashMap::new);
    old_item.unwrap_or(&EMPTY_ITEM)
}

impl SQLiteStorageProvider {
    fn execute_transact_put_encode(
        sqlite: &SqliteConn<'_>,
        put_request: &storage_types::TransactEncodePutRequest,
        item_index: usize,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<()> {
        let table_info = Self::do_get_table_info(&put_request.table_name, sqlite)?;
        let item = put_request.item.to_attribute_map()?;
        validate_transact_put_item_key(&table_info, &item)?;
        let old_item = if put_request.condition_expression.is_some() {
            let key = storage_provider::split_item_into_key_and_attributes_sync(
                item.clone(),
                &table_info,
            )?
            .key_attributes;
            Self::do_get_item(&put_request.table_name, &key, sqlite)?
        } else {
            None
        };
        let condition = if let Some(condition_expression) = &put_request.condition_expression {
            parse_condition_expression(
                condition_expression,
                put_request.expression_attribute_names.as_ref(),
                put_request.expression_attribute_values.as_ref(),
            )
            .map_err(|e| {
                warn!(error = e);
                StorageError::validation("Invalid condition expression")
            })
            .map(Some)?
        } else {
            None
        };

        if let Some(condition) = &condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(transaction_canceled_for_reason(
                item_index,
                conditional_check_failed_reason(
                    all_old(
                        put_request
                            .return_values_on_condition_check_failure
                            .as_ref(),
                    )
                    .then_some(old_item.as_ref())
                    .flatten(),
                )?,
            ));
        }

        Self::do_put_wire_item(
            &put_request.table_name,
            &put_request.item,
            &None,
            sqlite,
            immediate_gsi_consistency,
            false,
            None,
            put_request.aux_item_stream_ttl_hours,
        )
        .map(|_| ())
    }

    fn execute_transact_put(
        sqlite: &SqliteConn<'_>,
        put_request: &storage_types::TransactPutRequest,
        item_index: usize,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<()> {
        let table_info = Self::do_get_table_info(&put_request.table_name, sqlite)?;
        validate_transact_put_item_key(&table_info, &put_request.item)?;
        let old_item = if put_request.condition_expression.is_some() {
            let key = storage_provider::split_item_into_key_and_attributes_sync(
                put_request.item.clone(),
                &table_info,
            )?
            .key_attributes;
            Self::do_get_item(&put_request.table_name, &key, sqlite)?
        } else {
            None
        };
        let condition = if let Some(condition_expression) = &put_request.condition_expression {
            parse_condition_expression(
                condition_expression,
                put_request.expression_attribute_names.as_ref(),
                put_request.expression_attribute_values.as_ref(),
            )
            .map_err(|e| {
                warn!(error = e);
                StorageError::validation("Invalid condition expression")
            })
            .map(Some)?
        } else {
            None
        };

        if let Some(condition) = &condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(transaction_canceled_for_reason(
                item_index,
                conditional_check_failed_reason(
                    all_old(
                        put_request
                            .return_values_on_condition_check_failure
                            .as_ref(),
                    )
                    .then_some(old_item.as_ref())
                    .flatten(),
                )?,
            ));
        }

        Self::do_put_item(
            &put_request.table_name,
            &put_request.item,
            &None,
            sqlite,
            immediate_gsi_consistency,
            None,
            put_request.aux_item_stream_ttl_hours,
        )
        .map(|_| ())
    }

    fn execute_transact_update(
        sqlite: &SqliteConn<'_>,
        update_request: &storage_types::TransactUpdateRequest,
        item_index: usize,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<()> {
        let (operations, condition) = before_update_item(
            update_request.update_expression.as_str(),
            update_request.condition_expression.as_deref(),
            update_request.expression_attribute_names.as_ref(),
            update_request.expression_attribute_values.as_ref(),
        )?;
        let old_item = update_request
            .condition_expression
            .is_some()
            .then(|| Self::do_get_item(&update_request.table_name, &update_request.key, sqlite))
            .transpose()?
            .flatten();

        let result = Self::do_update_item(
            &operations,
            &condition,
            &update_request.table_name,
            &update_request.key,
            sqlite,
            immediate_gsi_consistency,
            update_request.aux_item_stream_ttl_hours,
        );
        if let Err(error) = result {
            if matches!(error.to_enum(), StorageEnum::ConditionalCheckFailed) {
                return Err(transaction_canceled_for_reason(
                    item_index,
                    conditional_check_failed_reason(
                        all_old(
                            update_request
                                .return_values_on_condition_check_failure
                                .as_ref(),
                        )
                        .then_some(old_item.as_ref())
                        .flatten(),
                    )?,
                ));
            }
            return Err(error);
        }
        Ok(())
    }

    fn execute_transact_delete(
        sqlite: &SqliteConn<'_>,
        delete_request: &storage_types::TransactDeleteRequest,
        item_index: usize,
        immediate_gsi_consistency: bool,
    ) -> StorageResult<()> {
        if delete_request.key.is_empty() {
            return Err(StorageError::validation(
                "Delete request must specify a key",
            ));
        }

        let table_info = Self::do_get_table_info(&delete_request.table_name, sqlite)?;
        validate_transact_key(&table_info, &delete_request.key)?;
        let old_item = if delete_request.condition_expression.is_some() {
            Self::do_get_item(&delete_request.table_name, &delete_request.key, sqlite)?
        } else {
            None
        };
        let condition = if let Some(condition_expression) = &delete_request.condition_expression {
            parse_condition_expression(
                condition_expression,
                delete_request.expression_attribute_names.as_ref(),
                delete_request.expression_attribute_values.as_ref(),
            )
            .map_err(|e| {
                warn!(error = e);
                StorageError::validation("Invalid condition expression")
            })
            .map(Some)?
        } else {
            None
        };

        if let Some(condition) = &condition
            && !evaluate_condition(condition_item_ref(old_item.as_ref()), condition)
        {
            return Err(transaction_canceled_for_reason(
                item_index,
                conditional_check_failed_reason(
                    all_old(
                        delete_request
                            .return_values_on_condition_check_failure
                            .as_ref(),
                    )
                    .then_some(old_item.as_ref())
                    .flatten(),
                )?,
            ));
        }

        Self::do_delete_item(
            &delete_request.table_name,
            &delete_request.key,
            &None,
            sqlite,
            immediate_gsi_consistency,
            None,
            delete_request.aux_item_stream_ttl_hours,
        )
        .map(|_| ())
    }

    fn execute_transact_condition_check(
        sqlite: &SqliteConn<'_>,
        condition_check_request: &storage_types::TransactConditionCheckRequest,
        item_index: usize,
    ) -> StorageResult<()> {
        let storage_types::TransactConditionCheckRequest {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values_on_condition_check_failure,
            ..
        } = condition_check_request;

        let table_info = Self::do_get_table_info(table_name, sqlite)?;
        validate_transact_key(&table_info, key)?;
        let old_item = Self::do_get_item(table_name, key, sqlite)?;

        let condition = parse_condition_expression(
            condition_expression,
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )
        .map_err(|e| {
            warn!(error = e);
            StorageError::validation("Invalid condition expression")
        })?;

        if !evaluate_condition(condition_item_ref(old_item.as_ref()), &condition) {
            return Err(transaction_canceled_for_reason(
                item_index,
                conditional_check_failed_reason(
                    all_old(return_values_on_condition_check_failure.as_ref())
                        .then_some(old_item.as_ref())
                        .flatten(),
                )?,
            ));
        }

        Ok(())
    }

    pub async fn do_transact_write_items(
        &self,
        request: storage_types::TransactWriteItemsRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        // Validate request
        if request.transact_items.is_empty() {
            return Err(StorageError::validation(
                "Transaction request must contain at least one item",
            ));
        }

        if request.transact_items.len() > 100 {
            return Err(StorageError::validation(
                "Transaction request cannot contain more than 100 items",
            ));
        }

        // Execute transaction
        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let result = with_transaction(&self.connection, move |sqlite| {
            let mut preflights = Vec::with_capacity(request.transact_items.len());
            for item in &request.transact_items {
                preflights.push(Self::preflight_transact_item_key(sqlite, item)?);
            }
            if let Some(error) = transaction_canceled_for_preflights(&preflights) {
                return Err(error);
            }
            validate_no_duplicate_transact_item_keys(&preflights)?;

            let item_count = request.transact_items.len();
            let mut cancellation_reasons = vec![None; item_count];
            for (index, item) in request.transact_items.iter().enumerate() {
                let result = match item {
                    storage_types::TransactWriteItem {
                        put: Some(put_request),
                        update: None,
                        delete: None,
                        condition_check: None,
                    } => Self::execute_transact_put(
                        sqlite,
                        put_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: Some(update_request),
                        delete: None,
                        condition_check: None,
                    } => Self::execute_transact_update(
                        sqlite,
                        update_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: None,
                        delete: Some(delete_request),
                        condition_check: None,
                    } => Self::execute_transact_delete(
                        sqlite,
                        delete_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: None,
                        delete: None,
                        condition_check: Some(condition_check_request),
                    } => Self::execute_transact_condition_check(
                        sqlite,
                        condition_check_request,
                        index,
                    ),
                    _ => {
                        return Err(StorageError::validation(
                            "Each TransactWriteItem must contain exactly one of Put, Update, \
                             Delete, or ConditionCheck",
                        ));
                    }
                };
                if let Err(error) = result {
                    let Some(reason) = transaction_cancellation_reason_at(&error, index) else {
                        return Err(error);
                    };
                    cancellation_reasons[index] = Some(reason);
                }
            }
            if let Some(error) = transaction_canceled_for_indexed_reasons(cancellation_reasons) {
                return Err(error);
            }
            Ok(())
        })
        .await;

        match result {
            Ok(()) => Ok(storage_types::TransactWriteItemsResponse {
                consumed_capacity: None,
                item_collection_metrics: None,
            }),
            Err(e) => Err(e),
        }
    }

    fn preflight_transact_item_key(
        sqlite: &SqliteConn<'_>,
        item: &storage_types::TransactWriteItem,
    ) -> StorageResult<TransactionKeyPreflight> {
        let Some(table_name) = transact_item_table_name(item) else {
            return Ok(TransactionKeyPreflight::default());
        };
        let table_info = Self::do_get_table_info(table_name, sqlite)?;
        preflight_transact_item_key_with_table_info(item, &table_info)
    }

    pub async fn do_guarded_transact_write_items(
        &self,
        request: GuardedTransactWriteItemsRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        if request.request.transact_items.is_empty() {
            return Err(StorageError::validation(
                "Transaction request must contain at least one item",
            ));
        }

        if request.request.transact_items.len() > 100 {
            return Err(StorageError::validation(
                "Transaction request cannot contain more than 100 items",
            ));
        }

        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let result = with_transaction(&self.connection, move |sqlite| {
            for guard in &request.guards {
                Self::validate_durable_guard(&guard.table_name, &guard.key, &guard.guard, sqlite)?;
            }
            let item_count = request.request.transact_items.len();
            for (index, item) in request.request.transact_items.iter().enumerate() {
                let result = match item {
                    storage_types::TransactWriteItem {
                        put: Some(put_request),
                        update: None,
                        delete: None,
                        condition_check: None,
                    } => Self::execute_transact_put(
                        sqlite,
                        put_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: Some(update_request),
                        delete: None,
                        condition_check: None,
                    } => Self::execute_transact_update(
                        sqlite,
                        update_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: None,
                        delete: Some(delete_request),
                        condition_check: None,
                    } => Self::execute_transact_delete(
                        sqlite,
                        delete_request,
                        index,
                        immediate_gsi_consistency,
                    ),
                    storage_types::TransactWriteItem {
                        put: None,
                        update: None,
                        delete: None,
                        condition_check: Some(condition_check_request),
                    } => Self::execute_transact_condition_check(
                        sqlite,
                        condition_check_request,
                        index,
                    ),
                    _ => {
                        return Err(StorageError::validation(
                            "Each TransactWriteItem must contain exactly one of Put, Update, \
                             Delete, or ConditionCheck",
                        ));
                    }
                };
                if let Err(error) = result {
                    return Err(transaction_canceled_for_item_error_with_len(
                        index, item_count, error,
                    ));
                }
            }
            Ok(())
        })
        .await;

        match result {
            Ok(()) => Ok(storage_types::TransactWriteItemsResponse {
                consumed_capacity: None,
                item_collection_metrics: None,
            }),
            Err(e) => Err(e),
        }
    }

    pub async fn do_transact_write_items_encode(
        &self,
        request: TransactWriteItemsEncodeRequest,
    ) -> StorageResult<storage_types::TransactWriteItemsResponse> {
        if request.transact_items.is_empty() {
            return Err(StorageError::validation(
                "Transaction request must contain at least one item",
            ));
        }

        if request.transact_items.len() > 100 {
            return Err(StorageError::validation(
                "Transaction request cannot contain more than 100 items",
            ));
        }

        let immediate_gsi_consistency = self.immediate_gsi_consistency;
        let result = with_transaction(&self.connection, move |sqlite| {
            for (index, item) in request.transact_items.iter().enumerate() {
                match item {
                    storage_types::TransactEncodeItem {
                        put: Some(put_request),
                        update: None,
                        delete: None,
                        condition_check: None,
                    } => {
                        Self::execute_transact_put_encode(
                            sqlite,
                            put_request,
                            index,
                            immediate_gsi_consistency,
                        )?;
                    }
                    storage_types::TransactEncodeItem {
                        put: None,
                        update: Some(update_request),
                        delete: None,
                        condition_check: None,
                    } => {
                        Self::execute_transact_update(
                            sqlite,
                            update_request,
                            index,
                            immediate_gsi_consistency,
                        )?;
                    }
                    storage_types::TransactEncodeItem {
                        put: None,
                        update: None,
                        delete: Some(delete_request),
                        condition_check: None,
                    } => {
                        Self::execute_transact_delete(
                            sqlite,
                            delete_request,
                            index,
                            immediate_gsi_consistency,
                        )?;
                    }
                    storage_types::TransactEncodeItem {
                        put: None,
                        update: None,
                        delete: None,
                        condition_check: Some(condition_check_request),
                    } => {
                        Self::execute_transact_condition_check(
                            sqlite,
                            condition_check_request,
                            index,
                        )?;
                    }
                    _ => {
                        return Err(StorageError::validation(
                            "Each TransactWriteItem must contain exactly one of Put, Update, \
                             Delete, or ConditionCheck",
                        ));
                    }
                }
            }
            Ok(())
        })
        .await;

        match result {
            Ok(()) => Ok(storage_types::TransactWriteItemsResponse {
                consumed_capacity: None,
                item_collection_metrics: None,
            }),
            Err(e) => Err(e),
        }
    }
}
