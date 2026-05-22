use storage_types::{StorageError, StorageResult};

use crate::{
    backends::postgres::{PostgresStorageProvider, record_write},
    billing_metrics::WriteCostTally,
    provider_core::transaction::{
        TransactionKeyPreflight, preflight_transact_item_key_with_table_info,
        transact_item_table_name, transaction_canceled_for_item_error_with_len,
        transaction_canceled_for_preflights, validate_no_duplicate_transact_item_keys,
    },
};

impl PostgresStorageProvider {
    pub(super) async fn do_transact_write_items(
        &self,
        request: storage_types::TransactWriteItemsRequest,
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

        let mut billed_tally = WriteCostTally::default();
        for item in &request.transact_items {
            billed_tally.record_transact_item(item);
        }
        let response = self
            .retry_postgres_conflicts("transact_write_items", || {
                let request = request.clone();
                async move {
                    let mut client = self
                        .pool
                        .get()
                        .await
                        .map_err(Self::map_postgres_client_acquire_error)?;
                    let tx = client.transaction().await.map_err(|err| {
                        Self::map_postgres_write_error("start transact write transaction", err)
                    })?;

                    let mut preflights = Vec::with_capacity(request.transact_items.len());
                    for item in &request.transact_items {
                        preflights.push(self.preflight_transact_item_key(item).await?);
                    }
                    if let Some(error) = transaction_canceled_for_preflights(&preflights) {
                        return Err(error);
                    }
                    validate_no_duplicate_transact_item_keys(&preflights)?;

                    let item_count = request.transact_items.len();
                    for (index, item) in request.transact_items.clone().into_iter().enumerate() {
                        let action_count = usize::from(item.put.is_some())
                            + usize::from(item.update.is_some())
                            + usize::from(item.delete.is_some())
                            + usize::from(item.condition_check.is_some());
                        if action_count != 1 {
                            return Err(StorageError::validation(
                                "Each TransactWriteItem must contain exactly one of Put, Update, \
                                 Delete, or ConditionCheck",
                            ));
                        }

                        if let Some(put) = item.put {
                            self.transact_put_with_client(&tx, put, Some(index))
                                .await
                                .map_err(|error| {
                                    transaction_canceled_for_item_error_with_len(
                                        index, item_count, error,
                                    )
                                })?;
                            continue;
                        }

                        if let Some(update) = item.update {
                            self.transact_update_with_client(&tx, update, Some(index))
                                .await
                                .map_err(|error| {
                                    transaction_canceled_for_item_error_with_len(
                                        index, item_count, error,
                                    )
                                })?;
                            continue;
                        }

                        if let Some(delete) = item.delete {
                            self.transact_delete_with_client(&tx, delete, Some(index))
                                .await
                                .map_err(|error| {
                                    transaction_canceled_for_item_error_with_len(
                                        index, item_count, error,
                                    )
                                })?;
                            continue;
                        }

                        let Some(condition_check) = item.condition_check else {
                            return Err(StorageError::validation(
                                "Each TransactWriteItem must contain exactly one operation",
                            ));
                        };
                        self.transact_condition_check_with_client(
                            &tx,
                            condition_check,
                            Some(index),
                        )
                        .await
                        .map_err(|error| {
                            transaction_canceled_for_item_error_with_len(index, item_count, error)
                        })?;
                    }

                    tx.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit transact write transaction", err)
                    })?;

                    Ok(storage_types::TransactWriteItemsResponse {
                        consumed_capacity: None,
                        item_collection_metrics: None,
                    })
                }
            })
            .await?;
        let applied_items =
            billed_tally.put_ops + billed_tally.delete_ops + billed_tally.update_ops;
        let applied_bytes =
            billed_tally.put_bytes + billed_tally.delete_bytes + billed_tally.update_bytes;
        record_write(applied_items, applied_bytes as usize);
        billed_tally.emit("transact_write_items");
        Ok(response)
    }

    async fn preflight_transact_item_key(
        &self,
        item: &storage_types::TransactWriteItem,
    ) -> StorageResult<TransactionKeyPreflight> {
        let Some(table_name) = transact_item_table_name(item) else {
            return Ok(TransactionKeyPreflight::default());
        };
        let table_info = self.get_table_info_cached_arc(table_name).await?;
        preflight_transact_item_key_with_table_info(item, &table_info)
    }
}
