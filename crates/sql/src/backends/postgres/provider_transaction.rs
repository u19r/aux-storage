use std::sync::Arc;

use storage_types::{StorageError, StorageResult, StoredTableInfo, TableName};

use crate::{
    backends::postgres::{PostgresStorageProvider, record_write},
    billing_metrics::WriteCostTally,
    provider_core::transaction::{
        TransactionKeyPreflight, preflight_transact_item_key_with_table_info,
        transact_item_table_name, transaction_canceled_for_indexed_reasons,
        transaction_canceled_for_preflights, transaction_cancellation_reason_at,
        validate_no_duplicate_transact_item_keys,
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
                    let mut client = self.acquire_client("transact_write_items").await?;
                    let _connection_hold = self.connection_hold_timer("transact_write_items");
                    let tx = self
                        .begin_transaction(
                            &mut client,
                            "transact_write_items",
                            "start transact write transaction",
                        )
                        .await?;
                    let _transaction_hold = self.transaction_hold_timer("transact_write_items");

                    let mut preflights = Vec::with_capacity(request.transact_items.len());
                    if let Some(table_name) = single_transact_table_name(&request.transact_items) {
                        let table_info = self.get_table_info_cached_arc(table_name).await?;
                        for item in &request.transact_items {
                            preflights.push(preflight_transact_item_key_with_table_info(
                                item,
                                &table_info,
                            )?);
                        }
                    } else {
                        let mut table_infos = Vec::new();
                        for item in &request.transact_items {
                            preflights.push(
                                self.preflight_transact_item_key_with_request_cache(
                                    item,
                                    &mut table_infos,
                                )
                                .await?,
                            );
                        }
                    }
                    if let Some(error) = transaction_canceled_for_preflights(&preflights) {
                        return Err(error);
                    }
                    validate_no_duplicate_transact_item_keys(&preflights)?;

                    let item_count = request.transact_items.len();
                    let mut cancellation_reasons = vec![None; item_count];
                    for (index, item) in request.transact_items.into_iter().enumerate() {
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

                        let result = if let Some(put) = item.put {
                            self.transact_put_with_client(&tx, put, Some(index)).await
                        } else if let Some(update) = item.update {
                            self.transact_update_with_client(&tx, update, Some(index))
                                .await
                        } else if let Some(delete) = item.delete {
                            self.transact_delete_with_client(&tx, delete, Some(index))
                                .await
                        } else {
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
                        };
                        if let Err(error) = result {
                            let Some(reason) = transaction_cancellation_reason_at(&error, index)
                            else {
                                return Err(error);
                            };
                            cancellation_reasons[index] = Some(reason);
                        }
                    }
                    if let Some(error) =
                        transaction_canceled_for_indexed_reasons(cancellation_reasons)
                    {
                        return Err(error);
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

    async fn preflight_transact_item_key_with_request_cache(
        &self,
        item: &storage_types::TransactWriteItem,
        table_infos: &mut Vec<(TableName, Arc<StoredTableInfo>)>,
    ) -> StorageResult<TransactionKeyPreflight> {
        let Some(table_name) = transact_item_table_name(item) else {
            return Ok(TransactionKeyPreflight::default());
        };
        let table_info = if let Some((_, table_info)) = table_infos
            .iter()
            .find(|(cached_table, _)| cached_table == table_name)
        {
            Arc::clone(table_info)
        } else {
            let table_info = self.get_table_info_cached_arc(table_name).await?;
            table_infos.push((table_name.clone(), Arc::clone(&table_info)));
            table_info
        };
        preflight_transact_item_key_with_table_info(item, &table_info)
    }
}

fn single_transact_table_name(items: &[storage_types::TransactWriteItem]) -> Option<&TableName> {
    let mut table_name = None;
    for item in items {
        let item_table_name = transact_item_table_name(item)?;
        match table_name {
            Some(table_name) if table_name != item_table_name => return None,
            Some(_) => {}
            None => table_name = Some(item_table_name),
        }
    }
    table_name
}
