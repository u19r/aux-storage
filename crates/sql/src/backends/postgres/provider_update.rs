use std::time::Instant;

use storage_condition::evaluate_condition;
use storage_provider::{
    before_update_item_optional, return_values_need_old_item, update_item_response,
};
use storage_types::{StorageResult, UpdateItemRequest, UpdateItemResponse};

use crate::{
    backends::postgres::{
        PostgresStorageProvider, record_write, transaction_helpers::PostgresUpsertTransactItemInput,
    },
    billing_metrics::{record_write_cost, serializable_payload_bytes},
    provider_core::write::apply_update_to_existing_or_key,
};

impl PostgresStorageProvider {
    pub(super) async fn do_update_item(
        &self,
        request: UpdateItemRequest,
    ) -> StorageResult<UpdateItemResponse> {
        let billed_bytes = serializable_payload_bytes(&request);
        let UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_values_on_condition_check_failure,
            aux_item_stream_ttl_hours,
            indexers,
            ..
        } = request;

        let (operations, condition) = before_update_item_optional(
            update_expression.as_deref(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let table_info = self.get_table_info_cached_arc(&table_name).await?;
        let return_old_on_condition_failure =
            storage_types::return_values_on_condition_check_failure_all_old(
                return_values_on_condition_check_failure.as_ref(),
            );
        let key_attributes = key.clone();
        let response = self
            .retry_postgres_conflicts("update_item", || {
                let table_name = table_name.clone();
                let key = key.clone();
                let key_attributes = key_attributes.clone();
                let operations = operations.clone();
                let condition = condition.clone();
                let return_values = return_values.clone();
                let table_info = table_info.clone();
                let indexers = indexers.clone();
                async move {
                    let mut client = self.acquire_client("update_item").await?;
                    let _connection_hold = self.connection_hold_timer("update_item");
                    let transaction = self
                        .begin_transaction(
                            &mut client,
                            "update_item",
                            "start update_item transaction",
                        )
                        .await?;
                    let _transaction_hold = self.transaction_hold_timer("update_item");

                    let old_item_started = Instant::now();
                    let prepared =
                        Self::prepare_get_item_query(&table_name, &key_attributes, &table_info)?;
                    let existing = self
                        .execute_prepared_get_item_query_with_indexers(
                            &transaction,
                            &prepared,
                            "update_item",
                            "old_item_query",
                        )
                        .await?;
                    let (existing_item, stored_indexers) = match existing {
                        Some(item) => (Some(item.item.into_attribute_map()?), item.indexers),
                        None => (None, Vec::new()),
                    };
                    let effective_indexers =
                        indexers.as_deref().unwrap_or(stored_indexers.as_slice());
                    self.record_transaction_phase(
                        "update_item",
                        "old_item_read",
                        old_item_started.elapsed(),
                    );
                    let update_plan_started = Instant::now();
                    if let Some(condition) = condition.as_ref() {
                        let empty_item = std::collections::HashMap::new();
                        let condition_item = existing_item.as_ref().unwrap_or(&empty_item);
                        if !evaluate_condition(condition_item, condition) {
                            return Err(crate::provider_core::write::conditional_failure(
                                existing_item.as_ref(),
                                return_old_on_condition_failure,
                            ));
                        }
                    }
                    let old_item_for_write = existing_item.clone();
                    let (old_item, updated_item) =
                        apply_update_to_existing_or_key(existing_item, &key, &operations)?;
                    let old_item_for_response = return_values_need_old_item(return_values.as_ref())
                        .then(|| old_item.clone());
                    self.record_transaction_phase(
                        "update_item",
                        "update_plan",
                        update_plan_started.elapsed(),
                    );
                    let transact_put_started = Instant::now();
                    self.upsert_transact_item_with_client(
                        &transaction,
                        PostgresUpsertTransactItemInput {
                            table_name: &table_name,
                            table_info: &table_info,
                            item: updated_item.clone(),
                            indexers: effective_indexers,
                            old_item: old_item_for_write.as_ref(),
                            old_indexers: old_item_for_write
                                .as_ref()
                                .map(|_| stored_indexers.as_slice()),
                            item_stream_ttl_hours: aux_item_stream_ttl_hours,
                        },
                    )
                    .await?;
                    self.record_transaction_phase(
                        "update_item",
                        "transact_put",
                        transact_put_started.elapsed(),
                    );
                    let commit_started = Instant::now();
                    transaction.commit().await.map_err(|err| {
                        Self::map_postgres_write_error("commit update_item transaction", err)
                    })?;
                    self.record_transaction_phase(
                        "update_item",
                        "tx_commit",
                        commit_started.elapsed(),
                    );

                    let return_values_started = Instant::now();
                    let response = update_item_response(
                        &operations,
                        old_item_for_response,
                        Some(updated_item),
                        return_values.as_ref(),
                    );
                    self.record_transaction_phase(
                        "update_item",
                        "return_values",
                        return_values_started.elapsed(),
                    );
                    response
                }
            })
            .await?;
        record_write(1, 0);
        record_write_cost("update_item", "update", 1, billed_bytes);
        Ok(response)
    }
}
