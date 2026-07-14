use std::collections::HashMap;

use storage_cache::RuntimePreparedQueryExecution;
use storage_types::{
    AttributeValue, ItemKey, KeyAttributes, KeySchemaElement, QueryTableRequest, StorageError,
    StorageResult, TableNamespace, TryFromWireItem, WireItem, validate_expression_attribute_usage,
};

use crate::{
    QueryIndexInput, QueryTableInput,
    database_manager::{
        DatabaseManager, decode_wire_items_to_decoded, decode_wire_items_to_maps,
        normalize_wire_items_for_shared_table, record_storage_operation,
    },
    namespace_routing::NamespaceStorageMode,
    newtypes::DatabaseTrait,
};

impl DatabaseManager {
    pub async fn query_table_map(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = self.query_table_request(input.into()).await?;
        Ok((decode_wire_items_to_maps(items)?, lek))
    }

    pub async fn query_table(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_request(input.into()).await
    }

    pub async fn query_table_decode<T>(
        &self,
        input: QueryTableInput,
    ) -> StorageResult<(Vec<T>, Option<String>)>
    where
        T: TryFromWireItem,
    {
        let (items, lek) = self.query_table(input).await?;
        Ok((decode_wire_items_to_decoded(items)?, lek))
    }

    pub async fn query_index_map(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = self.query_table_request(input.into()).await?;
        Ok((decode_wire_items_to_maps(items)?, lek))
    }

    pub async fn query_index(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.query_table_request(input.into()).await
    }

    pub async fn query_index_decode<T>(
        &self,
        input: QueryIndexInput,
    ) -> StorageResult<(Vec<T>, Option<String>)>
    where
        T: TryFromWireItem,
    {
        let (items, lek) = self.query_index(input).await?;
        Ok((decode_wire_items_to_decoded(items)?, lek))
    }

    async fn query_table_request(
        &self,
        request: QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let logical_request = request.clone();
        let cache_query_runtime = self.cache_query_runtime();
        match cache_query_runtime
            .prepare_query_execution(&logical_request)
            .await?
        {
            RuntimePreparedQueryExecution::WholePage {
                items,
                last_evaluated_key,
                ..
            } => return Ok((items, last_evaluated_key)),
            RuntimePreparedQueryExecution::PrefixWithDbSuffix {
                prefix_items,
                resume_token,
                remaining_limit,
            } => {
                let mut suffix_request = logical_request.clone();
                suffix_request.exclusive_start_key = Some(resume_token);
                suffix_request.limit = remaining_limit;
                let (mut suffix_items, suffix_lek) =
                    self.execute_query_table_db_only(suffix_request).await?;
                let mut merged_items = prefix_items;
                merged_items.append(&mut suffix_items);
                cache_query_runtime.record_partial_hit();
                return Ok((merged_items, suffix_lek));
            }
            RuntimePreparedQueryExecution::PrefixOnly { items } => return Ok((items, None)),
            RuntimePreparedQueryExecution::None => {}
        }
        cache_query_runtime.record_miss_if_eventual(request.consistent_read);
        self.execute_query_table_db_only(logical_request).await
    }

    async fn execute_query_table_db_only(
        &self,
        mut request: QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        validate_expression_attribute_usage(
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            std::iter::once(request.key_condition_expression.as_str()),
        )?;
        let logical_request = request.clone();
        let mut provider: std::sync::Arc<dyn DatabaseTrait> = std::sync::Arc::clone(&self.storage);
        let mut normalize_namespace: Option<TableNamespace> = None;

        if let Some(route) = self
            .resolve_namespace_route_for_table(&request.table_name)
            .await?
        {
            if route.storage_mode == NamespaceStorageMode::SharedTable {
                self.request_rewriter
                    .rewrite_query_for_shared_table(&route.namespace, &mut request)?;
                self.rewrite_query_start_key_for_shared_table(&route.namespace, &mut request)
                    .await?;
                normalize_namespace = Some(route.namespace.clone());
            }
            request.table_name = route.read_target.table_name.clone();
            provider = self.provider_for_connection(&route.read_target.connection_id)?;
        }

        if let Some(index_name) = request.index_name.as_ref() {
            let table_info = record_storage_operation(
                "get_table_info",
                provider.get_table_info(&request.table_name),
            )
            .await?;
            let has_index = table_info
                .global_secondary_indexes
                .as_ref()
                .is_some_and(|indexes| indexes.iter().any(|index| index.index_name == *index_name));
            if !has_index {
                return Err(StorageError::validation(format!(
                    "One or more parameter values were invalid: The table does not have the \
                     specified index: {index_name}"
                )));
            }
        }

        let (mut items, mut lek) =
            record_storage_operation("query_table", provider.query_table(&request)).await?;
        if let Some(namespace) = normalize_namespace.as_ref() {
            normalize_wire_items_for_shared_table(&self.request_rewriter, namespace, &mut items)?;
            lek = self
                .normalize_query_start_key_from_shared_table(namespace, &logical_request, lek)
                .await?;
        }
        self.cache_query_runtime()
            .observe_db_query_result(&logical_request, &items, lek.is_some())
            .await?;
        Ok((items, lek))
    }

    pub(super) async fn rewrite_query_start_key_for_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &mut QueryTableRequest,
    ) -> StorageResult<()> {
        let Some(token) = request.exclusive_start_key.as_deref() else {
            return Ok(());
        };
        if request.index_name.is_some() {
            return Ok(());
        }

        let table_info = self.get_table_info_arc(&request.table_name).await?;
        let Some(item_key) = ItemKey::item_key_from_next_page_token(token, &table_info, &None)
            .map_err(|err| {
                StorageError::validation(format!("invalid shared-table query start key: {err}"))
            })?
        else {
            request.exclusive_start_key = None;
            return Ok(());
        };
        let mut key = KeyAttributes::from(item_key_to_attribute_map(
            &table_info.key_schema,
            &item_key,
        )?);
        self.request_rewriter
            .rewrite_key_for_shared_table(namespace, &mut key)?;
        let rewritten_item_key =
            ItemKey::from_key_schema(request.table_name.clone(), &table_info.key_schema, &key)
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "rewrite shared-table query start key from logical key: {err}"
                    ))
                })?;
        request.exclusive_start_key =
            Some(rewritten_item_key.next_page_token().map_err(|err| {
                StorageError::internal(&format!(
                    "encode rewritten shared-table query start key: {err}"
                ))
            })?);
        Ok(())
    }

    pub(super) async fn normalize_query_start_key_from_shared_table(
        &self,
        namespace: &TableNamespace,
        request: &QueryTableRequest,
        token: Option<String>,
    ) -> StorageResult<Option<String>> {
        let Some(token) = token else {
            return Ok(None);
        };
        if request.index_name.is_some() {
            return Ok(Some(token));
        }

        let table_info = self.get_table_info_arc(&request.table_name).await?;
        let Some(item_key) = ItemKey::item_key_from_next_page_token(&token, &table_info, &None)
            .map_err(|err| {
                StorageError::validation(format!("invalid shared-table query next token: {err}"))
            })?
        else {
            return Ok(None);
        };
        let mut key = KeyAttributes::from(item_key_to_attribute_map(
            &table_info.key_schema,
            &item_key,
        )?);
        self.request_rewriter
            .normalize_key_from_shared_table(namespace, &mut key)?;
        let normalized_item_key =
            ItemKey::from_key_schema(request.table_name.clone(), &table_info.key_schema, &key)
                .map_err(|err| {
                    StorageError::internal(&format!(
                        "normalize shared-table query next token to logical key: {err}"
                    ))
                })?;
        Ok(Some(normalized_item_key.next_page_token().map_err(
            |err| {
                StorageError::internal(&format!(
                    "encode normalized shared-table query next token: {err}"
                ))
            },
        )?))
    }
}

pub(crate) fn item_key_to_attribute_map(
    key_schema: &[KeySchemaElement],
    item_key: &ItemKey,
) -> StorageResult<HashMap<String, AttributeValue>> {
    let mut key = HashMap::new();
    for element in key_schema {
        match element.key_type {
            storage_types::KeyType::Hash => {
                key.insert(element.attribute_name.clone(), item_key.hash_key().clone());
            }
            storage_types::KeyType::Range => {
                let Some(range_key) = item_key.range_key().cloned() else {
                    return Err(StorageError::invalid_or_missing_key());
                };
                key.insert(element.attribute_name.clone(), range_key);
            }
        }
    }
    Ok(key)
}
