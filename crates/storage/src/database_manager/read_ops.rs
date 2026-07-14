use std::collections::HashMap;

use storage_cache::batch_request_has_items;
use storage_provider::StorageProvider;
use storage_types::{
    AttributeValue, BatchGetItemRequest, BatchGetWireItemResponse, DurableBatchPointReadProof,
    DurableBatchPointReadProofEntry, DurablePointReadProof, DurablePointReadRequest,
    GetStreamRecordsResponse, KeyAttributes, KeySchemaElement, KeysAndAttributes, ScanTableRequest,
    StorageEnum, StorageError, StorageResult, StreamItemId, StreamName, StreamSpecification,
    StreamViewType, TableName, TableNamespace, TryFromWireItem, WireItem, context::WrappedError,
};
use stream::StreamError;

use crate::{
    ScanTableInput,
    cache_batch_get_runtime::{request_contains_consistent_reads, strong_only_batch_get_request},
    database_manager::{
        DatabaseManager, ROUTED_DEFAULT_CONNECTION_ID, decode_wire_items_to_decoded,
        decode_wire_items_to_maps, normalize_wire_items_for_shared_table, record_storage_operation,
    },
    namespace_routing::{NamespaceRequestRewriter, NamespaceStorageMode},
};

impl DatabaseManager {
    pub async fn get_item_map<K>(
        &self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        K: Into<KeyAttributes>,
    {
        let item = self.get_item(table_name, key).await?;
        item.map(WireItem::into_attribute_map).transpose()
    }

    pub async fn get_item<K>(
        &self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<WireItem>>
    where
        K: Into<KeyAttributes>,
    {
        self.get_item_with_consistent_read(table_name, key, true)
            .await
    }

    pub async fn get_item_with_consistent_read<K>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>>
    where
        K: Into<KeyAttributes>,
    {
        let mut key = key.into();
        let point_read_runtime = self.cache_point_read_runtime();
        let prepared_cache_read = point_read_runtime
            .prepare_get(&table_name, &key, consistent_read)
            .await?;
        if let Some(item) = prepared_cache_read.cache_hit() {
            return Ok(item);
        }

        let table_info = self.get_table_info_arc(&table_name).await?;
        storage_types::validate_key_attributes_for_schema(&table_info.key_schema, &key)?;

        if let Some(route) = self.resolve_namespace_route_for_table(&table_name).await? {
            if route.storage_mode == NamespaceStorageMode::SharedTable {
                self.request_rewriter
                    .rewrite_key_for_shared_table(&route.namespace, &mut key)?;
            }
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            let mut item = if should_try_strong_read_through_warming(
                &point_read_runtime,
                &prepared_cache_read,
                consistent_read,
            ) && route.storage_mode != NamespaceStorageMode::SharedTable
            {
                match record_storage_operation(
                    "get_item_with_durable_proof",
                    provider.get_item_with_durable_proof(DurablePointReadRequest {
                        table_name: route.read_target.table_name.clone(),
                        key: key.clone(),
                        consistent_read,
                    }),
                )
                .await
                {
                    Ok(proof) => {
                        item_from_durable_proof_for_cache_warming(
                            &point_read_runtime,
                            &prepared_cache_read,
                            proof,
                        )
                        .await?
                    }
                    Err(error) if matches!(error.to_enum(), StorageEnum::Unsupported { .. }) => {
                        record_storage_operation(
                            "get_item",
                            provider.get_item(
                                route.read_target.table_name.clone(),
                                key,
                                consistent_read,
                            ),
                        )
                        .await?
                    }
                    Err(error) => return Err(error),
                }
            } else {
                record_storage_operation(
                    "get_item",
                    provider.get_item(route.read_target.table_name.clone(), key, consistent_read),
                )
                .await?
            };
            if route.storage_mode == NamespaceStorageMode::SharedTable
                && let Some(item_ref) = item.as_mut()
            {
                self.request_rewriter
                    .normalize_wire_item_from_shared_table(&route.namespace, item_ref)?;
            }
            prepared_cache_read.record_db_miss();
            return Ok(item);
        }

        let result = if should_try_strong_read_through_warming(
            &point_read_runtime,
            &prepared_cache_read,
            consistent_read,
        ) {
            match record_storage_operation(
                "get_item_with_durable_proof",
                self.storage
                    .get_item_with_durable_proof(DurablePointReadRequest {
                        table_name: table_name.clone(),
                        key: key.clone(),
                        consistent_read,
                    }),
            )
            .await
            {
                Ok(proof) => {
                    item_from_durable_proof_for_cache_warming(
                        &point_read_runtime,
                        &prepared_cache_read,
                        proof,
                    )
                    .await?
                }
                Err(error) if matches!(error.to_enum(), StorageEnum::Unsupported { .. }) => {
                    record_storage_operation(
                        "get_item",
                        self.storage.get_item(table_name, key, consistent_read),
                    )
                    .await?
                }
                Err(error) => return Err(error),
            }
        } else {
            record_storage_operation(
                "get_item",
                self.storage.get_item(table_name, key, consistent_read),
            )
            .await?
        };
        prepared_cache_read.record_db_miss();
        Ok(result)
    }

    pub async fn get_item_decode<K, T>(
        &self,
        table_name: TableName,
        key: K,
    ) -> StorageResult<Option<T>>
    where
        K: Into<KeyAttributes>,
        T: TryFromWireItem,
    {
        self.get_item_decode_with_consistent_read(table_name, key, true)
            .await
    }

    pub async fn get_item_decode_with_consistent_read<K, T>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<T>>
    where
        K: Into<KeyAttributes>,
        T: TryFromWireItem,
    {
        let item = self
            .get_item_with_consistent_read(table_name, key, consistent_read)
            .await?;
        item.as_ref().map(T::try_from_wire_item).transpose()
    }

    pub async fn get_item_map_with_consistent_read<K>(
        &self,
        table_name: TableName,
        key: K,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>>
    where
        K: Into<KeyAttributes>,
    {
        let item = self
            .get_item_with_consistent_read(table_name, key, consistent_read)
            .await?;
        item.map(WireItem::into_attribute_map).transpose()
    }

    pub async fn scan_table_map(
        &self,
        input: ScanTableInput,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = self.scan_table(input).await?;
        Ok((decode_wire_items_to_maps(items)?, lek))
    }

    pub async fn scan_table(
        &self,
        input: ScanTableInput,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let mut request = ScanTableRequest {
            table_name: input.table_name,
            index_name: input.index_name,
            limit: input.limit,
            exclusive_start_key: input.exclusive_start_key,
            consistent_read: input.consistent_read,
        };

        if let Some(route) = self
            .resolve_namespace_route_for_table(&request.table_name)
            .await?
        {
            if route.storage_mode == NamespaceStorageMode::SharedTable {
                self.request_rewriter
                    .rewrite_scan_for_shared_table(&route.namespace, &mut request)?;
            }
            request.table_name = route.read_target.table_name.clone();
            let provider = self.provider_for_connection(&route.read_target.connection_id)?;
            return record_storage_operation("scan_table", provider.scan_table(&request)).await;
        }

        record_storage_operation("scan_table", self.storage.scan_table(&request)).await
    }

    pub async fn scan_table_decode<T>(
        &self,
        input: ScanTableInput,
    ) -> StorageResult<(Vec<T>, Option<String>)>
    where
        T: TryFromWireItem,
    {
        let (items, lek) = self.scan_table(input).await?;
        Ok((decode_wire_items_to_decoded(items)?, lek))
    }

    pub async fn get_stream_records(
        &self,
        table_name: &TableName,
        key_schema: &[KeySchemaElement],
        stream_spec: &StreamSpecification,
        page_token: Option<&str>,
        limit: Option<u32>,
    ) -> StorageResult<GetStreamRecordsResponse> {
        let table_stream_name = StreamName::table_stream(table_name);
        let page_token = parse_stream_page_token(page_token)?;

        let (stream_items, last_evaluated_key) = self
            .storage
            .get_stream_records_from_pointer_stream(
                table_stream_name,
                key_schema,
                page_token,
                limit,
            )
            .await
            .map_err(StreamError::into_storage_enum)?;

        let stream_items_response = stream_items.into_iter().map(|mut item| {
            if let Some(stream_view_type) = &stream_spec.stream_view_type {
                match stream_view_type {
                    StreamViewType::KeysOnly => {
                        item.new_image = None;
                        item.old_image = None;
                    }
                    StreamViewType::NewImage => {
                        item.old_image = None;
                    }
                    StreamViewType::OldImage => {
                        item.new_image = None;
                    }
                    StreamViewType::NewAndOldImages => {}
                }
            }
            item
        });

        Ok(GetStreamRecordsResponse {
            table_name: table_name.clone(),
            records: stream_items_response.collect(),
            last_evaluated_key: last_evaluated_key.map(|key| key.to_string()),
        })
    }

    pub async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let batch_get_cache = self.cache_batch_get_runtime();
        let prepared_cache_read = batch_get_cache.prepare(request).await?;
        let db_request = prepared_cache_read.db_request;
        let cache_outcome = prepared_cache_read.cache_outcome;

        if self.route_resolver.is_none() {
            let mut response = if batch_request_has_items(&db_request) {
                warm_strong_batch_read_through(
                    &batch_get_cache,
                    self.storage.as_ref(),
                    &db_request,
                    None,
                )
                .await?;
                record_storage_operation(
                    "batch_get_item",
                    self.storage.batch_get_item(db_request.clone()),
                )
                .await?
            } else {
                BatchGetWireItemResponse::default()
            };
            batch_get_cache
                .merge_cached_responses(&mut response, prepared_cache_read.cached_responses);
            batch_get_cache.record_outcome(cache_outcome);
            return Ok(response);
        }

        let BatchGetItemRequest {
            request_items,
            return_consumed_capacity,
        } = db_request.clone();

        let default_connection_id = ROUTED_DEFAULT_CONNECTION_ID.to_string();
        let mut routed_requests = Vec::with_capacity(request_items.len());

        for (logical_table, mut keys_and_attributes) in request_items {
            let route = self
                .resolve_namespace_route_for_table(&logical_table)
                .await?;
            let (connection_id, physical_table, shared_namespace) = if let Some(route) = route {
                if route.storage_mode == NamespaceStorageMode::SharedTable {
                    for key in &mut keys_and_attributes.keys {
                        self.request_rewriter
                            .rewrite_key_for_shared_table(&route.namespace, key)?;
                    }
                }
                (
                    route.read_target.connection_id.clone(),
                    route.read_target.table_name.clone(),
                    (route.storage_mode == NamespaceStorageMode::SharedTable)
                        .then_some(route.namespace.clone()),
                )
            } else {
                (default_connection_id.clone(), logical_table.clone(), None)
            };
            routed_requests.push((
                connection_id,
                logical_table,
                shared_namespace,
                BatchGetItemRequest {
                    request_items: HashMap::from([(physical_table, keys_and_attributes)]),
                    return_consumed_capacity: return_consumed_capacity.clone(),
                },
            ));
        }

        let mut merged_responses: HashMap<TableName, Vec<WireItem>> =
            HashMap::with_capacity(routed_requests.len());
        let mut merged_unprocessed = HashMap::new();
        for (connection_id, logical_table, shared_namespace, request) in routed_requests {
            let provider = self.provider_for_request_connection(&connection_id)?;
            if connection_id == default_connection_id {
                warm_strong_batch_read_through(
                    &batch_get_cache,
                    provider.as_ref(),
                    &request,
                    Some(RoutedBatchProofTarget {
                        logical_table: &logical_table,
                        shared_namespace: shared_namespace.as_ref(),
                        request_rewriter: &self.request_rewriter,
                    }),
                )
                .await?;
            }
            let response =
                record_storage_operation("batch_get_item", provider.batch_get_item(request))
                    .await?;
            if let Some(responses) = response.responses {
                for (_, items) in responses {
                    let mut items = items;
                    if let Some(namespace) = shared_namespace.as_ref() {
                        normalize_wire_items_for_shared_table(
                            &self.request_rewriter,
                            namespace,
                            &mut items,
                        )?;
                    }
                    merged_responses
                        .entry(logical_table.clone())
                        .or_default()
                        .extend(items);
                }
            }
            if let Some(unprocessed_keys) = response.unprocessed_keys {
                for (_, keys) in unprocessed_keys {
                    let mut keys = keys;
                    if let Some(namespace) = shared_namespace.as_ref() {
                        normalize_unprocessed_keys_for_shared_table(
                            &self.request_rewriter,
                            namespace,
                            &mut keys,
                        )?;
                    }
                    merged_unprocessed.insert(logical_table.clone(), keys);
                }
            }
        }

        let response = BatchGetWireItemResponse {
            responses: (!merged_responses.is_empty()).then_some(merged_responses),
            unprocessed_keys: (!merged_unprocessed.is_empty()).then_some(merged_unprocessed),
            consumed_capacity: None,
        };
        let mut response = response;
        batch_get_cache.merge_cached_responses(&mut response, prepared_cache_read.cached_responses);
        batch_get_cache.record_outcome(cache_outcome);
        Ok(response)
    }

    pub async fn get_stream_records_for_table_name(
        &self,
        table_name: &TableName,
        page_token: Option<&str>,
        limit: Option<u32>,
    ) -> StorageResult<GetStreamRecordsResponse> {
        let table_info = self.get_table_info_arc(table_name).await?;
        let key_schema = table_info.key_schema.clone();

        let stream_spec = StreamSpecification {
            stream_enabled: true,
            stream_view_type: Some(StreamViewType::NewAndOldImages),
        };

        self.get_stream_records(table_name, &key_schema, &stream_spec, page_token, limit)
            .await
    }
}

fn should_try_strong_read_through_warming(
    point_read_runtime: &crate::cache_point_read_runtime::StoragePointReadCacheRuntime<'_>,
    prepared_cache_read: &crate::cache_point_read_runtime::PreparedPointReadCacheRead,
    consistent_read: bool,
) -> bool {
    consistent_read
        && point_read_runtime.strong_read_through_warming_enabled()
        && prepared_cache_read.request().is_some()
}

async fn item_from_durable_proof_for_cache_warming(
    point_read_runtime: &crate::cache_point_read_runtime::StoragePointReadCacheRuntime<'_>,
    prepared_cache_read: &crate::cache_point_read_runtime::PreparedPointReadCacheRead,
    proof: DurablePointReadProof,
) -> StorageResult<Option<WireItem>> {
    let item = match &proof {
        DurablePointReadProof::Present { item, .. } => Some((**item).clone()),
        DurablePointReadProof::Absent { .. } => None,
    };
    if let Some(request) = prepared_cache_read.request() {
        let _ = point_read_runtime
            .warm_authoritative_read(request, proof)
            .await;
    }
    Ok(item)
}

async fn warm_strong_batch_read_through(
    batch_get_cache: &crate::cache_batch_get_runtime::StorageBatchGetCacheRuntime<'_>,
    provider: &dyn StorageProvider,
    db_request: &BatchGetItemRequest,
    target: Option<RoutedBatchProofTarget<'_>>,
) -> StorageResult<()> {
    if !batch_get_cache.strong_read_through_warming_enabled()
        || !request_contains_consistent_reads(db_request)
    {
        return Ok(());
    }

    let Some(strong_request) = strong_only_batch_get_request(db_request) else {
        return Ok(());
    };
    let mut proof = match provider
        .batch_get_item_with_durable_proofs(storage_types::DurableBatchPointReadRequest {
            request_items: strong_request.request_items,
        })
        .await
    {
        Ok(proof) => proof,
        Err(error) if matches!(error.to_enum(), StorageEnum::Unsupported { .. }) => return Ok(()),
        Err(error) => return Err(error),
    };
    if let Some(target) = target {
        proof = remap_routed_batch_proof(proof, target)?;
    }
    batch_get_cache.warm_authoritative_batch(proof).await
}

pub(crate) struct RoutedBatchProofTarget<'a> {
    pub(crate) logical_table: &'a TableName,
    pub(crate) shared_namespace: Option<&'a TableNamespace>,
    pub(crate) request_rewriter: &'a NamespaceRequestRewriter,
}

pub(crate) fn remap_routed_batch_proof(
    proof: DurableBatchPointReadProof,
    target: RoutedBatchProofTarget<'_>,
) -> StorageResult<DurableBatchPointReadProof> {
    let DurableBatchPointReadProof {
        responses,
        unprocessed_keys,
    } = proof;
    let mut entries = responses.into_values().flatten().collect::<Vec<_>>();
    for entry in &mut entries {
        normalize_durable_proof_entry(entry, &target)?;
    }
    let mut unprocessed_values = unprocessed_keys.into_values();
    let mut unprocessed = unprocessed_values.next();
    if unprocessed_values.next().is_some() {
        return Err(StorageError::internal(
            "single-table routed batch proof returned multiple unprocessed tables",
        ));
    }
    if let (Some(namespace), Some(keys)) = (target.shared_namespace, unprocessed.as_mut()) {
        for key in &mut keys.keys {
            target
                .request_rewriter
                .normalize_key_from_shared_table(namespace, key)?;
        }
    }
    Ok(DurableBatchPointReadProof {
        responses: if !entries.is_empty() {
            HashMap::from([(target.logical_table.clone(), entries)])
        } else {
            Default::default()
        },
        unprocessed_keys: unprocessed
            .map(|keys| HashMap::from([(target.logical_table.clone(), keys)]))
            .unwrap_or_default(),
    })
}

fn normalize_durable_proof_entry(
    entry: &mut DurableBatchPointReadProofEntry,
    target: &RoutedBatchProofTarget<'_>,
) -> StorageResult<()> {
    let Some(namespace) = target.shared_namespace else {
        return Ok(());
    };
    target
        .request_rewriter
        .normalize_key_from_shared_table(namespace, &mut entry.key)?;
    if let DurablePointReadProof::Present { item, .. } = &mut entry.proof {
        target
            .request_rewriter
            .normalize_wire_item_from_shared_table(namespace, item)?;
    }
    Ok(())
}

pub(crate) fn parse_stream_page_token(
    page_token: Option<&str>,
) -> StorageResult<Option<StreamItemId>> {
    page_token
        .map(|token| {
            token
                .parse::<StreamItemId>()
                .map_err(|_| StorageError::internal("Invalid page token"))
        })
        .transpose()
}

pub(crate) fn normalize_unprocessed_keys_for_shared_table(
    rewriter: &NamespaceRequestRewriter,
    namespace: &TableNamespace,
    keys: &mut KeysAndAttributes,
) -> StorageResult<()> {
    for key in &mut keys.keys {
        rewriter.normalize_key_from_shared_table(namespace, key)?;
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn storage_api_projection(
    items: &[HashMap<String, AttributeValue>],
    projection_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> Vec<HashMap<String, AttributeValue>> {
    let path_count = projection_expr
        .as_bytes()
        .iter()
        .filter(|byte| **byte == b',')
        .count()
        + 1;
    let mut paths = Vec::with_capacity(path_count);
    for path in projection_expr.split(',').map(str::trim) {
        if path.is_empty() {
            continue;
        }
        if let Some(path) = parse_projection_path(path, expression_attribute_names) {
            paths.push(path);
        }
    }

    let mut projected_items = Vec::with_capacity(items.len());
    for item in items {
        projected_items.push(project_item_with_paths(item, &paths, path_count));
    }
    projected_items
}

pub(super) fn storage_api_project_item(
    item: &HashMap<String, AttributeValue>,
    projection_expr: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> HashMap<String, AttributeValue> {
    let path_count = projection_expr
        .as_bytes()
        .iter()
        .filter(|byte| **byte == b',')
        .count()
        + 1;
    let mut paths = Vec::with_capacity(path_count);
    for path in projection_expr.split(',').map(str::trim) {
        if path.is_empty() {
            continue;
        }
        if let Some(path) = parse_projection_path(path, expression_attribute_names) {
            paths.push(path);
        }
    }
    project_item_with_paths(item, &paths, path_count)
}

fn project_item_with_paths(
    item: &HashMap<String, AttributeValue>,
    paths: &[Vec<ProjectionSegment>],
    path_count: usize,
) -> HashMap<String, AttributeValue> {
    let mut projected_item = ProjectedValue::Map(HashMap::with_capacity(path_count));
    for path in paths {
        if let Some(value) = get_projection_path_value(item, path) {
            insert_projected_value(&mut projected_item, path, value.clone());
        }
    }
    projected_item.into_attribute_map().unwrap_or_default()
}

#[derive(Clone)]
enum ProjectionSegment {
    Key(String),
    Index(usize),
}

enum ProjectedValue {
    Map(HashMap<String, AttributeValue>),
    List(Vec<Option<AttributeValue>>),
}

fn parse_projection_path(
    path: &str,
    attribute_names: Option<&HashMap<String, String>>,
) -> Option<Vec<ProjectionSegment>> {
    let mut segments =
        Vec::with_capacity(path.as_bytes().iter().filter(|byte| **byte == b'.').count() + 1);
    let mut cursor = 0usize;
    while cursor < path.len() {
        let bytes = path.as_bytes();
        match bytes.get(cursor).copied()? {
            b'.' => cursor += 1,
            b'[' => {
                cursor += 1;
                let end = path.get(cursor..)?.find(']')? + cursor;
                let index = path.get(cursor..end)?.parse().ok()?;
                segments.push(ProjectionSegment::Index(index));
                cursor = end + 1;
            }
            _ => {
                let end = path
                    .get(cursor..)?
                    .find(['.', '['])
                    .map_or(path.len(), |offset| cursor + offset);
                let raw = path.get(cursor..end)?;
                let key = attribute_names
                    .and_then(|names| names.get(raw))
                    .map_or_else(|| raw.to_string(), Clone::clone);
                segments.push(ProjectionSegment::Key(key));
                cursor = end;
            }
        }
    }
    Some(segments)
}

fn get_projection_path_value<'a>(
    item: &'a HashMap<String, AttributeValue>,
    path: &[ProjectionSegment],
) -> Option<&'a AttributeValue> {
    let (first, rest) = path.split_first()?;
    let ProjectionSegment::Key(first_key) = first else {
        return None;
    };
    let mut current = item.get(first_key)?;
    for segment in rest {
        match (segment, current) {
            (ProjectionSegment::Key(key), AttributeValue::M(map)) => current = map.get(key)?,
            (ProjectionSegment::Index(index), AttributeValue::L(list)) => {
                current = list.get(*index)?
            }
            _ => return None,
        }
    }
    Some(current)
}

fn insert_projected_value(
    target: &mut ProjectedValue,
    path: &[ProjectionSegment],
    value: AttributeValue,
) {
    let Some((head, tail)) = path.split_first() else {
        return;
    };
    match (target, head) {
        (ProjectedValue::Map(map), ProjectionSegment::Key(key)) if tail.is_empty() => {
            map.insert(key.clone(), value);
        }
        (ProjectedValue::Map(map), ProjectionSegment::Key(key)) => {
            let child = map
                .entry(key.clone())
                .or_insert_with(|| match tail.first() {
                    Some(ProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                    _ => AttributeValue::M(HashMap::new()),
                });
            insert_projected_attribute_value(child, tail, value);
        }
        (ProjectedValue::List(list), ProjectionSegment::Index(index)) => {
            if list.len() <= *index {
                list.resize_with(index + 1, || None);
            }
            if tail.is_empty() {
                list[*index] = Some(value);
            } else {
                let child = list[*index].get_or_insert_with(|| match tail.first() {
                    Some(ProjectionSegment::Index(_)) => AttributeValue::L(Vec::new()),
                    _ => AttributeValue::M(HashMap::new()),
                });
                insert_projected_attribute_value(child, tail, value);
            }
        }
        _ => {}
    }
}

fn insert_projected_attribute_value(
    target: &mut AttributeValue,
    path: &[ProjectionSegment],
    value: AttributeValue,
) {
    match target {
        AttributeValue::M(map) => {
            let mut projected = ProjectedValue::Map(std::mem::take(map));
            insert_projected_value(&mut projected, path, value);
            if let ProjectedValue::Map(updated) = projected {
                *map = updated;
            }
        }
        AttributeValue::L(list) => {
            let mut projected = ProjectedValue::List(
                std::mem::take(list)
                    .into_iter()
                    .map(Some)
                    .collect::<Vec<_>>(),
            );
            insert_projected_value(&mut projected, path, value);
            if let ProjectedValue::List(updated) = projected {
                *list = updated.into_iter().flatten().collect();
            }
        }
        _ => {}
    }
}

impl ProjectedValue {
    fn into_attribute_map(self) -> Option<HashMap<String, AttributeValue>> {
        match self {
            Self::Map(map) => Some(map),
            Self::List(_) => None,
        }
    }
}
