use crate::{keyspace::compact::KeyRange, storage_ops::provider_impl::*};

pub(super) struct SortedKvReadSequenceReadContext<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
> {
    provider: SortedKvDbStorageProvider<S>,
    read_context: Box<dyn SortedKvReadContext>,
}

#[async_trait]
impl<S> StorageProviderReadContext for SortedKvReadSequenceReadContext<S>
where S: crate::partition_family::PartitionFamilyKvStore + 'static
{
    async fn get_item(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.provider
            .get_item_with_kv_read_context(
                self.read_context.as_ref(),
                table_name,
                key,
                consistent_read,
            )
            .await
    }

    async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.provider
            .batch_get_item_with_kv_read_context(self.read_context.as_ref(), request)
            .await
    }

    async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        self.provider
            .query_table_with_kv_read_context(self.read_context.as_ref(), request)
            .await
    }
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(super) async fn begin_read_sequence_read_context_impl(
        &self,
        consistency: ReadSequenceConsistency,
    ) -> StorageResult<Box<dyn StorageProviderReadContext>> {
        if consistency != ReadSequenceConsistency::Transactional {
            return Err(StorageError::unsupported(
                "kv read-sequence provider contexts are only used for transactional reads",
            ));
        }
        let read_context = self.kv_store.begin_read_context().await?;
        Ok(self.read_sequence_context(read_context))
    }

    pub(super) fn read_sequence_context(
        &self,
        read_context: Box<dyn SortedKvReadContext>,
    ) -> Box<dyn StorageProviderReadContext> {
        Box::new(SortedKvReadSequenceReadContext {
            provider: self.clone(),
            read_context,
        })
    }

    pub(super) async fn get_item_impl(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.get_item_from_reader(
            KvReadSource::Store(&self.kv_store),
            table_name,
            key,
            consistent_read,
        )
        .await
    }

    pub(super) async fn get_item_with_kv_read_context(
        &self,
        read_context: &dyn SortedKvReadContext,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        self.get_item_from_reader(
            KvReadSource::Context(read_context),
            table_name,
            key,
            consistent_read,
        )
        .await
    }

    pub(super) async fn batch_get_item_impl(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.batch_get_item_from_reader(KvReadSource::Store(&self.kv_store), request)
            .await
    }

    pub(super) async fn scan_table_impl(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<WireItem>, Option<String>)> {
        let consistent_read = request.consistent_read;
        if consistent_read && request.index_name.is_some() {
            return Err(StorageError::validation(
                "Consistent reads are not supported on global secondary indexes",
            ));
        }
        if let Some(idx) = request.index_name.as_ref() {
            Span::current().record("index_name", idx.to_string());
        }

        let table_name = request.table_name.clone();
        let table_metadata = self
            .get_table_identity_from_name(&request.table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = &table_metadata.table_info;
        let data_range = scan_data_range(&table_metadata.identity, request)?;
        let page_token = scan_page_token(&table_metadata.identity, table_info, request)?;

        let prefix_result = self
            .kv_store
            .get_range_values(
                &data_range.start,
                &data_range.end,
                request.limit,
                page_token,
                consistent_read,
            )
            .await?;
        let has_more_items = prefix_result.has_more;
        let (result_items, bytes_read) = decode_scan_items(prefix_result.values)?;

        record_read(result_items.len(), bytes_read);
        record_read_cost(
            "scan_table",
            "scan",
            1,
            wire_items_payload_bytes(&result_items),
        );

        let last_evaluated_key = scan_last_evaluated_key(
            has_more_items,
            &result_items,
            table_info,
            &request.index_name,
        )?;

        Ok((result_items, last_evaluated_key))
    }

    #[allow(dead_code)]
    pub(crate) async fn get_item_map(
        &self,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<HashMap<String, AttributeValue>>> {
        if key.is_empty() {
            return Ok(None);
        }

        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = &table_metadata.table_info;
        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)?;
        let item_key = table_keys::item_key(&table_metadata.identity, &item_key)?;
        let raw = self.kv_store.get(&item_key, consistent_read).await?;
        let wire_item = raw
            .as_deref()
            .map(decode_wire_item_from_storage_bytes)
            .transpose()?;
        wire_item.map(WireItem::into_attribute_map).transpose()
    }

    async fn batch_get_item_with_kv_read_context(
        &self,
        read_context: &dyn SortedKvReadContext,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        self.batch_get_item_from_reader(KvReadSource::Context(read_context), request)
            .await
    }

    async fn get_item_from_reader(
        &self,
        reader: KvReadSource<'_, S>,
        table_name: TableName,
        key: KeyAttributes,
        consistent_read: bool,
    ) -> StorageResult<Option<WireItem>> {
        if key.is_empty() {
            record_read(0, 0);
            return Ok(None);
        }

        let table_metadata = self
            .get_table_identity_from_name(&table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(&table_name))?;
        let table_info = &table_metadata.table_info;
        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, &key)?;
        let item_key = table_keys::item_key(&table_metadata.identity, &item_key)?;

        let Some(data) = reader.get(&item_key, consistent_read).await? else {
            record_read(0, 0);
            record_read_cost("get_item", "get", 1, 0);
            return Ok(None);
        };

        record_read(1, data.len());
        let json = storage_types::storage_serde::decompress_owned_bytes(data)?;
        let item = WireItem::dynamo_json(json);
        record_read_cost("get_item", "get", 1, item.payload_len() as u64);
        Ok(Some(item))
    }

    async fn batch_get_item_from_reader(
        &self,
        reader: KvReadSource<'_, S>,
        request: BatchGetItemRequest,
    ) -> StorageResult<BatchGetWireItemResponse> {
        let mut result = BatchGetAccumulator::new(&request);

        for (table_name, keys_and_attributes) in &request.request_items {
            if keys_and_attributes.keys.is_empty() {
                continue;
            }

            let Some(raw_results) = self
                .load_batch_get_raw_items(&reader, table_name, keys_and_attributes)
                .await?
            else {
                result.record_unprocessed_keys(table_name, keys_and_attributes);
                continue;
            };

            result.record_table_items(table_name, raw_results)?;
        }

        Ok(result.into_response())
    }

    async fn load_batch_get_raw_items(
        &self,
        reader: &KvReadSource<'_, S>,
        table_name: &TableName,
        keys_and_attributes: &KeysAndAttributes,
    ) -> StorageResult<Option<Vec<Option<Vec<u8>>>>> {
        let table_metadata = self
            .get_table_identity_from_name(table_name)
            .await?
            .ok_or_else(|| StorageError::table_not_found(table_name))?;
        let table_info = &table_metadata.table_info;
        let serialized_keys =
            serialize_batch_get_keys(&table_metadata.identity, table_info, keys_and_attributes)?;
        let consistent_read = keys_and_attributes.consistent_read.unwrap_or(false);

        let fdb_wait_started = Instant::now();
        let result = reader.multi_get(serialized_keys, consistent_read).await;
        record_provider_stage("batch_get_item", "fdb_wait", fdb_wait_started.elapsed());

        match result {
            Ok(results) => Ok(Some(results)),
            Err(error)
                if matches!(error.to_enum(), StorageEnum::TableNotFound { .. })
                    || matches!(error.to_enum(), StorageEnum::KeyValidation { .. }) =>
            {
                Err(error)
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    table_name = %table_name,
                    "batch_get_item.multi_get_failed"
                );
                Ok(None)
            }
        }
    }
}

enum KvReadSource<'a, S> {
    Store(&'a S),
    Context(&'a dyn SortedKvReadContext),
}

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> KvReadSource<'_, S> {
    async fn get(&self, key: &[u8], consistent_read: bool) -> StorageResult<Option<Vec<u8>>> {
        match self {
            Self::Store(store) => store.get(key, consistent_read).await,
            Self::Context(context) => context.get(key, consistent_read).await,
        }
    }

    async fn multi_get(
        &self,
        keys: Vec<Vec<u8>>,
        consistent_read: bool,
    ) -> StorageResult<Vec<Option<Vec<u8>>>> {
        match self {
            Self::Store(store) => store.multi_get(keys, consistent_read).await,
            Self::Context(context) => context.multi_get(keys, consistent_read).await,
        }
    }
}

struct BatchGetAccumulator {
    total_requested_keys: usize,
    total_items_returned: usize,
    total_bytes_read: usize,
    billed_bytes_read: usize,
    responses: HashMap<TableName, Vec<WireItem>>,
    unprocessed_keys: HashMap<TableName, KeysAndAttributes>,
}

impl BatchGetAccumulator {
    fn new(request: &BatchGetItemRequest) -> Self {
        Self {
            total_requested_keys: request
                .request_items
                .values()
                .map(|item| item.keys.len())
                .sum(),
            total_items_returned: 0,
            total_bytes_read: 0,
            billed_bytes_read: 0,
            responses: HashMap::with_capacity(request.request_items.len()),
            unprocessed_keys: HashMap::new(),
        }
    }

    fn record_unprocessed_keys(
        &mut self,
        table_name: &TableName,
        keys_and_attributes: &KeysAndAttributes,
    ) {
        self.unprocessed_keys
            .insert(table_name.clone(), keys_and_attributes.clone());
    }

    fn record_table_items(
        &mut self,
        table_name: &TableName,
        raw_results: Vec<Option<Vec<u8>>>,
    ) -> StorageResult<()> {
        let decode_started = Instant::now();
        let mut retrieved_items = Vec::with_capacity(raw_results.len());

        for raw in raw_results.into_iter().flatten() {
            self.total_bytes_read += raw.len();
            let json = storage_types::storage_serde::decompress_bytes(&raw)?;
            let item = WireItem::dynamo_json(json);
            self.billed_bytes_read += item.payload_len();
            retrieved_items.push(item);
        }

        record_provider_stage("batch_get_item", "decode", decode_started.elapsed());
        self.total_items_returned += retrieved_items.len();

        if !retrieved_items.is_empty() {
            self.responses.insert(table_name.clone(), retrieved_items);
        }

        Ok(())
    }

    fn into_response(self) -> BatchGetWireItemResponse {
        let materialize_started = Instant::now();
        let response = BatchGetWireItemResponse {
            responses: if self.responses.is_empty() {
                None
            } else {
                Some(self.responses)
            },
            unprocessed_keys: if self.unprocessed_keys.is_empty() {
                None
            } else {
                Some(self.unprocessed_keys)
            },
            consumed_capacity: None,
        };
        record_provider_stage(
            "batch_get_item",
            "response_materialization",
            materialize_started.elapsed(),
        );
        record_read(self.total_items_returned, self.total_bytes_read);
        record_read_cost(
            "batch_get_item",
            "get",
            self.total_requested_keys,
            self.billed_bytes_read as u64,
        );
        response
    }
}

fn serialize_batch_get_keys(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    keys_and_attributes: &KeysAndAttributes,
) -> StorageResult<Vec<Vec<u8>>> {
    let mut serialized_keys = Vec::with_capacity(keys_and_attributes.keys.len());

    for key in &keys_and_attributes.keys {
        let item_key =
            ItemKey::from_key_schema(table_info.table_name.clone(), &table_info.key_schema, key)?;
        serialized_keys.push(table_keys::item_key(table_identity, &item_key)?);
    }

    Ok(serialized_keys)
}

fn scan_data_range(
    table_identity: &TableIdentity,
    request: &ScanTableRequest,
) -> StorageResult<KeyRange> {
    if let Some(index_name) = &request.index_name {
        return table_keys::gsi_prefix(table_identity, index_name).ok_or_else(|| {
            StorageError::internal(&format!("missing storage identity for index {index_name}"))
        });
    }

    Ok(table_keys::primary_item_prefix(table_identity.table_id))
}

fn scan_page_token(
    table_identity: &TableIdentity,
    table_info: &StoredTableInfo,
    request: &ScanTableRequest,
) -> StorageResult<Option<RawKey>> {
    request
        .exclusive_start_key
        .as_ref()
        .and_then(|token| {
            ItemKey::item_key_from_next_page_token(token, table_info, &request.index_name).ok()
        })
        .flatten()
        .as_ref()
        .map(|token| table_keys::item_key(table_identity, token))
        .transpose()
        .map(|key| key.map(RawKey))
}

fn decode_scan_items(values: Vec<Vec<u8>>) -> StorageResult<(Vec<WireItem>, usize)> {
    let mut result_items = Vec::new();
    let mut bytes_read = 0_usize;
    for data in values {
        bytes_read += data.len();
        let json = storage_types::storage_serde::decompress_bytes(&data)?;
        result_items.push(WireItem::dynamo_json(json));
    }
    Ok((result_items, bytes_read))
}

fn scan_last_evaluated_key(
    has_more_items: bool,
    result_items: &[WireItem],
    table_info: &StoredTableInfo,
    index_name: &Option<IndexName>,
) -> StorageResult<Option<String>> {
    if !has_more_items || result_items.is_empty() {
        return Ok(None);
    }

    result_items
        .last()
        .ok_or_else(|| StorageError::internal("missing last scan result item"))?
        .last_evaluated_key(table_info, index_name)
}
