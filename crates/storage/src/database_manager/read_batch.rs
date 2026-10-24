use storage_types::{
    BatchGetItemRequest, BatchGetItemResponse, KeyAttributes, KeysAndAttributes, StorageResult,
    TableName, TryFromWireItem, VecMap,
};

use super::{DatabaseManager, constants::DECODE_BATCH_GET_KEYS};

impl DatabaseManager {
    /// Collect existing BatchGetItem pages, retrying only returned unprocessed
    /// keys. Each page retains its existing operation metrics and native
    /// attempt. This paginator does not promise a transactional snapshot across
    /// pages.
    pub async fn batch_get_all_pages(
        &self,
        mut request: BatchGetItemRequest,
    ) -> StorageResult<Vec<BatchGetItemResponse>> {
        let mut pages = Vec::new();
        loop {
            let page = self.batch_get_item(request).await?;
            let next = page
                .unprocessed_keys
                .clone()
                .filter(|keys| !keys.is_empty());
            pages.push(page);
            let Some(request_items) = next else {
                return Ok(pages);
            };
            request = BatchGetItemRequest {
                request_items,
                return_consumed_capacity: None,
            };
        }
    }

    /// Strongly read distinct entity keys and strictly decode every returned
    /// item. Missing keys are omitted. Empty input performs no database
    /// operation.
    pub async fn batch_get_items_decode<T>(
        &self,
        table_name: TableName,
        keys: Vec<KeyAttributes>,
    ) -> StorageResult<Vec<T>>
    where
        T: TryFromWireItem,
    {
        let mut items = Vec::with_capacity(keys.len());
        let mut keys = keys.into_iter();
        while keys.len() > 0 {
            let request = strong_batch_request(
                table_name.clone(),
                keys.by_ref().take(DECODE_BATCH_GET_KEYS).collect(),
            );
            for page in self.batch_get_all_pages(request).await? {
                append_decoded_items(&mut items, &page, &table_name)?;
            }
        }
        Ok(items)
    }
}

fn strong_batch_request(table_name: TableName, keys: Vec<KeyAttributes>) -> BatchGetItemRequest {
    BatchGetItemRequest {
        request_items: VecMap::from_iter([(
            table_name,
            KeysAndAttributes {
                keys,
                consistent_read: Some(true),
                attributes_to_get: None,
                projection_expression: None,
                expression_attribute_names: None,
            },
        )]),
        return_consumed_capacity: None,
    }
}

fn append_decoded_items<T: TryFromWireItem>(
    output: &mut Vec<T>,
    page: &BatchGetItemResponse,
    table_name: &TableName,
) -> StorageResult<()> {
    let Some(items) = page
        .responses
        .as_ref()
        .and_then(|tables| tables.get(table_name))
    else {
        return Ok(());
    };
    for item in items {
        output.push(T::try_from_wire_item(item)?);
    }
    Ok(())
}
