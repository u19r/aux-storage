use crate::storage_ops::provider_impl::*;

#[cfg(test)]
impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub async fn scan_table(
        &self,
        request: &ScanTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::scan_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    pub async fn query_table(
        &self,
        request: &QueryTableRequest,
    ) -> StorageResult<(Vec<HashMap<String, AttributeValue>>, Option<String>)> {
        let (items, lek) = <Self as StorageProvider>::query_table(self, request).await?;
        let mut decoded = Vec::with_capacity(items.len());
        for item in items {
            decoded.push(item.into_attribute_map()?);
        }
        Ok((decoded, lek))
    }

    pub async fn batch_get_item(
        &self,
        request: BatchGetItemRequest,
    ) -> StorageResult<storage_types::BatchGetItemResponse> {
        let response = <Self as StorageProvider>::batch_get_item(self, request).await?;
        decode_batch_get_response_to_maps(response)
    }
}

#[cfg(test)]
fn decode_batch_get_response_to_maps(
    response: BatchGetWireItemResponse,
) -> StorageResult<storage_types::BatchGetItemResponse> {
    let responses = if let Some(table_items) = response.responses {
        let mut decoded = HashMap::with_capacity(table_items.len());
        for (table, items) in table_items {
            let mut table_rows = Vec::with_capacity(items.len());
            for item in items {
                table_rows.push(item.into_attribute_map()?.into());
            }
            decoded.insert(table, table_rows);
        }
        Some(decoded)
    } else {
        None
    };

    Ok(storage_types::BatchGetItemResponse {
        responses,
        unprocessed_keys: response.unprocessed_keys,
        consumed_capacity: response.consumed_capacity,
    })
}
