use crate::storage_ops::provider_impl::*;

impl<S: crate::partition_family::PartitionFamilyKvStore + 'static> SortedKvDbStorageProvider<S> {
    pub(crate) fn requires_immediate_gsi_updates(&self, table_info: &StoredTableInfo) -> bool {
        self.immediate_gsi_consistency
            && table_info
                .global_secondary_indexes
                .as_ref()
                .is_some_and(|indexes| {
                    indexes
                        .iter()
                        .any(|gsi| !ttl::is_ttl_index(&gsi.index_name))
                })
    }

    pub(in crate::storage_ops) fn gsi_batch_mutations_for_items(
        &self,
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        old_item: Option<&HashMap<String, AttributeValue>>,
        new_item: Option<&HashMap<String, AttributeValue>>,
        declaration: Option<&IndexerDeclaration>,
    ) -> StorageResult<Vec<BatchItem>> {
        let Some(gsis) = table_info.global_secondary_indexes.as_ref() else {
            return Ok(Vec::new());
        };

        let mut mutations = Vec::new();
        for gsi in gsis
            .iter()
            .filter(|gsi| !ttl::is_ttl_index(&gsi.index_name))
        {
            let old_key = Self::gsi_batch_item_key(table_identity, table_info, gsi, old_item)?;
            let new_key = Self::gsi_batch_item_key(table_identity, table_info, gsi, new_item)?;

            if let Some(old_key) = old_key
                && Some(old_key.clone()) != new_key
            {
                mutations.push(BatchItem {
                    key: old_key,
                    value: None,
                });
            }

            if let (Some(item), Some(key)) = (new_item, new_key) {
                let declaration = declaration.ok_or_else(|| {
                    StorageError::internal("GSI put requires an indexer declaration")
                })?;
                let projected = storage_common::apply_gsi_projection(
                    item,
                    Some(&gsi.projection),
                    &table_info.key_schema,
                    &gsi.key_schema,
                );
                mutations.push(BatchItem {
                    key,
                    value: Some(encode_indexed_wire_item(
                        self.kv_store.item_value_codec(),
                        &IndexedWireItem::extract_projected(item, &projected, declaration)?,
                    )?),
                });
            }
        }

        Ok(mutations)
    }

    fn gsi_batch_item_key(
        table_identity: &TableIdentity,
        table_info: &StoredTableInfo,
        gsi: &storage_types::GlobalSecondaryIndex,
        item: Option<&HashMap<String, AttributeValue>>,
    ) -> StorageResult<Option<Vec<u8>>> {
        let Some(item) = item else {
            return Ok(None);
        };

        ItemKey::from_key_schema_for_index(
            table_info.table_name.clone(),
            &table_info.key_schema,
            &gsi.index_name,
            &gsi.key_schema,
            item,
        )?
        .map(|key| table_keys::item_key(table_identity, &key))
        .transpose()
    }

    #[inline]
    pub(crate) fn find_gsi_projection<'a>(
        table_info: &'a StoredTableInfo,
        index_name: &IndexName,
    ) -> Option<&'a Projection> {
        table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|gsis| gsis.iter().find(|g| g.index_name == *index_name))
            .map(|g| &g.projection)
    }
}
