use std::collections::HashMap;

use storage_types::{
    AttributeValue, DecodedIndexedWireItem, GlobalSecondaryIndex, IndexName, IndexedWireItem,
    IndexerDeclaration, KeyAttributes, MaxIndexers, StorageError, StorageResult, StoredTableInfo,
    WireItem,
};

pub(crate) struct SqlIndexedItem {
    residual_json: String,
    slots: Vec<Option<String>>,
}

pub(crate) struct SqlDecodedItem {
    pub(crate) item: WireItem,
    pub(crate) indexers: Vec<String>,
}

pub(crate) type SqlLogicalItem = (HashMap<String, AttributeValue>, Vec<String>);

impl SqlIndexedItem {
    pub(crate) fn extract(
        logical_item: &HashMap<String, AttributeValue>,
        payload_item: &HashMap<String, AttributeValue>,
        indexers: Option<&[String]>,
        capacity: MaxIndexers,
    ) -> StorageResult<Self> {
        let declaration =
            IndexerDeclaration::try_new(indexers.unwrap_or_default().to_vec(), capacity)?;
        let indexed = IndexedWireItem::extract_projected(logical_item, payload_item, &declaration)?;
        let (residual_json, slots) = indexed.into_parts();
        let residual_json = String::from_utf8(residual_json)
            .map_err(|_| StorageError::internal("indexed residual JSON is not UTF-8"))?;
        Ok(Self {
            residual_json,
            slots,
        })
    }

    pub(crate) fn reconstruct_with_indexers(
        residual_json: String,
        slots: Vec<Option<String>>,
        key_attributes: &KeyAttributes,
        capacity: MaxIndexers,
    ) -> StorageResult<SqlDecodedItem> {
        if slots.len() != capacity.as_usize() {
            return Err(StorageError::internal(
                "stored_item_corruption:sql_slot_count",
            ));
        }
        let DecodedIndexedWireItem {
            mut item,
            declaration,
            ..
        } = IndexedWireItem::decode_padded_parts(residual_json.into_bytes(), slots)?;
        for (name, value) in key_attributes.iter() {
            if let Some(stored) = item.get(name)
                && stored != value
            {
                return Err(StorageError::internal(
                    "stored_item_corruption:indexed_key_mismatch",
                ));
            }
            item.insert(name.to_string(), value.clone());
        }
        Ok(SqlDecodedItem {
            item: WireItem::from_attribute_map(&item)?,
            indexers: declaration.into_names(),
        })
    }

    pub(crate) fn residual_json(&self) -> &str {
        &self.residual_json
    }

    pub(crate) fn slots(&self) -> &[Option<String>] {
        &self.slots
    }
}

pub(crate) fn sqlite_row_to_decoded_item(
    row: &rusqlite::Row<'_>,
    table_info: &storage_types::StoredTableInfo,
    key_column_prefix: Option<&str>,
) -> rusqlite::Result<SqlDecodedItem> {
    let key_attributes = crate::key_attribute_handler::key_attributes_from_row(
        row,
        &table_info.key_schema,
        &table_info.attribute_definitions,
        key_column_prefix,
    )
    .map_err(storage_error_to_rusqlite)?;
    let residual_json = row
        .get::<_, Option<String>>("attributes_blob")?
        .unwrap_or_else(|| "{}".to_string());
    let mut slots = Vec::with_capacity(table_info.max_indexers.as_usize());
    for ordinal in 0..table_info.max_indexers.as_usize() {
        let column = format!("__aux_indexer_{ordinal}");
        slots.push(row.get::<_, Option<String>>(column.as_str())?);
    }
    SqlIndexedItem::reconstruct_with_indexers(
        residual_json,
        slots,
        &key_attributes,
        table_info.max_indexers,
    )
    .map_err(storage_error_to_rusqlite)
}

#[cfg(any(feature = "turso-backend", feature = "postgres-backend"))]
pub(crate) fn project_gsi_wire_items(
    items: &mut [WireItem],
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
) -> StorageResult<()> {
    let Some(index) = gsi_for_read(table_info, index_name)? else {
        return Ok(());
    };
    for item in items {
        project_gsi_wire_item(item, table_info, index)?;
    }
    Ok(())
}

pub(crate) fn project_gsi_decoded_items(
    items: &mut [SqlDecodedItem],
    table_info: &StoredTableInfo,
    index_name: Option<&IndexName>,
) -> StorageResult<()> {
    let Some(index) = gsi_for_read(table_info, index_name)? else {
        return Ok(());
    };
    for decoded in items {
        project_gsi_wire_item(&mut decoded.item, table_info, index)?;
    }
    Ok(())
}

fn gsi_for_read<'a>(
    table_info: &'a StoredTableInfo,
    index_name: Option<&IndexName>,
) -> StorageResult<Option<&'a GlobalSecondaryIndex>> {
    index_name
        .map(|index_name| {
            table_info
                .global_secondary_indexes
                .as_ref()
                .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
                .ok_or_else(|| StorageError::internal("validated GSI read is missing its metadata"))
        })
        .transpose()
}

fn project_gsi_wire_item(
    item: &mut WireItem,
    table_info: &StoredTableInfo,
    index: &GlobalSecondaryIndex,
) -> StorageResult<()> {
    let logical = item.to_attribute_map()?;
    let projected = storage_common::apply_gsi_projection(
        &logical,
        Some(&index.projection),
        &table_info.key_schema,
        &index.key_schema,
    );
    *item = WireItem::from_attribute_map(&projected)?;
    Ok(())
}

fn storage_error_to_rusqlite(error: StorageError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            error.to_string(),
        )),
    )
}
