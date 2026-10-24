use storage_types::{BoundTableInfo, IndexerDeclaration, KeyAttributes, LogicalItemSize, WireItem};

use crate::{AcceptedUpsert, BoundPutItem};

/// Validated new-image facts, independent of protected old state.
pub struct UpsertImage<'a> {
    pub table: &'a BoundTableInfo,
    pub key: &'a KeyAttributes,
    pub new_item: &'a WireItem,
    pub item_size: LogicalItemSize,
    pub declaration: &'a IndexerDeclaration,
}

impl UpsertImage<'_> {
    /// Primary identity comes from the validated key. Secondary columns are
    /// a nullable projection: old items may violate a subsequently added
    /// GSI's declared type without invalidating their authoritative image.
    pub fn key_column_value<'a>(
        &'a self,
        column: &storage_types::PlannedKeyColumn,
        attributes: &'a storage_types::AttributeMap,
    ) -> Option<&'a storage_types::AttributeValue> {
        if self
            .table
            .key_schema
            .iter()
            .any(|key| key.attribute_name == column.attribute_name)
        {
            return self.key.get(&column.attribute_name);
        }
        attributes
            .get(&column.attribute_name)
            .filter(|value| value.matches_key_type(&column.attribute_type))
    }
}

impl<'a> From<&'a BoundPutItem> for UpsertImage<'a> {
    fn from(item: &'a BoundPutItem) -> Self {
        Self {
            table: &item.table,
            key: &item.key,
            new_item: &item.new_item,
            item_size: item.item_size,
            declaration: &item.declaration,
        }
    }
}

impl<'a> From<&'a AcceptedUpsert> for UpsertImage<'a> {
    fn from(item: &'a AcceptedUpsert) -> Self {
        Self {
            table: &item.table,
            key: &item.key,
            new_item: &item.new_item,
            item_size: item.item_size,
            declaration: &item.declaration,
        }
    }
}
