use storage_types::{
    AllOld, AttributeMap, AttributeValue, PutItemResponse, StorageError, StorageResult, TableName,
    TryIntoWireItem, UpdateItemResponse,
};

use super::{
    constants::{OCC_CREATE_CONDITION, OCC_UPDATE_CONDITION, OCC_VERSION_ATTR, OCC_VERSION_NAME},
    core::DatabaseManager,
    models::{PutItemEntityEncodeInput, PutItemInput, UpdateItemInput},
};

fn occ_version_expression_names() -> storage_types::VecMap<String, String> {
    [(OCC_VERSION_NAME.to_string(), OCC_VERSION_ATTR.to_string())]
        .into_iter()
        .collect()
}

fn entity_indexer_names<T>() -> Vec<String>
where T: storage_types::single_table_entity::SingleTableEntity {
    T::INDEXERS
        .iter()
        .map(|indexer| indexer.attribute_name().to_string())
        .collect()
}

impl DatabaseManager {
    pub async fn put_item_encode<T>(
        &self,
        table_name: TableName,
        item: &T,
        condition_expression: Option<String>,
        expression_attribute_names: Option<storage_types::VecMap<String, String>>,
        expression_attribute_values: Option<storage_types::AttributeMap>,
        return_values: Option<AllOld>,
    ) -> StorageResult<PutItemResponse>
    where
        T: TryIntoWireItem,
    {
        let item = item.try_into_wire_item()?;
        self.put_item(PutItemInput {
            table_name,
            item: item.into(),
            indexers: None,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }

    pub async fn put_item_entity_encode<T>(
        &self,
        input: PutItemEntityEncodeInput<'_, T>,
    ) -> StorageResult<PutItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity + TryIntoWireItem,
    {
        let entity = storage_types::single_table_entity::to_wire_entity(input.item)
            .map_err(|err| StorageError::internal(&err.to_string()))?;
        let (mut item, indexers) = entity.into_write_parts();
        crate::updated_at_apply::stamp_wire_item_now(&mut item)?;
        self.put_item(PutItemInput {
            table_name: input.table_name,
            item: item.into(),
            indexers,
            condition_expression: input.condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            return_values: input.return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }

    pub async fn put_new_entity_encode<T>(
        &self,
        table_name: TableName,
        item: &T,
    ) -> StorageResult<PutItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity + TryIntoWireItem,
    {
        self.put_item_entity_encode(
            PutItemEntityEncodeInput::builder()
                .table_name(table_name)
                .item(item)
                .condition_expression(Some(
                    "attribute_not_exists(pk) AND attribute_not_exists(sk)".to_string(),
                ))
                .build(),
        )
        .await
    }

    pub async fn update_item_entity<T>(
        &self,
        mut input: UpdateItemInput,
    ) -> StorageResult<UpdateItemResponse>
    where
        T: storage_types::single_table_entity::SingleTableEntity,
    {
        input.indexers = Some(entity_indexer_names::<T>());
        self.update_item(input).await
    }

    /// Put a brand new item enforcing that no OCC version exists yet.
    ///
    /// This will set `_v = 0` on the stored item (overwriting any supplied
    /// value) and applies a condition `attribute_not_exists(#v)` (with
    /// `ExpressionAttributeNames`) so that if
    /// the item already exists (was previously created) the write fails
    /// with a conditional check error.
    pub async fn put_new(
        &self,
        mut item: AttributeMap,
        table_name: TableName,
    ) -> StorageResult<PutItemResponse> {
        // Force initial version 0
        item.insert(
            OCC_VERSION_ATTR.to_string(),
            AttributeValue::N("0".to_string()),
        );
        self.put_item(
            PutItemInput::builder()
                .table_name(table_name)
                .item(item)
                .condition_expression(OCC_CREATE_CONDITION.to_string())
                .expression_attribute_names(occ_version_expression_names())
                .build(),
        )
        .await
    }

    /// Update (or create-if-missing) with optimistic concurrency control.
    ///
    /// `expected_v` semantics:
    /// - Some(v): condition `attribute_not_exists(#v) OR #v = :v`; stored item
    ///   gets `_v = v+1`. This allows idempotent create if two writers race
    ///   where the first wins (no version yet) and later updates require
    ///   matching version.
    /// - None: performs an unconditional write (bootstrap/admin paths) setting
    ///   `_v = 0` if not already present.
    pub async fn put_update(
        &self,
        mut item: AttributeMap,
        table_name: TableName,
        expected_v: Option<u64>,
    ) -> StorageResult<PutItemResponse> {
        let next_v = expected_v.map_or(0, |v| v + 1);
        item.insert(
            OCC_VERSION_ATTR.to_string(),
            AttributeValue::N(next_v.to_string()),
        );
        if let Some(v) = expected_v {
            return self
                .put_item(
                    PutItemInput::builder()
                        .table_name(table_name)
                        .item(item)
                        .condition_expression(OCC_UPDATE_CONDITION.to_string())
                        .expression_attribute_names(occ_version_expression_names())
                        .expression_attribute_values(
                            [(":v".to_string(), AttributeValue::N(v.to_string()))]
                                .into_iter()
                                .collect::<AttributeMap>(),
                        )
                        .build(),
                )
                .await;
        }

        self.put_item(
            PutItemInput::builder()
                .table_name(table_name)
                .item(item)
                .build(),
        )
        .await
    }
}
