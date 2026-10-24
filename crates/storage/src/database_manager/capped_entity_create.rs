use storage_types::{
    KeyAttributes, StorageEnum, StorageError, StorageResult, TableName, TransactEncodeItem,
    TransactEncodePutRequest, TransactUpdateRequest, TransactWriteItemsEncodeRequest,
    TryIntoWireItem, context::WrappedError as _, single_table_entity::SingleTableEntity,
};

use super::{
    CAPPED_ENTITY_COUNTER_CREATE_CONDITION, CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION,
    CappedStorageError, DatabaseManager, ENTITY_ABSENT_CONDITION,
    capped_entity_ops::{
        capped_entity_counter_expression_names, capped_entity_counter_expression_values,
        capped_entity_counter_key,
    },
};

/// Entity creation and its cap are prepared together, without opening a write
/// attempt.
pub struct PreparedCappedEntityCreate {
    entity: TransactEncodeItem,
    counter: TransactEncodeItem,
    outcome: CappedEntityCreateOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CappedEntityCreateRejection {
    ItemExists,
    CapacityExceeded,
}

impl From<CappedEntityCreateRejection> for CappedStorageError {
    fn from(rejection: CappedEntityCreateRejection) -> Self {
        match rejection {
            CappedEntityCreateRejection::ItemExists => Self::ItemExistError,
            CappedEntityCreateRejection::CapacityExceeded => Self::CapacityExceededError,
        }
    }
}

/// Exact cancellation positions and authoritative fallback reads for one
/// prepared attempt.
pub struct CappedEntityCreateOutcome {
    table_name: TableName,
    item_key: KeyAttributes,
    counted_entity_type: String,
    max_value: u64,
    entity_index: usize,
    counter_index: usize,
}

impl PreparedCappedEntityCreate {
    pub fn try_new<T>(
        table_name: TableName,
        item: &T,
        counted_entity_type: String,
        max_value: u64,
    ) -> StorageResult<Self>
    where
        T: SingleTableEntity + TryIntoWireItem,
    {
        Ok(Self {
            entity: prepare_entity_create(&table_name, item)?,
            counter: prepare_counter_create(&table_name, &counted_entity_type, max_value),
            outcome: CappedEntityCreateOutcome {
                table_name,
                item_key: item.table_key_map().into(),
                counted_entity_type,
                max_value,
                entity_index: 0,
                counter_index: 1,
            },
        })
    }

    /// Keep caller-owned effects first and retain the cap's final cancellation
    /// positions.
    pub fn into_transaction(
        self,
        leading_items: Vec<TransactEncodeItem>,
    ) -> (TransactWriteItemsEncodeRequest, CappedEntityCreateOutcome) {
        self.assemble_transaction(leading_items, Vec::new())
    }

    // Existing native job identities include entity/side-items/counter
    // ordering.
    pub(super) fn into_capped_transaction(
        self,
        additional_items: Vec<TransactEncodeItem>,
    ) -> (TransactWriteItemsEncodeRequest, CappedEntityCreateOutcome) {
        self.assemble_transaction(Vec::new(), additional_items)
    }

    fn assemble_transaction(
        mut self,
        mut leading_items: Vec<TransactEncodeItem>,
        additional_items: Vec<TransactEncodeItem>,
    ) -> (TransactWriteItemsEncodeRequest, CappedEntityCreateOutcome) {
        self.outcome.entity_index = leading_items.len();
        leading_items.push(self.entity);
        leading_items.extend(additional_items);
        self.outcome.counter_index = leading_items.len();
        leading_items.push(self.counter);
        (
            TransactWriteItemsEncodeRequest {
                transact_items: leading_items,
                ..Default::default()
            },
            self.outcome,
        )
    }
}

impl CappedEntityCreateOutcome {
    pub async fn classify_error(
        self,
        db: &DatabaseManager,
        error: &StorageError,
    ) -> StorageResult<Option<CappedEntityCreateRejection>> {
        if self.has_other_condition_failure(error) {
            return Ok(None);
        }
        if error.is_canceled_condition_check_failed(self.entity_index) {
            return Ok(Some(CappedEntityCreateRejection::ItemExists));
        }
        if error.is_canceled_condition_check_failed(self.counter_index) {
            return Ok(Some(CappedEntityCreateRejection::CapacityExceeded));
        }
        if !error.is_conditional_write_contention() {
            return Ok(None);
        }
        if db
            .get_item_with_consistent_read(self.table_name.clone(), self.item_key, true)
            .await?
            .is_some()
        {
            return Ok(Some(CappedEntityCreateRejection::ItemExists));
        }
        let count = db
            .load_capped_entity_counter_value(&self.table_name, &self.counted_entity_type)
            .await?;
        Ok(count
            .filter(|value| *value >= self.max_value)
            .map(|_| CappedEntityCreateRejection::CapacityExceeded))
    }

    fn has_other_condition_failure(&self, error: &StorageError) -> bool {
        let StorageEnum::TransactionCanceled { reasons } = error.to_enum() else {
            return false;
        };
        (0..reasons.len()).any(|index| {
            index != self.entity_index
                && index != self.counter_index
                && error.is_canceled_condition_check_failed(index)
        })
    }
}

fn prepare_entity_create<T>(table: &TableName, item: &T) -> StorageResult<TransactEncodeItem>
where T: SingleTableEntity + TryIntoWireItem {
    let item = storage_types::single_table_entity::to_wire_entity(item)
        .map_err(|error| StorageError::internal(&error.to_string()))?;
    Ok(TransactEncodeItem {
        put: Some(TransactEncodePutRequest {
            table_name: table.clone(),
            item,
            condition_expression: Some(ENTITY_ABSENT_CONDITION.into()),
            expression_attribute_names: None,
            expression_attribute_values: None,
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        ..Default::default()
    })
}

fn prepare_counter_create(
    table: &TableName,
    entity_type: &str,
    max_value: u64,
) -> TransactEncodeItem {
    TransactEncodeItem {
        update: Some(TransactUpdateRequest {
            table_name: table.clone(),
            key: capped_entity_counter_key(entity_type),
            update_expression: CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION.into(),
            indexers: None,
            condition_expression: Some(CAPPED_ENTITY_COUNTER_CREATE_CONDITION.into()),
            expression_attribute_names: Some(capped_entity_counter_expression_names()),
            expression_attribute_values: Some(capped_entity_counter_expression_values(
                1,
                entity_type,
                Some(max_value),
                None,
            )),
            return_values_on_condition_check_failure: None,
            aux_item_stream_ttl_hours: None,
        }),
        ..Default::default()
    }
}
