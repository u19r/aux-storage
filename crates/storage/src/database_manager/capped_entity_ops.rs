use sha2::{Digest, Sha256};
use storage_types::{
    AttributeValue, StorageError, StorageResult, TableName, TransactEncodeItem,
    TransactUpdateRequest, TransactWriteItemsEncodeRequest,
};

use super::constants::{
    CAPPED_ENTITY_COUNTER_DELTA_VALUE, CAPPED_ENTITY_COUNTER_ENTITY_TYPE_NAME,
    CAPPED_ENTITY_COUNTER_ENTITY_TYPE_VALUE, CAPPED_ENTITY_COUNTER_MAX_VALUE,
    CAPPED_ENTITY_COUNTER_PK, CAPPED_ENTITY_COUNTER_VALUE_ATTR, CAPPED_ENTITY_COUNTER_VALUE_NAME,
    CAPPED_ENTITY_COUNTER_ZERO_VALUE,
};
use crate::database_manager::{
    CAPPED_ENTITY_COUNTER_DELETE_CONDITION, CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION,
    CappedStorageError, CreateCappedEntityInput, DatabaseManager, DeleteCappedEntityInput,
    ENTITY_EXISTS_CONDITION,
};

pub(super) fn capped_entity_counter_key(entity_type: &str) -> storage_types::KeyAttributes {
    storage_types::KeyOwned::pk_sk(CAPPED_ENTITY_COUNTER_PK, entity_type).into()
}

pub(super) fn capped_entity_counter_expression_names() -> storage_types::VecMap<String, String> {
    [
        (
            CAPPED_ENTITY_COUNTER_VALUE_NAME.to_string(),
            CAPPED_ENTITY_COUNTER_VALUE_ATTR.to_string(),
        ),
        (
            CAPPED_ENTITY_COUNTER_ENTITY_TYPE_NAME.to_string(),
            storage_types::single_table_entity::ENTITY_TYPE_ATTR.to_string(),
        ),
    ]
    .into_iter()
    .collect()
}

pub(super) fn capped_entity_counter_expression_values(
    delta: i64,
    entity_type: &str,
    max_value: Option<u64>,
    zero_value: Option<u64>,
) -> storage_types::AttributeMap {
    let mut values: storage_types::AttributeMap = [
        (
            CAPPED_ENTITY_COUNTER_DELTA_VALUE.to_string(),
            AttributeValue::N(delta.to_string()),
        ),
        (
            CAPPED_ENTITY_COUNTER_ENTITY_TYPE_VALUE.to_string(),
            AttributeValue::S(entity_type.to_string()),
        ),
    ]
    .into_iter()
    .collect();
    if let Some(max_value) = max_value {
        values.insert(
            CAPPED_ENTITY_COUNTER_MAX_VALUE.to_string(),
            AttributeValue::N(max_value.to_string()),
        );
    }
    if let Some(zero_value) = zero_value {
        values.insert(
            CAPPED_ENTITY_COUNTER_ZERO_VALUE.to_string(),
            AttributeValue::N(zero_value.to_string()),
        );
    }
    values
}

impl DatabaseManager {
    pub async fn create_capped_entity<T>(
        &self,
        input: CreateCappedEntityInput<'_, T>,
    ) -> Result<(), CappedStorageError>
    where
        T: storage_types::single_table_entity::SingleTableEntity + storage_types::TryIntoWireItem,
    {
        let CreateCappedEntityInput {
            table_name,
            item,
            counted_entity_type,
            max_value,
            additional_transact_items,
            job_effect_authority,
        } = input;
        let prepared = super::PreparedCappedEntityCreate::try_new(
            table_name,
            item,
            counted_entity_type,
            max_value,
        )?;
        let (request, outcome) = prepared.into_capped_transaction(additional_transact_items);
        match self
            .commit_capped_entity_creation(request, job_effect_authority)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                let rejection = outcome.classify_error(self, &error).await?;
                Err(rejection.map_or(CappedStorageError::StorageError(error), Into::into))
            }
        }
    }

    async fn commit_capped_entity_creation(
        &self,
        request: TransactWriteItemsEncodeRequest,
        authority: Option<scheduled_bg_jobs::JobEffectAuthority>,
    ) -> StorageResult<()> {
        let Some(authority) = authority else {
            self.transact_write_items_encode(request).await?;
            return Ok(());
        };
        let mut request: storage_types::TransactWriteItemsRequest = request.try_into()?;
        let identity = serde_json::to_vec(&request.transact_items)
            .map_err(|_| StorageError::internal("capped effect identity encoding failed"))?;
        request.client_request_token = Some(hex::encode(&Sha256::digest(identity)[..16]));
        self.transact_write_job_effect(request, authority).await?;
        Ok(())
    }

    pub async fn delete_capped_entity(
        &self,
        input: DeleteCappedEntityInput,
    ) -> Result<(), CappedStorageError> {
        let DeleteCappedEntityInput {
            table_name,
            key,
            counted_entity_type,
        } = input;
        let item_key = key.clone();
        let transact_items = vec![
            TransactEncodeItem {
                delete: Some(storage_types::TransactDeleteRequest {
                    table_name: table_name.clone(),
                    key,
                    condition_expression: Some(ENTITY_EXISTS_CONDITION.to_string()),
                    expression_attribute_names: None,
                    expression_attribute_values: None,
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..Default::default()
            },
            TransactEncodeItem {
                update: Some(TransactUpdateRequest {
                    indexers: None,
                    table_name: table_name.clone(),
                    key: capped_entity_counter_key(&counted_entity_type),
                    update_expression: CAPPED_ENTITY_COUNTER_UPDATE_EXPRESSION.to_string(),
                    condition_expression: Some(CAPPED_ENTITY_COUNTER_DELETE_CONDITION.to_string()),
                    expression_attribute_names: Some(capped_entity_counter_expression_names()),
                    expression_attribute_values: Some(capped_entity_counter_expression_values(
                        -1,
                        &counted_entity_type,
                        None,
                        Some(0),
                    )),
                    return_values_on_condition_check_failure: None,
                    aux_item_stream_ttl_hours: None,
                }),
                ..Default::default()
            },
        ];

        match self
            .transact_write_items_encode(TransactWriteItemsEncodeRequest {
                transact_items,
                ..Default::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => Err(self
                .classify_delete_capped_entity_error(error, &table_name, item_key, 0)
                .await),
        }
    }

    async fn classify_delete_capped_entity_error(
        &self,
        error: StorageError,
        table_name: &TableName,
        item_key: storage_types::KeyAttributes,
        delete_index: usize,
    ) -> CappedStorageError {
        if error.is_canceled_condition_check_failed(delete_index) {
            return CappedStorageError::ItemNotExistsError;
        }

        if !error.is_conditional_write_contention() {
            return CappedStorageError::StorageError(error);
        }

        match self
            .get_item_with_consistent_read(table_name.clone(), item_key, true)
            .await
        {
            Ok(None) => CappedStorageError::ItemNotExistsError,
            Ok(Some(_)) => CappedStorageError::StorageError(error),
            Err(read_error) => CappedStorageError::StorageError(read_error),
        }
    }

    pub(super) async fn load_capped_entity_counter_value(
        &self,
        table_name: &TableName,
        counted_entity_type: &str,
    ) -> StorageResult<Option<u64>> {
        let item = self
            .get_item_decode_with_consistent_read::<_, storage_types::AttributeMap>(
                table_name.clone(),
                capped_entity_counter_key(counted_entity_type),
                true,
            )
            .await?;
        let Some(item) = item else {
            return Ok(None);
        };
        let Some(value) = item.get(crate::database_manager::CAPPED_ENTITY_COUNTER_VALUE_ATTR)
        else {
            return Ok(None);
        };
        match value {
            AttributeValue::N(raw) => raw.parse::<u64>().map(Some).map_err(|err| {
                StorageError::internal(&format!(
                    "invalid capped entity counter value for {counted_entity_type}: {err}"
                ))
            }),
            other => Err(StorageError::internal(&format!(
                "capped entity counter value for {counted_entity_type} must be numeric, got \
                 {other:?}"
            ))),
        }
    }
}
