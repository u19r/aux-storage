//! Shared UpdateItem decision; PutItem owns the same physical upsert effects.

use std::sync::Arc;

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    AttributeMap, BoundTableInfo, DynamoRequestValidate, IndexerDeclaration, KeyAttributes,
    LogicalItemSize, ReturnValuesOldNewUpdated, StorageEnum, StorageError, StorageResult,
    TableName, UpdateItemRequest, UpdateItemResponse, condition::CompiledCondition,
    return_values_on_condition_check_failure_all_old, validate_transact_key,
};

use crate::{
    AcceptedUpsert, ItemWriteObservation, SemanticDecision,
    dynamo_put_item::ObservedWrite,
    update_logic::{
        UpdateOperation, apply_update_operations, parse_update_expression, updated_attribute_map,
        updated_attribute_map_borrowed,
    },
};

pub struct UpdateItemShape {
    pub table_name: TableName,
    key: KeyAttributes,
    pub(crate) operations: Vec<UpdateOperation>,
    indexers: Option<Vec<String>>,
    condition: Option<CompiledCondition>,
    return_values: Option<ReturnValuesOldNewUpdated>,
    return_old_on_condition_failure: bool,
}

fn reject_key_attribute_updates(
    key_schema: &[storage_types::KeySchemaElement],
    operations: &[UpdateOperation],
) -> StorageResult<()> {
    for operation in operations {
        let root = operation.field_path().root();
        if key_schema.iter().any(|key| key.attribute_name == root) {
            return Err(StorageError::validation(format!(
                "One or more parameter values were invalid: Cannot update attribute {root}. This \
                 attribute is part of the key"
            )));
        }
    }
    Ok(())
}

impl UpdateItemShape {
    pub(crate) fn key(&self) -> &KeyAttributes {
        &self.key
    }

    pub fn try_from_request(request: UpdateItemRequest) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        Self::from_validated_request(request)
    }

    /// Parent batch/transaction validation covers member request fields.
    pub(crate) fn from_validated_request(mut request: UpdateItemRequest) -> StorageResult<Self> {
        let operations = match (
            request.update_expression.as_deref(),
            request.attribute_updates,
        ) {
            (Some(expression), None) => parse_update_expression(
                expression,
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_ref(),
            )?,
            (None, Some(_)) => {
                return Err(StorageError::validation(
                    "AttributeUpdates is not supported; use UpdateExpression",
                ));
            }
            (None, None) => Vec::new(),
            (Some(_), Some(_)) => {
                return Err(StorageError::validation(
                    "UpdateItem cannot combine UpdateExpression and AttributeUpdates",
                ));
            }
        };
        let condition = CompiledCondition::compile_write(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
        request.key.normalize_numbers_for_write();
        Ok(Self {
            table_name: request.table_name,
            key: request.key,
            operations,
            indexers: request.indexers,
            condition,
            return_values: request.return_values,
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
        })
    }

    pub fn bind_table(
        self: Arc<Self>,
        table: Arc<BoundTableInfo>,
    ) -> StorageResult<BoundUpdateItem> {
        validate_transact_key(&table.info, &self.key)?;
        reject_key_attribute_updates(&table.info.key_schema, &self.operations)?;
        Ok(BoundUpdateItem {
            table,
            key: self.key.clone(),
            shape: self,
        })
    }
}

pub struct BoundUpdateItem {
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    shape: Arc<UpdateItemShape>,
}

impl BoundUpdateItem {
    pub fn accept_old(
        self,
        observed: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<(AcceptedUpsert, UpdateReturnShape)>> {
        // An if_not_exists operand too large for any item is a size error
        // whether or not the condition would pass.
        for operation in &self.shape.operations {
            if let UpdateOperation::SetIfNotExists { operand, .. } = operation {
                LogicalItemSize::try_from_updated_entries([("x", operand)])?;
            }
        }
        let old = ObservedWrite::try_new(observed, &self.key)?;
        // The only decode of the old image: condition, failure item, response
        // and update base.
        let old_map = old
            .item
            .as_deref()
            .map(AttributeMap::try_from)
            .transpose()?;
        if let Some(condition) = self.shape.condition.as_ref() {
            let absent = AttributeMap::new();
            if !condition.matches(old_map.as_ref().unwrap_or(&absent)) {
                let error = match (self.shape.return_old_on_condition_failure, old_map) {
                    (true, Some(item)) => StorageEnum::ConditionalCheckFailedWithItem { item },
                    _ => StorageEnum::ConditionalCheckFailed,
                };
                return Ok(SemanticDecision::Rejected(error.into()));
            }
        }
        let old_response = match self.shape.return_values {
            Some(ReturnValuesOldNewUpdated::AllOld) => old_map.clone(),
            Some(ReturnValuesOldNewUpdated::UpdatedOld) => old_map
                .as_ref()
                .and_then(|item| updated_attribute_map_borrowed(&self.shape.operations, item)),
            _ => None,
        };
        let base = old_map.unwrap_or_else(|| AttributeMap::from(&self.key));
        let indexers = self
            .shape
            .indexers
            .clone()
            .unwrap_or_else(|| old.indexers.clone());
        let updated = match self.evaluate_updated_item(base, indexers)? {
            SemanticDecision::Accepted(updated) => updated,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        SemanticDecision::try_from(self.accept_updated_item(old, lifecycle, updated, old_response))
    }

    fn evaluate_updated_item(
        &self,
        base: AttributeMap,
        indexers: Vec<String>,
    ) -> StorageResult<SemanticDecision<(AttributeMap, LogicalItemSize, IndexerDeclaration)>> {
        let result: StorageResult<(AttributeMap, LogicalItemSize, IndexerDeclaration)> = (|| {
            let mut updated = apply_update_operations(base, &self.shape.operations)?
                .with_verified_keys(&self.key)?;
            updated.normalize_numbers_for_write();
            let item_size = LogicalItemSize::try_from_updated_entries(updated.iter())?;
            let declaration = IndexerDeclaration::try_new(indexers, self.table.info.max_indexers)?;
            storage_types::validate_logical_item(&updated, &declaration)?;
            Ok((updated, item_size, declaration))
        })(
        );
        SemanticDecision::try_from(result)
    }

    fn accept_updated_item(
        self,
        old: ObservedWrite,
        lifecycle: GsiProjectionLifecycle,
        (updated, item_size, declaration): (AttributeMap, LogicalItemSize, IndexerDeclaration),
        old_response: Option<AttributeMap>,
    ) -> StorageResult<(AcceptedUpsert, UpdateReturnShape)> {
        let new_item = storage_types::VerifiedWireItem::encode_key_complete(&updated)?;
        let attributes = match self.shape.return_values {
            Some(ReturnValuesOldNewUpdated::AllOld | ReturnValuesOldNewUpdated::UpdatedOld) => {
                old_response
            }
            Some(ReturnValuesOldNewUpdated::AllNew) => Some(updated),
            Some(ReturnValuesOldNewUpdated::UpdatedNew) => {
                updated_attribute_map(&self.shape.operations, updated)
            }
            None | Some(ReturnValuesOldNewUpdated::None) => None,
        };
        let prepared_gsi =
            storage_backfill::PreparedGsiPut::try_new(&self.table.info, &new_item, &declaration)
                .map_err(StorageError::for_index_key_update)?;
        let history_route =
            crate::dynamo_write_execution::prepare_history_route(&self.table, &self.key)?;
        let accepted = AcceptedUpsert {
            history_route,
            ttl: crate::dynamo_put_item::UpsertTtlPreparation::ObservedItem,
            table: self.table,
            key: self.key,
            new_item,
            item_size,
            old_item: old.item,
            declaration,
            old_indexers: old.indexers,
            old_provenance: old.provenance,
            old_marker: old.marker,
            regional: None,
            gsi_lifecycle: lifecycle,
            prepared_gsi,
        };
        Ok((accepted, UpdateReturnShape { attributes }))
    }
}

/// The response attributes, shaped from the maps the accept step already held.
pub struct UpdateReturnShape {
    attributes: Option<AttributeMap>,
}

impl UpdateReturnShape {
    pub(crate) fn into_response(self) -> UpdateItemResponse {
        UpdateItemResponse {
            attributes: self.attributes,
            consumed_capacity: None,
        }
    }
}
