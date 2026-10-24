//! Shared UpdateItem decision; PutItem owns the same physical upsert effects.

use std::sync::Arc;

use storage_backfill::GsiProjectionLifecycle;
use storage_types::{
    AttributeMap, BoundTableInfo, DynamoRequestValidate, IndexedWireItem, IndexerDeclaration,
    KeyAttributes, LogicalItemSize, ReturnValuesOldNewUpdated, StorageError, StorageResult,
    StreamRetentionDuration, TableName, UpdateItemRequest, UpdateItemResponse, WireItem,
    return_values_on_condition_check_failure_all_old, validate_transact_key,
};

use crate::{
    AcceptedUpsert, ItemWriteObservation, SemanticDecision, UncheckedCondition,
    dynamo_put_item::{ObservedWrite, check_write_condition},
    update_logic::{
        UpdateOperation, apply_update_operations, parse_update_expression,
        update_item_response_attributes,
    },
};

#[derive(Clone)]
pub struct UpdateItemShape {
    pub table_name: TableName,
    key: KeyAttributes,
    operations: Vec<UpdateOperation>,
    indexers: Option<Vec<String>>,
    condition: Option<UncheckedCondition>,
    return_values: Option<ReturnValuesOldNewUpdated>,
    return_old_on_condition_failure: bool,
    item_stream_ttl_hours: Option<StreamRetentionDuration>,
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
    pub(crate) fn from_validated_request(request: UpdateItemRequest) -> StorageResult<Self> {
        let operations = match request.update_expression.as_deref() {
            Some(expression) => parse_update_expression(
                expression,
                request.expression_attribute_names.as_ref(),
                request.expression_attribute_values.as_ref(),
            )?,
            None => Vec::new(),
        };
        let condition = UncheckedCondition::parse(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
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
            item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
    }

    pub fn bind_table(mut self, table: Arc<BoundTableInfo>) -> StorageResult<BoundUpdateItem> {
        validate_transact_key(&table.info, &self.key)?;
        self.key.normalize_numbers_for_write();
        let route = stream_provider::DynamoMutationRoute::try_new(
            &table.info.table_name,
            &table.info.key_schema,
            &self.key,
        )?;
        Ok(BoundUpdateItem {
            route,
            table,
            key: self.key,
            operations: self.operations,
            indexers: self.indexers,
            condition: self.condition,
            return_values: self.return_values,
            return_old_on_condition_failure: self.return_old_on_condition_failure,
            item_stream_ttl_hours: self.item_stream_ttl_hours,
        })
    }
}

pub struct BoundUpdateItem {
    route: stream_provider::DynamoMutationRoute,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    operations: Vec<UpdateOperation>,
    indexers: Option<Vec<String>>,
    condition: Option<UncheckedCondition>,
    return_values: Option<ReturnValuesOldNewUpdated>,
    return_old_on_condition_failure: bool,
    item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl BoundUpdateItem {
    pub fn accept_old(
        mut self,
        observed: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<(AcceptedUpsert, UpdateReturnShape)>> {
        let old = ObservedWrite::try_new(observed, &self.key)?;
        if let SemanticDecision::Rejected(error) = check_write_condition(
            self.condition.as_ref(),
            old.item.as_deref(),
            self.return_old_on_condition_failure,
        )? {
            return Ok(SemanticDecision::Rejected(error));
        }
        let base = old
            .item
            .as_deref()
            .map(AttributeMap::try_from)
            .transpose()?
            .unwrap_or_else(|| AttributeMap::from(&self.key));
        let indexers = self.indexers.take().unwrap_or_else(|| old.indexers.clone());
        let updated = match self.evaluate_updated_item(base, indexers)? {
            SemanticDecision::Accepted(updated) => updated,
            SemanticDecision::Rejected(error) => return Ok(SemanticDecision::Rejected(error)),
        };
        SemanticDecision::try_from(self.accept_updated_item(old, lifecycle, updated))
    }

    fn evaluate_updated_item(
        &self,
        base: AttributeMap,
        indexers: Vec<String>,
    ) -> StorageResult<SemanticDecision<(AttributeMap, LogicalItemSize, IndexerDeclaration)>> {
        let result: StorageResult<(AttributeMap, LogicalItemSize, IndexerDeclaration)> = (|| {
            let mut updated =
                apply_update_operations(base, &self.operations)?.with_verified_keys(&self.key)?;
            updated.normalize_numbers_for_write();
            let item_size = LogicalItemSize::try_from_entries(updated.iter())?;
            let declaration = IndexerDeclaration::try_new(indexers, self.table.info.max_indexers)?;
            IndexedWireItem::validate_logical_item(&updated, &declaration)?;
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
    ) -> StorageResult<(AcceptedUpsert, UpdateReturnShape)> {
        let new_item = WireItem::try_from(updated)?;
        let prepared_gsi =
            storage_backfill::PreparedGsiPut::try_new(&self.table.info, &new_item, &declaration)?;
        let stream_image =
            stream_provider::PreparedStreamImage::try_new(&self.route, new_item.clone())?;
        let accepted = AcceptedUpsert {
            ttl: crate::dynamo_put_item::UpsertTtlPreparation::ObservedItem,
            stream_image,
            route: self.route,
            table: self.table,
            key: self.key,
            new_item,
            item_size,
            old_item: old.item,
            declaration,
            old_indexers: old.indexers,
            old_revision: old.revision,
            old_provenance: old.provenance,
            old_ttl_epoch: old.ttl_epoch,
            item_stream_ttl_hours: self.item_stream_ttl_hours,
            gsi_lifecycle: lifecycle,
            prepared_gsi,
        };
        Ok((
            accepted,
            UpdateReturnShape {
                operations: self.operations,
                return_values: self.return_values,
            },
        ))
    }
}

/// Response-only work consumes committed images outside the write connection.
pub struct UpdateReturnShape {
    operations: Vec<UpdateOperation>,
    return_values: Option<ReturnValuesOldNewUpdated>,
}

impl UpdateReturnShape {
    pub(crate) fn finish(
        self,
        item: crate::dynamo_write_execution::UpsertResponseImages,
    ) -> StorageResult<UpdateItemResponse> {
        let old_response = matches!(
            self.return_values,
            Some(ReturnValuesOldNewUpdated::AllOld | ReturnValuesOldNewUpdated::UpdatedOld)
        )
        .then(|| {
            item.old_item
                .as_ref()
                .map(AttributeMap::try_from)
                .transpose()
        })
        .transpose()?
        .flatten();
        let new_response = matches!(
            self.return_values,
            Some(ReturnValuesOldNewUpdated::AllNew | ReturnValuesOldNewUpdated::UpdatedNew)
        )
        .then(|| AttributeMap::try_from(&item.new_item))
        .transpose()?;
        update_item_response_attributes(
            &self.operations,
            old_response,
            new_response,
            self.return_values.as_ref(),
        )
    }
}
