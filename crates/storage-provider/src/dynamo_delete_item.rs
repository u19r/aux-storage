//! Shared DeleteItem validation, protected observation, and physical effects.
//!
//! A family lowerer observes the current item and GSI lifecycle in the native
//! write attempt. The shared decision then chooses condition, revision, TTL,
//! index, stream, and response effects before that attempt settles.

use std::sync::Arc;

use storage_backfill::{GsiProjectionLifecycle, GsiProjectionWriteBatch};
use storage_common::{TtlConfigRecord, TtlIndexWriteBatch};
use storage_types::{
    AllOld, BoundTableInfo, DeleteItemRequest, DeleteItemResponse, DynamoRequestValidate as _,
    ItemProvenance, ItemRevision, KeyAttributes, StorageError, StorageResult,
    StreamRetentionDuration, TableName, return_values_on_condition_check_failure_all_old,
    validate_transact_key,
};

use crate::{
    DynamoStreamWriteAttempt, ItemPrimaryEffects, ItemWriteObservation,
    ItemWriteObservationAttempt, SemanticDecision, UncheckedCondition,
    dynamo_put_item::{ObservedWrite, check_write_condition, old_image_for_response},
};

/// A physical delete lowerer uses the same protected read as PutItem. It must
/// persist the tombstone revision, TTL removal, GSI removal, and later stream
/// effects before acknowledging the native attempt.
pub trait DeleteItemAttempt: ItemWriteObservationAttempt + DynamoStreamWriteAttempt {
    fn stage_primary_delete(
        &mut self,
        item: &AcceptedDeleteItem,
        effects: &ItemPrimaryEffects,
        writes: TtlIndexWriteBatch,
    ) -> StorageResult<()>;
}

/// Request facts validated before opening the native write attempt.
#[derive(Clone)]
pub struct DeleteItemShape {
    pub table_name: TableName,
    key: KeyAttributes,
    condition: Option<UncheckedCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
    item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl DeleteItemShape {
    pub(crate) fn key(&self) -> &KeyAttributes {
        &self.key
    }

    pub fn try_from_request(request: DeleteItemRequest) -> StorageResult<Self> {
        request
            .validate_for_dynamodb()
            .map_err(StorageError::raw_validation)?;
        Self::from_validated_request(request)
    }

    /// Parent batch/transaction validation covers member request fields.
    pub(crate) fn from_validated_request(request: DeleteItemRequest) -> StorageResult<Self> {
        let condition = UncheckedCondition::parse(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
        Ok(Self {
            table_name: request.table_name,
            key: request.key,
            condition,
            return_old: matches!(request.return_values, Some(AllOld::AllOld)),
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
            item_stream_ttl_hours: request.aux_item_stream_ttl_hours,
        })
    }

    pub fn bind_table(mut self, table: Arc<BoundTableInfo>) -> StorageResult<BoundDeleteItem> {
        if self.key.len() != table.info.key_schema.len()
            || table
                .info
                .key_schema
                .iter()
                .any(|element| !self.key.contains_key(&element.attribute_name))
        {
            return Err(StorageError::validation(
                "One or more parameter values were invalid",
            ));
        }
        validate_transact_key(&table.info, &self.key)?;
        self.key.normalize_numbers_for_write();
        let route = stream_provider::DynamoMutationRoute::try_new(
            &table.info.table_name,
            &table.info.key_schema,
            &self.key,
        )?;
        Ok(BoundDeleteItem {
            route,
            table,
            key: self.key,
            condition: self.condition,
            return_old: self.return_old,
            return_old_on_condition_failure: self.return_old_on_condition_failure,
            item_stream_ttl_hours: self.item_stream_ttl_hours,
        })
    }
}

pub struct BoundDeleteItem {
    route: stream_provider::DynamoMutationRoute,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    condition: Option<UncheckedCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
    item_stream_ttl_hours: Option<StreamRetentionDuration>,
}

impl BoundDeleteItem {
    pub fn for_maintenance(
        table: Arc<BoundTableInfo>,
        mut key: KeyAttributes,
    ) -> StorageResult<Self> {
        validate_transact_key(&table.info, &key)?;
        key.normalize_numbers_for_write();
        let route = stream_provider::DynamoMutationRoute::try_new(
            &table.info.table_name,
            &table.info.key_schema,
            &key,
        )?;
        Ok(Self {
            route,
            table,
            key,
            condition: None,
            return_old: false,
            return_old_on_condition_failure: false,
            item_stream_ttl_hours: None,
        })
    }

    pub fn accept_old(
        self,
        observation: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<AcceptedDeleteItem>> {
        let old = ObservedWrite::try_new(observation, &self.key)?;
        if let SemanticDecision::Rejected(error) = check_write_condition(
            self.condition.as_ref(),
            old.item.as_deref(),
            self.return_old_on_condition_failure,
        )? {
            return Ok(SemanticDecision::Rejected(error));
        }
        Ok(SemanticDecision::Accepted(AcceptedDeleteItem {
            route: self.route,
            table: self.table,
            key: self.key,
            old_item: old.item,
            old_indexers: old.indexers,
            old_revision: old.revision,
            old_provenance: old.provenance,
            item_stream_ttl_hours: self.item_stream_ttl_hours,
            gsi_lifecycle: lifecycle,
            return_old: self.return_old,
        }))
    }
}

/// Accepted logical deletion. The caller must stage every physical effect on
/// the same attempt before it can return this response.
pub struct AcceptedDeleteItem {
    pub(crate) route: stream_provider::DynamoMutationRoute,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub old_indexers: Vec<String>,
    pub old_revision: Option<ItemRevision>,
    pub old_provenance: Option<ItemProvenance>,
    pub item_stream_ttl_hours: Option<StreamRetentionDuration>,
    gsi_lifecycle: GsiProjectionLifecycle,
    return_old: bool,
}

impl AcceptedDeleteItem {
    pub fn primary_effects_at_least(
        &self,
        floor: ItemRevision,
        provenance: ItemProvenance,
    ) -> StorageResult<ItemPrimaryEffects> {
        ItemPrimaryEffects::for_delete(self.old_revision, floor, provenance)
    }

    pub fn gsi_writes(&self, revision: ItemRevision) -> StorageResult<GsiProjectionWriteBatch> {
        self.gsi_lifecycle.plan_immediate_writes(
            &self.table.info,
            self.old_item.as_deref(),
            None,
            &[],
            revision,
        )
    }

    pub fn ttl_writes(
        &self,
        config: Option<&TtlConfigRecord>,
    ) -> StorageResult<TtlIndexWriteBatch> {
        TtlIndexWriteBatch::for_wire_items(&self.table.info, config, self.old_item.as_deref(), None)
    }

    pub fn response(&self) -> StorageResult<DeleteItemResponse> {
        let attributes = old_image_for_response(self.old_item.as_deref(), self.return_old)?;
        Ok(DeleteItemResponse { attributes })
    }
}
