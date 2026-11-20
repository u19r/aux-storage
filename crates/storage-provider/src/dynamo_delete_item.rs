//! Shared DeleteItem validation, protected observation, and physical effects.
//!
//! A family lowerer observes the current item and GSI lifecycle in the native
//! write attempt. The shared decision then chooses condition, TTL,
//! index, stream, and response effects before that attempt settles.

use std::sync::Arc;

use storage_backfill::{GsiProjectionLifecycle, GsiProjectionWriteBatch};
use storage_common::TtlIndexWriteBatch;
use storage_types::{
    AllOld, BoundTableInfo, DeleteItemRequest, DeleteItemResponse, DynamoRequestValidate as _,
    ItemProvenance, KeyAttributes, RegionalMarker, StorageError, StorageResult, TableName,
    condition::CompiledCondition, return_values_on_condition_check_failure_all_old,
    validate_transact_key,
};

use crate::{
    ItemWriteObservation, SemanticDecision,
    dynamo_put_item::{ObservedWrite, check_write_condition, old_image_for_response},
};

/// Request facts validated before opening the native write attempt.
pub struct DeleteItemShape {
    pub table_name: TableName,
    key: KeyAttributes,
    condition: Option<CompiledCondition>,
    return_old: bool,
    return_old_on_condition_failure: bool,
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
    pub(crate) fn from_validated_request(mut request: DeleteItemRequest) -> StorageResult<Self> {
        let condition = CompiledCondition::compile_write(
            request.condition_expression,
            request.expression_attribute_names,
            request.expression_attribute_values,
        )?;
        request.key.normalize_numbers_for_write();
        Ok(Self {
            table_name: request.table_name,
            key: request.key,
            condition,
            return_old: matches!(request.return_values, Some(AllOld::AllOld)),
            return_old_on_condition_failure: return_values_on_condition_check_failure_all_old(
                request.return_values_on_condition_check_failure.as_ref(),
            ),
        })
    }

    pub fn bind_table(
        self: Arc<Self>,
        table: Arc<BoundTableInfo>,
    ) -> StorageResult<BoundDeleteItem> {
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
        let history_route =
            crate::dynamo_write_execution::prepare_history_route(&table, &self.key)?;
        Ok(BoundDeleteItem {
            history_route,
            table,
            key: self.key.clone(),
            shape: self,
        })
    }
}

pub struct BoundDeleteItem {
    history_route: Option<stream_provider::DynamoMutationRoute>,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    shape: Arc<DeleteItemShape>,
}

impl BoundDeleteItem {
    pub fn accept_old(
        self,
        observation: ItemWriteObservation,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<SemanticDecision<AcceptedDeleteItem>> {
        let old = ObservedWrite::try_new(observation, &self.key)?;
        if let SemanticDecision::Rejected(error) = check_write_condition(
            self.shape.condition.as_ref(),
            old.item.as_deref(),
            self.shape.return_old_on_condition_failure,
        )? {
            return Ok(SemanticDecision::Rejected(error));
        }
        let mut accepted = AcceptedDeleteItem::from_observed(
            self.table,
            self.key,
            self.history_route,
            old,
            lifecycle,
            crate::DeleteCause::Customer,
        );
        accepted.return_old = self.shape.return_old;
        Ok(SemanticDecision::Accepted(accepted))
    }
}

/// Accepted logical deletion. The caller must stage every physical effect on
/// the same attempt before it can return this response.
pub struct AcceptedDeleteItem {
    pub(crate) history_route: Option<stream_provider::DynamoMutationRoute>,
    pub table: Arc<BoundTableInfo>,
    pub key: KeyAttributes,
    pub old_item: Option<storage_types::VerifiedWireItem>,
    pub old_indexers: Vec<String>,
    pub old_provenance: Option<ItemProvenance>,
    pub old_marker: Option<RegionalMarker>,
    /// Set by `prepare_accepted_delete`; read by the backend lowering.
    pub regional: Option<crate::RegionalDelete>,
    pub cause: crate::DeleteCause,
    gsi_lifecycle: GsiProjectionLifecycle,
    return_old: bool,
    pub user_identity: Option<storage_types::StreamUserIdentity>,
}

impl AcceptedDeleteItem {
    /// Maintenance callers already checked this key against the current table
    /// before reading. Accept the verified protected image for deletion
    /// without constructing a customer request or evaluating a condition.
    pub(crate) fn try_from_observed(
        table: Arc<BoundTableInfo>,
        key: KeyAttributes,
        old: ObservedWrite,
        lifecycle: GsiProjectionLifecycle,
    ) -> StorageResult<Self> {
        let history_route = crate::dynamo_write_execution::prepare_history_route(&table, &key)?;
        Ok(Self::from_observed(
            table,
            key,
            history_route,
            old,
            lifecycle,
            crate::DeleteCause::Expiration,
        ))
    }

    fn from_observed(
        table: Arc<BoundTableInfo>,
        key: KeyAttributes,
        history_route: Option<stream_provider::DynamoMutationRoute>,
        old: ObservedWrite,
        lifecycle: GsiProjectionLifecycle,
        cause: crate::DeleteCause,
    ) -> Self {
        Self {
            history_route,
            table,
            key,
            old_item: old.item,
            old_indexers: old.indexers,
            old_provenance: old.provenance,
            old_marker: old.marker,
            regional: None,
            cause,
            gsi_lifecycle: lifecycle,
            return_old: false,
            user_identity: None,
        }
    }

    pub fn gsi_writes(&self) -> StorageResult<GsiProjectionWriteBatch> {
        self.gsi_lifecycle.plan_immediate_writes(
            &self.table.info,
            self.old_item.as_deref(),
            None,
            &[],
        )
    }

    pub fn ttl_writes(&self) -> StorageResult<TtlIndexWriteBatch> {
        TtlIndexWriteBatch::for_wire_items(
            &self.table.info,
            self.table.binding.ttl.as_ref(),
            self.old_item.as_deref(),
            None,
        )
    }

    pub fn response(&self) -> StorageResult<DeleteItemResponse> {
        let attributes = old_image_for_response(self.old_item.as_deref(), self.return_old)?;
        Ok(DeleteItemResponse {
            attributes,
            consumed_capacity: None,
        })
    }
}
