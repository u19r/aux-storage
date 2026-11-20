//! Metadata-bound TransactGet preflight and ordered reads (phase 5, §3.2).
//!
//! `PreparedTransactGet` borrows the validated plan and owns one metadata
//! observation per distinct table. `reads` yields the request-ordered
//! observation keys for the I/O wave; `finish` builds the ordered response
//! with the moved projection and the zero capacity shape.

use storage_types::{
    AttributeProjection, CapacityUnitField, ConsumedCapacity, ItemResponse, KeyAttributes,
    StorageError, StorageResult, StoredTableInfo, TransactGetItemsResponse, WireItem,
};

use super::plan::TransactGetPlan;

/// TransactGet plan bound to one metadata observation per distinct table.
pub struct PreparedTransactGet<'plan> {
    plan: &'plan TransactGetPlan,
    tables: Vec<StoredTableInfo>,
}

impl<'plan> PreparedTransactGet<'plan> {
    pub(super) fn bound(plan: &'plan TransactGetPlan, tables: Vec<StoredTableInfo>) -> Self {
        Self { plan, tables }
    }

    /// Request-order item reads, including keys expected to be absent. No
    /// I/O and no provider access.
    pub fn reads(&self) -> impl Iterator<Item = (&StoredTableInfo, &KeyAttributes)> {
        (0..self.plan.len()).map(|ordinal| {
            (
                &self.tables[self.plan.member_table(ordinal)],
                &self.plan.member(ordinal).get.key,
            )
        })
    }

    /// Build the ordered response from one observation per requested member,
    /// preserving empty slots and empty-projection behavior. Consumed
    /// capacity is the zero shape per distinct table, only when requested.
    pub fn finish(
        self,
        items: storage_types::Exact<Option<WireItem>>,
    ) -> StorageResult<TransactGetItemsResponse> {
        // AWS refuses a read whose items total more than 4 MB, returning none.
        let mut remaining = storage_types::MAX_TRANSACTION_ITEM_BYTES;
        for item in items.iter().flatten() {
            remaining = remaining
                .checked_sub(u64::from(item.logical_size()?))
                .ok_or_else(|| StorageError::raw_validation("Payload in response is too large."))?;
        }
        let mut responses = Vec::with_capacity(items.len());
        for (ordinal, item) in items.into_iter().enumerate() {
            responses.push(self.project_response_slot(ordinal, item)?);
        }
        Ok(TransactGetItemsResponse {
            responses,
            consumed_capacity: ConsumedCapacity::zero_for_tables(
                self.plan.return_consumed_capacity(),
                CapacityUnitField::Read,
                self.plan.response_table_names(),
            ),
        })
    }

    /// Build one ordered response slot from its wire observation. Owns the
    /// empty-slot and empty-projection invariants: an absent observation
    /// stays `None`, and a projection that matches nothing leaves the slot
    /// empty rather than returning an empty map.
    fn project_response_slot(
        &self,
        ordinal: usize,
        item: Option<WireItem>,
    ) -> StorageResult<ItemResponse> {
        let member = self.plan.member(ordinal);
        let item = match (item, member.get.projection_expression.as_deref()) {
            (Some(item), Some(projection_expression)) => AttributeProjection::from_expression(
                projection_expression,
                member.get.expression_attribute_names.as_ref(),
            )
            .project_wire_nonempty(&item)?,
            (Some(item), None) => Some(item),
            (None, _) => None,
        };
        Ok(ItemResponse { item })
    }
}
