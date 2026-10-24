//! Metadata-bound TransactGet preflight and ordered reads (phase 5, §3.2).
//!
//! `PreparedTransactGet` borrows the validated plan and owns one metadata
//! observation per distinct table. `reads` yields the request-ordered
//! observation keys for the I/O wave; `finish` builds the ordered response
//! with the moved projection and capacity formatting.

use storage_types::{
    ItemResponse, KeyAttributes, StorageError, StorageResult, StoredTableInfo,
    TransactGetItemsResponse, WireItem,
};

use super::{
    plan::TransactGetPlan, projection::TransactGetProjection, response::TransactGetCapacityCount,
};

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
    /// preserving empty slots, empty-projection behavior, and the current
    /// capacity grouping, order, and `NONE`/`TOTAL`/`INDEXES` formatting.
    pub fn finish(self, items: Vec<Option<WireItem>>) -> StorageResult<TransactGetItemsResponse> {
        if items.len() != self.plan.len() {
            return Err(StorageError::internal(
                "transact-get observation count must match requested members",
            ));
        }
        let return_consumed_capacity = self.plan.return_consumed_capacity();
        let mut counts = Vec::new();
        let mut responses = Vec::with_capacity(items.len());
        for (ordinal, item) in items.into_iter().enumerate() {
            responses.push(self.project_response_slot(ordinal, item)?);
            if TransactGetCapacityCount::should_track(return_consumed_capacity) {
                TransactGetCapacityCount::record(
                    &mut counts,
                    self.plan.member_table(ordinal),
                    self.plan.member_response_table(ordinal),
                );
            }
        }
        Ok(TransactGetItemsResponse {
            responses,
            consumed_capacity: TransactGetCapacityCount::render(
                return_consumed_capacity,
                &counts,
                self.plan.table_names(),
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
            (Some(item), Some(projection_expression)) => {
                let item = item.into_attribute_map()?;
                let projected = TransactGetProjection::project_item(
                    &item,
                    projection_expression,
                    member.get.expression_attribute_names.as_ref(),
                );
                (!projected.is_empty())
                    .then(|| WireItem::try_from(projected))
                    .transpose()?
            }
            (Some(item), None) => Some(item),
            (None, _) => None,
        };
        Ok(ItemResponse { item })
    }
}
