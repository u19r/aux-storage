//! Ordered TransactGet consumed-capacity construction.
//!
//! [`TransactGetCapacityCount`] owns one table's share of the capacity
//! answer plus the accumulation/rendering behavior over those shares;
//! called only from
//! [`PreparedTransactGet::finish`](super::prepared::PreparedTransactGet::finish).
//! Moved from the manager fallback without change: counts group by distinct
//! table keeping the first member's requested spelling, order sorts by
//! canonical name descending, and `NONE`/`TOTAL`/`INDEXES` keep their exact
//! shapes.

use storage_types::{ReturnConsumedCapacity, TableName};

/// One table's share of a TransactGet capacity answer: the distinct-table
/// index for canonical ordering plus the first member's requested spelling.
pub(super) struct TransactGetCapacityCount {
    table_index: usize,
    response_table_name: TableName,
    count: usize,
}

impl TransactGetCapacityCount {
    /// Whether this request mode tracks per-table counts. Owns the
    /// `TOTAL`/`INDEXES`-only tracking invariant.
    pub(super) fn should_track(return_consumed_capacity: Option<ReturnConsumedCapacity>) -> bool {
        matches!(
            return_consumed_capacity,
            Some(ReturnConsumedCapacity::Total | ReturnConsumedCapacity::Indexes)
        )
    }

    /// Accumulate one member observation into its table's share. Owns the
    /// group-by-distinct-table invariant: the first member's requested
    /// spelling is kept, later members only bump the count.
    pub(super) fn record(
        counts: &mut Vec<Self>,
        table_index: usize,
        response_table_name: &TableName,
    ) {
        if let Some(entry) = counts
            .iter_mut()
            .find(|entry| entry.table_index == table_index)
        {
            entry.accumulate();
            return;
        }
        counts.push(Self {
            table_index,
            response_table_name: response_table_name.clone(),
            count: 1,
        });
    }

    /// Render the final capacity answer from the accumulated shares. Owns
    /// the grouping/order contract and the exact `NONE`/`TOTAL`/`INDEXES`
    /// shapes.
    pub(super) fn render(
        return_consumed_capacity: Option<ReturnConsumedCapacity>,
        counts: &[Self],
        canonical_table_names: &[TableName],
    ) -> Option<serde_json::Value> {
        let mode = return_consumed_capacity?;
        if mode == ReturnConsumedCapacity::None {
            return None;
        }
        let counts = Self::ordered(counts, canonical_table_names);
        match mode {
            ReturnConsumedCapacity::Total => Some(serde_json::Value::Array(
                counts.iter().map(|entry| entry.total_entry()).collect(),
            )),
            ReturnConsumedCapacity::Indexes => Some(serde_json::Value::Array(
                counts.iter().map(|entry| entry.indexes_entry()).collect(),
            )),
            ReturnConsumedCapacity::None => None,
        }
    }

    /// Count one more member against this table's share.
    fn accumulate(&mut self) {
        self.count += 1;
    }

    /// Order capacity shares by canonical table name descending. Owns the
    /// response ordering invariant; counting keeps the first member's
    /// requested spelling separately.
    fn ordered<'counts>(
        counts: &'counts [Self],
        canonical_table_names: &[TableName],
    ) -> Vec<&'counts Self> {
        let mut ordered = counts.iter().collect::<Vec<_>>();
        ordered.sort_by(|left, right| {
            canonical_table_names[right.table_index]
                .as_ref()
                .cmp(canonical_table_names[left.table_index].as_ref())
        });
        ordered
    }

    /// Exact `TOTAL` entry shape: doubled capacity units under the member's
    /// requested table spelling.
    fn total_entry(&self) -> serde_json::Value {
        let capacity_units = (self.count as f64) * 2.0;
        serde_json::json!({
            "TableName": self.response_table_name,
            "CapacityUnits": capacity_units,
            "ReadCapacityUnits": capacity_units
        })
    }

    /// Exact `INDEXES` entry shape: the `TOTAL` fields plus the nested table
    /// breakdown carrying the same doubled units.
    fn indexes_entry(&self) -> serde_json::Value {
        let capacity_units = (self.count as f64) * 2.0;
        serde_json::json!({
            "TableName": self.response_table_name,
            "CapacityUnits": capacity_units,
            "ReadCapacityUnits": capacity_units,
            "Table": {
                "ReadCapacityUnits": capacity_units,
                "CapacityUnits": capacity_units
            }
        })
    }
}
