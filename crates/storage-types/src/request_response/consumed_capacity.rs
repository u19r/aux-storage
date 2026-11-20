//! Zero-valued `ConsumedCapacity` shapes.
//!
//! aux-storage does no capacity accounting. A request that asks for
//! `ReturnConsumedCapacity` `TOTAL` or `INDEXES` gets the AWS-shaped object
//! with every unit value `0`, built from the request alone. A request that
//! omits the parameter or sends `NONE` gets nothing and builds nothing.

use crate::{
    ConsumedCapacity, ConsumedCapacityMetrics, IndexName, ReturnConsumedCapacity, TableName, VecMap,
};

/// Which unit field a multi-table answer carries next to `CapacityUnits`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityUnitField {
    /// `CapacityUnits` only (BatchGetItem, BatchWriteItem).
    Plain,
    /// `CapacityUnits` and `ReadCapacityUnits` (TransactGetItems).
    Read,
    /// `CapacityUnits` and `WriteCapacityUnits` (TransactWriteItems).
    Write,
}

impl CapacityUnitField {
    fn metrics(self) -> ConsumedCapacityMetrics {
        ConsumedCapacityMetrics {
            capacity_units: 0.0,
            read_capacity_units: matches!(self, Self::Read).then_some(0.0),
            write_capacity_units: matches!(self, Self::Write).then_some(0.0),
        }
    }
}

impl ConsumedCapacity {
    /// Zero answer for one table (point operations, Query, Scan); `None`
    /// unless the mode is `TOTAL` or `INDEXES`. `index` is the Query/Scan
    /// `IndexName`, listed under `GlobalSecondaryIndexes` for `INDEXES`.
    #[must_use]
    pub fn zero(
        mode: Option<ReturnConsumedCapacity>,
        table_name: &TableName,
        index: Option<&IndexName>,
    ) -> Option<Self> {
        Self::zero_with(mode, table_name, index, CapacityUnitField::Plain)
    }

    /// Zero answers for the distinct tables of a batch or transaction, one
    /// entry per canonical table keeping the first requested spelling,
    /// ordered by canonical name descending. `None` unless the mode is
    /// `TOTAL` or `INDEXES`.
    #[must_use]
    pub fn zero_for_tables<'a>(
        mode: Option<ReturnConsumedCapacity>,
        field: CapacityUnitField,
        tables: impl IntoIterator<Item = &'a TableName>,
    ) -> Option<Vec<Self>> {
        if !Self::asked(mode) {
            return None;
        }
        let mut distinct: Vec<&TableName> = Vec::new();
        for table in tables {
            let canonical = table.dynamodb_resource_name();
            if !distinct
                .iter()
                .any(|seen| seen.dynamodb_resource_name() == canonical)
            {
                distinct.push(table);
            }
        }
        distinct.sort_by(|left, right| {
            right
                .dynamodb_resource_name()
                .cmp(left.dynamodb_resource_name())
        });
        Some(
            distinct
                .into_iter()
                .filter_map(|table| Self::zero_with(mode, table, None, field))
                .collect(),
        )
    }

    fn asked(mode: Option<ReturnConsumedCapacity>) -> bool {
        matches!(
            mode,
            Some(ReturnConsumedCapacity::Total | ReturnConsumedCapacity::Indexes)
        )
    }

    fn zero_with(
        mode: Option<ReturnConsumedCapacity>,
        table_name: &TableName,
        index: Option<&IndexName>,
        field: CapacityUnitField,
    ) -> Option<Self> {
        if !Self::asked(mode) {
            return None;
        }
        let metrics = field.metrics();
        let indexes = mode == Some(ReturnConsumedCapacity::Indexes);
        Some(Self {
            table_name: table_name.clone(),
            capacity_units: 0.0,
            read_capacity_units: metrics.read_capacity_units,
            write_capacity_units: metrics.write_capacity_units,
            table: indexes.then(|| metrics.clone()),
            global_secondary_indexes: index.filter(|_| indexes).map(|name| {
                let mut map = VecMap::with_capacity(1);
                map.insert(
                    name.to_string(),
                    ConsumedCapacityMetrics {
                        capacity_units: 0.0,
                        read_capacity_units: None,
                        write_capacity_units: None,
                    },
                );
                map
            }),
        })
    }
}
