use std::collections::HashMap;

use storage_types::{ConsumedCapacity, ConsumedCapacityMetrics, IndexName, TableName};

pub(crate) fn calculate_consumed_capacity_from_inputs(
    return_consumed_capacity: Option<&str>,
    table_name: &TableName,
    index_name: Option<&IndexName>,
    scanned_count: u32,
) -> Option<ConsumedCapacity> {
    let return_consumed_capacity = return_consumed_capacity?;
    match return_consumed_capacity {
        "TOTAL" => {
            let capacity_units = f64::from(scanned_count).max(0.5);
            Some(ConsumedCapacity {
                table_name: table_name.clone(),
                capacity_units,
                global_secondary_indexes: None,
            })
        }
        "INDEXES" => {
            let capacity_units = f64::from(scanned_count).max(0.5);
            let global_secondary_indexes = index_name.map(|index_name| {
                HashMap::from([(
                    index_name.to_string(),
                    ConsumedCapacityMetrics { capacity_units },
                )])
            });
            Some(ConsumedCapacity {
                table_name: table_name.clone(),
                capacity_units,
                global_secondary_indexes,
            })
        }
        "NONE" => None,
        _ => None,
    }
}

#[cfg(test)]
pub(crate) fn calculate_consumed_capacity_json_baseline_for_tests(
    return_consumed_capacity: Option<&str>,
    table_name: &TableName,
    index_name: Option<&IndexName>,
    scanned_count: u32,
) -> Option<serde_json::Value> {
    let return_consumed_capacity = return_consumed_capacity?;
    match return_consumed_capacity {
        "TOTAL" => {
            let capacity_units = f64::from(scanned_count).max(0.5);
            Some(serde_json::json!({
                "TableName": table_name,
                "CapacityUnits": capacity_units
            }))
        }
        "INDEXES" => {
            let capacity_units = f64::from(scanned_count).max(0.5);
            let mut consumed_capacity = serde_json::json!({
                "TableName": table_name,
                "CapacityUnits": capacity_units
            });
            if let Some(index_name) = index_name {
                consumed_capacity["GlobalSecondaryIndexes"] = serde_json::json!({
                    index_name: {
                        "CapacityUnits": capacity_units
                    }
                });
            }
            Some(consumed_capacity)
        }
        "NONE" => None,
        _ => None,
    }
}
