//! Canonical CAS conditions for an entity owner's captured fields.
use crate::{
    AttributeMap, StorageError, StorageResult, TableName, TransactConditionCheckRequest,
    TryIntoWireItem, VecMap,
    single_table_entity::{SingleTableEntity, to_wire_item_fast},
};

pub fn prepare_captured_condition<T: SingleTableEntity + TryIntoWireItem>(
    entity: &T,
    table: TableName,
    fields: &[&str],
) -> StorageResult<TransactConditionCheckRequest> {
    let item = to_wire_item_fast(entity)
        .map_err(|_| StorageError::internal("captured entity encoding failed"))?
        .into_attribute_map()
        .map_err(|_| StorageError::internal("captured entity encoding failed"))?;
    let mut names = VecMap::new();
    let mut values = AttributeMap::new();
    let mut conditions = Vec::with_capacity(fields.len());
    for (index, field) in fields.iter().enumerate() {
        let name = format!("#captured{index}");
        let value = format!(":captured{index}");
        names.insert(name.clone(), (*field).to_owned());
        match item.get(field) {
            Some(attribute) => {
                conditions.push(format!("{name} = {value}"));
                values.insert(value, attribute.clone());
            }
            None => conditions.push(format!("attribute_not_exists({name})")),
        }
    }
    Ok(TransactConditionCheckRequest {
        table_name: table,
        key: entity.entity_key().into_map().into(),
        condition_expression: conditions.join(" AND "),
        expression_attribute_names: Some(names),
        expression_attribute_values: Some(values),
        return_values_on_condition_check_failure: None,
    })
}
