use storage_types::{IndexName, KeySchemaElement, StorageResult, StoredTableInfo, TableName};

use crate::{errors::missing_index_error, naming, read_path::RowOrigin};

#[derive(Debug, Clone)]
pub(crate) struct ReadTargetPlan {
    pub(crate) physical_name: String,
    pub(crate) origin: RowOrigin,
    pub(crate) key_schema: Vec<KeySchemaElement>,
    pub(crate) table_key_schema_for_index: Option<Vec<KeySchemaElement>>,
}

pub(crate) fn plan_read_target(
    table_name: &TableName,
    table_info: &StoredTableInfo,
    index_name: &Option<IndexName>,
) -> StorageResult<ReadTargetPlan> {
    if let Some(index_name) = index_name {
        let gsi = table_info
            .global_secondary_indexes
            .as_ref()
            .and_then(|indexes| indexes.iter().find(|index| index.index_name == *index_name))
            .ok_or_else(|| missing_index_error(table_info, index_name))?;

        return Ok(ReadTargetPlan {
            physical_name: naming::physical_gsi_table_name(table_name, index_name),
            origin: RowOrigin::Gsi,
            key_schema: gsi.key_schema.clone(),
            table_key_schema_for_index: Some(table_info.key_schema.clone()),
        });
    }

    Ok(ReadTargetPlan {
        physical_name: naming::physical_table_name(table_name),
        origin: RowOrigin::Main,
        key_schema: table_info.key_schema.clone(),
        table_key_schema_for_index: None,
    })
}
