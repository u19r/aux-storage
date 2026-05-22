use storage_types::{
    StorageError, StorageResult, TableName, TransactEncodeItem, TransactWriteItem,
};

pub(crate) fn transact_item_table_name(item: &TransactWriteItem) -> StorageResult<TableName> {
    if let Some(put) = item.put.as_ref() {
        return Ok(put.table_name.clone());
    }
    if let Some(update) = item.update.as_ref() {
        return Ok(update.table_name.clone());
    }
    if let Some(delete) = item.delete.as_ref() {
        return Ok(delete.table_name.clone());
    }
    if let Some(check) = item.condition_check.as_ref() {
        return Ok(check.table_name.clone());
    }
    Err(StorageError::validation(
        "transaction item must contain put, update, delete, or condition_check",
    ))
}

pub(crate) fn set_transact_item_table_name(item: &mut TransactWriteItem, table_name: TableName) {
    if let Some(put) = item.put.as_mut() {
        put.table_name = table_name.clone();
    }
    if let Some(update) = item.update.as_mut() {
        update.table_name = table_name.clone();
    }
    if let Some(delete) = item.delete.as_mut() {
        delete.table_name = table_name.clone();
    }
    if let Some(check) = item.condition_check.as_mut() {
        check.table_name = table_name;
    }
}
pub(crate) fn transact_encode_item_table_name(
    item: &TransactEncodeItem,
) -> StorageResult<TableName> {
    if let Some(put) = item.put.as_ref() {
        return Ok(put.table_name.clone());
    }
    if let Some(update) = item.update.as_ref() {
        return Ok(update.table_name.clone());
    }
    if let Some(delete) = item.delete.as_ref() {
        return Ok(delete.table_name.clone());
    }
    if let Some(check) = item.condition_check.as_ref() {
        return Ok(check.table_name.clone());
    }
    Err(StorageError::validation(
        "transaction item must contain put, update, delete, or condition_check",
    ))
}

pub(crate) fn set_transact_encode_item_table_name(
    item: &mut TransactEncodeItem,
    table_name: TableName,
) {
    if let Some(put) = item.put.as_mut() {
        put.table_name = table_name.clone();
    }
    if let Some(update) = item.update.as_mut() {
        update.table_name = table_name.clone();
    }
    if let Some(delete) = item.delete.as_mut() {
        delete.table_name = table_name.clone();
    }
    if let Some(check) = item.condition_check.as_mut() {
        check.table_name = table_name;
    }
}
