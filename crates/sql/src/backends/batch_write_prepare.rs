use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{
    DeleteRequest, PreparedBatchOperation, PutRequest, StorageError, StorageResult,
    StoredTableInfo, WriteRequest,
};

pub(crate) fn prepare_batch_operation(
    table_info: &StoredTableInfo,
    write_request: WriteRequest,
) -> StorageResult<PreparedBatchOperation> {
    match write_request {
        WriteRequest {
            put_request: Some(PutRequest { ref item }),
            delete_request: None,
        } => {
            if item.is_empty() {
                return Err(StorageError::validation(
                    "One or more parameter values were invalid: An AttributeValue may not contain \
                     an empty string",
                ));
            }

            let split = split_item_into_key_and_attributes_sync(item.clone(), table_info)?;

            Ok(PreparedBatchOperation::Put {
                table_name: table_info.table_name.clone(),
                table_info: table_info.clone(),
                write_request,
                key_attributes: split.key_attributes,
                non_key_attributes: split.non_key_attributes,
                full_item: split.all_attributes,
            })
        }
        WriteRequest {
            put_request: None,
            delete_request: Some(DeleteRequest { ref key }),
        } => {
            if key.is_empty() {
                return Err(StorageError::validation(
                    "Delete request must specify a key",
                ));
            }

            Ok(PreparedBatchOperation::Delete {
                table_name: table_info.table_name.clone(),
                table_info: table_info.clone(),
                key: key.clone(),
                write_request,
                existing_item: None,
            })
        }
        _ => Err(StorageError::validation(
            "Each WriteRequest must contain exactly one of PutRequest or DeleteRequest",
        )),
    }
}
