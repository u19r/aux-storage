use storage_provider::split_item_into_key_and_attributes_sync;
use storage_types::{
    DeleteRequest, PreparedBatchOperation, PutRequest, StorageEnum, StorageError, StorageResult,
    StoredTableInfo, WriteRequest, context::WrappedError as _, validate_transact_key,
    validate_transact_put_item_key,
};

pub(crate) fn prepare_batch_operation(
    table_info: &StoredTableInfo,
    write_request: WriteRequest,
) -> StorageResult<PreparedBatchOperation> {
    match write_request {
        WriteRequest {
            put_request:
                Some(PutRequest {
                    ref item,
                    aux_item_stream_ttl_hours,
                }),
            delete_request: None,
        } => {
            if item.is_empty() {
                return Err(StorageError::validation(
                    "One or more parameter values were invalid: An AttributeValue may not contain \
                     an empty string",
                ));
            }

            let split = split_item_into_key_and_attributes_sync(item.clone(), table_info)?;
            validate_transact_put_item_key(table_info, item).map_err(batch_write_key_error)?;

            Ok(PreparedBatchOperation::Put {
                table_name: table_info.table_name.clone(),
                table_info: table_info.clone(),
                write_request,
                key_attributes: split.key_attributes,
                non_key_attributes: split.non_key_attributes,
                full_item: split.all_attributes,
                aux_item_stream_ttl_hours,
            })
        }
        WriteRequest {
            put_request: None,
            delete_request:
                Some(DeleteRequest {
                    ref key,
                    aux_item_stream_ttl_hours,
                }),
        } => {
            if key.is_empty() {
                return Err(StorageError::validation(
                    "Delete request must specify a key",
                ));
            }

            validate_transact_key(table_info, key).map_err(batch_write_key_error)?;

            Ok(PreparedBatchOperation::Delete {
                table_name: table_info.table_name.clone(),
                table_info: table_info.clone(),
                key: key.clone(),
                write_request,
                existing_item: None,
                aux_item_stream_ttl_hours,
            })
        }
        _ => Err(StorageError::validation(
            "Each WriteRequest must contain exactly one of PutRequest or DeleteRequest",
        )),
    }
}

fn batch_write_key_error(error: StorageError) -> StorageError {
    let StorageEnum::Validation { message } = error.to_enum() else {
        return error;
    };
    if message == "The parameter cannot be converted to a numeric value" {
        return StorageError::raw_validation(message.clone());
    }
    if message == "Attempting to store more than 38 significant digits in a Number"
        || message
            == "Number underflow. Attempting to store a number with magnitude smaller than \
                supported range"
    {
        return StorageError::raw_validation(message.clone());
    }
    error
}
