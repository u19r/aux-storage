use crate::storage_ops::provider_impl::*;

pub(in crate::storage_ops) fn to_direct_write_operation(
    operation: TransactWriteOperation,
) -> StorageResult<DirectWriteOperation> {
    match operation {
        TransactWriteOperation::Put {
            key,
            value,
            condition,
        } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::Put { key, value })
        }
        TransactWriteOperation::PutTemplate {
            template,
            value,
            condition,
        } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::PutTemplate { template, value })
        }
        TransactWriteOperation::Delete { key, condition } => {
            if condition.is_some() {
                return Err(StorageError::validation(
                    "direct write operation does not support conditions",
                ));
            }
            Ok(DirectWriteOperation::Delete { key })
        }
        TransactWriteOperation::CheckValue {
            key,
            expected_value,
        } => Ok(DirectWriteOperation::CheckValue {
            key,
            expected_value,
        }),
        TransactWriteOperation::Check { .. } | TransactWriteOperation::Update { .. } => {
            Err(StorageError::validation(
                "direct write operation requires put/delete or exact-value checks only",
            ))
        }
    }
}

pub(super) fn kv_mutation_to_direct(mutation: KvMutation) -> DirectWriteOperation {
    match mutation {
        KvMutation::Put { key, value } => DirectWriteOperation::Put { key, value },
        KvMutation::PutTemplate { template, value } => {
            DirectWriteOperation::PutTemplate { template, value }
        }
        KvMutation::Delete { key } => DirectWriteOperation::Delete { key },
    }
}

pub(in crate::storage_ops) fn kv_mutation_to_direct_with_literal_templates(
    mutation: KvMutation,
) -> DirectWriteOperation {
    match mutation {
        KvMutation::PutTemplate { template, value } => DirectWriteOperation::Put {
            key: template.rocks_key(),
            value,
        },
        other => kv_mutation_to_direct(other),
    }
}
