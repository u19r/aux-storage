//! Recovery retains only the tables whose response contains customer data.
use serde::{Deserialize, Serialize};
use storage_types::{
    DeleteItemResponse, PutItemResponse, TableHistoryId, TableName, UpdateItemResponse,
};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReceiptDataOwner {
    pub table: TableName,
    pub physical: TableHistoryId,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OperationReceiptState {
    Complete,
    Incomplete,
}

pub(crate) trait OriginItemResponse: Serialize {
    fn has_item_data(&self) -> bool;
}

impl OriginItemResponse for PutItemResponse {
    fn has_item_data(&self) -> bool {
        self.attributes.is_some()
    }
}

impl OriginItemResponse for UpdateItemResponse {
    fn has_item_data(&self) -> bool {
        self.attributes.is_some()
    }
}

impl OriginItemResponse for DeleteItemResponse {
    fn has_item_data(&self) -> bool {
        self.attributes.is_some()
    }
}

impl super::receipt::OriginOperationReceipt<&serde_json::value::RawValue> {
    pub(crate) fn remove_table_data(
        self,
        history: TableHistoryId,
    ) -> storage_types::StorageResult<
        super::receipt::OriginOperationReceipt<storage_types::BatchWriteItemResponse>,
    > {
        if self.cause != super::receipt::OriginOperationKind::Batch {
            return Err(crate::native_record::corrupt_record());
        }
        let mut response: storage_types::BatchWriteItemResponse =
            serde_json::from_str(self.response.get())
                .map_err(|_| crate::native_record::corrupt_record())?;
        let items = response
            .unprocessed_items
            .as_mut()
            .ok_or_else(crate::native_record::corrupt_record)?;
        for owner in self.owners.iter().filter(|owner| owner.physical == history) {
            items
                .remove(&owner.table)
                .ok_or_else(crate::native_record::corrupt_record)?;
        }
        Ok(super::receipt::OriginOperationReceipt {
            operation: self.operation,
            cause: self.cause,
            response,
            owners: self
                .owners
                .into_iter()
                .filter(|owner| owner.physical != history)
                .collect(),
            state: OperationReceiptState::Incomplete,
        })
    }
}
