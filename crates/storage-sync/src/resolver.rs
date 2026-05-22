use serde::{Deserialize, Serialize};
use storage_types::{
    BatchWriteItemRequest, CreateTableRequest, DeleteItemRequest, DeleteTableRequest,
    PutItemRequest, TransactWriteItemsRequest, UpdateItemRequest, UpdateTableRequest,
    UpdateTimeToLiveRequest,
};

use crate::SyncProposalId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncWriteProposalRequest {
    pub proposal_id: SyncProposalId,
    pub request: SyncWriteRequest,
}

impl SyncWriteProposalRequest {
    #[must_use]
    pub const fn new(proposal_id: SyncProposalId, request: SyncWriteRequest) -> Self {
        Self {
            proposal_id,
            request,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SyncWriteRequest {
    PutItem(PutItemRequest),
    UpdateItem(UpdateItemRequest),
    DeleteItem(DeleteItemRequest),
    BatchWriteItem(BatchWriteItemRequest),
    TransactWriteItems(TransactWriteItemsRequest),
    CreateTable(CreateTableRequest),
    UpdateTable(UpdateTableRequest),
    DeleteTable(DeleteTableRequest),
    UpdateTimeToLive(UpdateTimeToLiveRequest),
}

impl SyncWriteRequest {
    #[must_use]
    pub const fn operation_name(&self) -> &'static str {
        match self {
            Self::PutItem(_) => "PutItem",
            Self::UpdateItem(_) => "UpdateItem",
            Self::DeleteItem(_) => "DeleteItem",
            Self::BatchWriteItem(_) => "BatchWriteItem",
            Self::TransactWriteItems(_) => "TransactWriteItems",
            Self::CreateTable(_) => "CreateTable",
            Self::UpdateTable(_) => "UpdateTable",
            Self::DeleteTable(_) => "DeleteTable",
            Self::UpdateTimeToLive(_) => "UpdateTimeToLive",
        }
    }
}
