use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use storage_types::{
    AttributeValue, ItemProvenance, KeyAttributes, StorageError, StorageResult, TableName,
};

pub enum AtomicItemWriteDecision {
    NoWrite {
        output: Vec<u8>,
    },
    Write {
        item: HashMap<String, AttributeValue>,
        additional_items: Vec<HashMap<String, AttributeValue>>,
        output: Vec<u8>,
    },
}

pub type AtomicItemTransform = Arc<
    dyn Fn(Option<&HashMap<String, AttributeValue>>) -> StorageResult<AtomicItemWriteDecision>
        + Send
        + Sync,
>;

pub struct AtomicItemReadModifyWriteRequest {
    pub table_name: TableName,
    pub key: KeyAttributes,
    pub transform: AtomicItemTransform,
}

pub struct ValidatedPrimaryRevisionPresence;

pub struct PrimaryRevisionState {
    pub revision: u64,
    pub deleted: bool,
}

/// Durable primary-item revision authority stored beside a physical item.
///
/// Key/value providers repeat the identity in the value so a misplaced record
/// can be rejected. SQL providers may map their identity columns and state into
/// this same model after reading a row.
#[derive(Clone, Serialize, Deserialize)]
pub struct PrimaryRevisionRecord<Text = String> {
    pub table_name: Text,
    pub key_json: Text,
    pub revision: u64,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub provenance: ItemProvenance,
}

impl<Text> From<&PrimaryRevisionRecord<Text>> for PrimaryRevisionState {
    fn from(record: &PrimaryRevisionRecord<Text>) -> Self {
        Self {
            revision: record.revision,
            deleted: record.deleted,
        }
    }
}

pub struct ItemWriteResult {
    pub old_item: Option<storage_types::AttributeMap>,
    pub new_item: Option<storage_types::AttributeMap>,
}

impl TryFrom<(bool, Option<bool>)> for ValidatedPrimaryRevisionPresence {
    type Error = StorageError;

    fn try_from((item_present, revision_deleted): (bool, Option<bool>)) -> StorageResult<Self> {
        match (item_present, revision_deleted) {
            (false, None) | (false, Some(true)) | (true, Some(false)) => Ok(Self),
            (true, None) => Err(StorageError::internal(
                "primary item exists without durable revision authority",
            )),
            (false, Some(false)) => Err(StorageError::internal(
                "durable present revision authority exists without primary item",
            )),
            (true, Some(true)) => Err(StorageError::internal(
                "primary tombstone authority conflicts with a present primary item",
            )),
        }
    }
}
