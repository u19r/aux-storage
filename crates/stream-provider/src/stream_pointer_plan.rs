//! Canonical stored pointer choice and encoding for stream writers.

use storage_types::{
    ItemRevision, ItemStreamVersion, ItemWriteVersions, StorageResult, StreamName, TableName,
};

use crate::{
    StoredStreamPointer, StreamDataType, embedded_stream_items, should_embed_stream_images,
};

pub struct StreamPointerInput<'a> {
    pub history_id: storage_types::TableHistoryId,
    pub item_stream: &'a StreamName,
    pub table_name: &'a TableName,
    pub item_stream_version: ItemStreamVersion,
    pub item_revision: ItemRevision,
    pub item_data: &'a [u8],
    pub old_item_data: Option<&'a [u8]>,
    pub data_type: StreamDataType,
    pub indexers: &'a [String],
    pub old_indexers: Option<&'a [String]>,
}

pub struct StreamPointerPlan {
    pub encoded: Vec<u8>,
    pub referenced_version: Option<ItemStreamVersion>,
}

impl StreamPointerPlan {
    pub fn try_new(input: StreamPointerInput<'_>) -> StorageResult<Self> {
        let embedded =
            should_embed_stream_images(input.item_data.len(), input.old_item_data.map(<[u8]>::len));
        let pointer = if embedded {
            StoredStreamPointer::embedded(
                storage_types::ItemHistoryAddress {
                    history_id: input.history_id,
                    stream_name: input.item_stream.clone(),
                },
                input.table_name.clone(),
                ItemWriteVersions {
                    item_revision: input.item_revision,
                    item_stream_version: input.item_stream_version,
                },
                embedded_stream_items(
                    input.item_data.to_vec(),
                    input.old_item_data.map(<[u8]>::to_vec),
                    input.data_type,
                ),
            )
        } else {
            StoredStreamPointer::pointer(
                storage_types::ItemHistoryAddress {
                    history_id: input.history_id,
                    stream_name: input.item_stream.clone(),
                },
                input.table_name.clone(),
                ItemWriteVersions {
                    item_revision: input.item_revision,
                    item_stream_version: input.item_stream_version,
                },
            )
        };
        let pointer = pointer
            .with_indexers(input.indexers.to_vec())
            .with_old_indexers(input.old_indexers.map(<[String]>::to_vec));
        Ok(Self {
            encoded: storage_types::storage_serde::to_bytes(&pointer)?,
            referenced_version: (!embedded).then_some(input.item_stream_version),
        })
    }
}
