//! Shared pointer-page reads. Embedded records do not depend on item history.

use futures::{StreamExt, TryStreamExt};
use storage_types::{ItemStreamVersion, StreamItemId, StreamName, TimestampMillis};

use super::StreamProvider;
use crate::{
    EmbeddedStreamItem, PointerRecordsResult, StoredStreamPointer, StreamItem, StreamPointer,
    StreamResult, constants::STREAM_LIMITS,
};

pub(super) enum PointerReadPurpose {
    Direct,
    Background,
}

pub(super) struct PointerStreamRead<'a, P: ?Sized> {
    pub provider: &'a P,
    pub purpose: PointerReadPurpose,
}

struct PointerRead {
    pointer: StreamPointer,
    history: storage_types::ItemHistoryAddress,
    embedded: Option<Vec<EmbeddedStreamItem>>,
}

impl<P: StreamProvider + ?Sized> PointerStreamRead<'_, P> {
    pub(super) async fn read(
        &self,
        stream: StreamName,
        start: Option<StreamItemId>,
        limit: Option<u32>,
    ) -> StreamResult<PointerRecordsResult> {
        let page = self
            .provider
            .read_forward(stream, start, STREAM_LIMITS.clamp(limit))
            .await?;
        let records = futures::stream::iter(page.items)
            .map(|item| self.read_record(item))
            .buffered(32)
            .try_filter_map(|record| futures::future::ready(Ok(record)))
            .try_collect()
            .await?;
        Ok(PointerRecordsResult {
            records,
            last_scanned_key: page.last_evaluated_key,
            last_evaluated_key: page.has_more.then_some(page.last_evaluated_key).flatten(),
            has_more: page.has_more,
        })
    }

    async fn read_record(
        &self,
        item: StreamItem,
    ) -> StreamResult<Option<(StreamPointer, Vec<StreamItem>)>> {
        let stored = match self.purpose {
            PointerReadPurpose::Direct => {
                Some(self.provider.decode_stored_stream_pointer(&item).await?)
            }
            PointerReadPurpose::Background => {
                self.provider
                    .decode_stored_stream_pointer_for_background(&item)
                    .await?
            }
        };
        let Some(stored) = stored else {
            return Ok(None);
        };
        let PointerRead {
            pointer,
            history,
            embedded,
        } = stored.into_read(item.id);
        let images = match embedded {
            Some(images) => images
                .into_iter()
                .map(|image| image.into_stream_item(pointer.item_stream_version, item.created_at))
                .collect(),
            None => {
                self.provider
                    .read_item_stream_backward_from_pointer(
                        history,
                        pointer.stream_item_id,
                        pointer.item_stream_version,
                        2,
                    )
                    .await?
                    .items
            }
        };
        Ok(Some((pointer, images)))
    }
}

impl StoredStreamPointer {
    fn into_read(mut self, pointer_id: StreamItemId) -> PointerRead {
        let history = self.history_address();
        let embedded = match &mut self {
            Self::Embedded { items, .. } => Some(std::mem::take(items)),
            Self::Pointer { .. } => None,
        };
        let (Self::Pointer {
            stream_name,
            table_name,
            item_stream_version,
            indexers,
            old_indexers,
            ..
        }
        | Self::Embedded {
            stream_name,
            table_name,
            item_stream_version,
            indexers,
            old_indexers,
            ..
        }) = self;
        PointerRead {
            history,
            pointer: StreamPointer {
                stream_name,
                table_name,
                item_stream_version,
                stream_item_id: pointer_id,
                indexers,
                old_indexers,
                system_position: None,
            },
            embedded,
        }
    }
}

impl EmbeddedStreamItem {
    fn into_stream_item(
        self,
        version: ItemStreamVersion,
        created_at: TimestampMillis,
    ) -> StreamItem {
        StreamItem {
            id: StreamItemId::from(version),
            stream_name: None,
            data: self.data,
            data_type: self.data_type,
            created_at,
        }
    }
}
