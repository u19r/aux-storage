//! One KV retention repository over the same native attempt used by streams.

use std::future::Future;

use storage_provider::{
    StreamDurationTrimPageRequest, StreamDurationTrimPageResult, StreamTrimDueMarker,
    StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    maintenance::{StreamRetentionFinish, StreamRetentionObservation, StreamRetentionPort},
};
use storage_types::{ReadProtection, StorageResult, StreamItemId, TimestampMillis};

use super::{boundary, page, read, write};
use crate::{KvAttempt, catalog::KvCatalogAttempt};

impl<A: KvAttempt> StreamRetentionPort for KvCatalogAttempt<'_, A> {
    fn load_stream_trim_observations(
        &mut self,
        markers: &[StreamTrimDueMarker],
    ) -> impl Future<Output = StorageResult<Vec<StreamRetentionObservation>>> {
        read::load_stream_trim_observations(self.attempt, markers)
    }

    fn load_stream_trim_boundaries(
        &mut self,
        scopes: &[StreamTrimScope],
        floor: Option<StreamItemId>,
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimScopeBoundaries>>> {
        boundary::load_stream_trim_boundaries(self.attempt, scopes, floor, self.protection)
    }

    fn trim_stream_pages(
        &mut self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> impl Future<Output = StorageResult<Vec<StreamDurationTrimPageResult>>> {
        page::trim_stream_pages(self.attempt, requests)
    }

    fn finish_stream_trim_markers(
        &mut self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> impl Future<Output = StorageResult<()>> {
        write::finish_stream_trim_markers(self.attempt, finishes)
    }
    fn list_due_stream_trim_markers(
        &mut self,
        due_before: TimestampMillis,
        limit: usize,
    ) -> impl Future<Output = StorageResult<Vec<StreamTrimDueMarker>>> {
        read::list_due_stream_trim_markers(self.attempt, due_before, limit)
    }
    fn load_stream_trim_states(
        &mut self,
        scopes: &[StreamTrimScope],
    ) -> impl Future<Output = StorageResult<Vec<Option<StreamTrimState>>>> {
        read::load_stream_trim_states(self.attempt, scopes, ReadProtection::Snapshot)
    }
}
