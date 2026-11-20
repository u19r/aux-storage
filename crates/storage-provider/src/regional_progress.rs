//! Per-lineage pair progress: receive cursors and send holds (REG-0 key and row
//! layouts).

use std::future::Future;

use regional_replication::message::LineageRef;
use storage_types::{
    BoundTableInfo, KeyAttributes, PairId, RegionalMarker, StorageResult, SystemStreamOffset,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ProgressDirection {
    /// The peer's lineage: what this region has applied.
    Receive = 1,
    /// This region's lineage: what the peer has acknowledged; the retention
    /// hold (REG-9).
    Send = 2,
}

/// A prefix of one lineage. Absent record = `Default` (nothing; not sealed).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LineageCursor {
    pub through: Option<SystemStreamOffset>,
    pub sealed: bool,
}

/// Families store one record per (table, pair, direction, lineage) under the
/// table (KV `'P'` family; SQL `regional_progress`). Reads protect what they
/// return for the attempt (KV read conflict; Postgres advisory lock per record,
/// taken AFTER any item locks of the attempt).
pub trait RegionalProgressAttempt: Send {
    /// One native read wave; result in `lineages` order.
    fn read_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineages: &[LineageRef],
    ) -> impl Future<Output = StorageResult<Vec<Option<LineageCursor>>>> + Send;

    /// Every record of one direction of the pair (one range read, unprotected
    /// snapshot is enough for the callers: sender turn and lag report re-read
    /// what they write).
    fn list_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
    ) -> impl Future<Output = StorageResult<Vec<(LineageRef, LineageCursor)>>> + Send;

    /// Stage an insert-or-replace of one record (no await; flushed with the
    /// attempt's writes).
    fn stage_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
        direction: ProgressDirection,
        lineage: &LineageRef,
        cursor: &LineageCursor,
    ) -> StorageResult<()>;

    /// Stage removal of every record of the pair, both directions (pair-end
    /// cleanup, REG-11).
    fn clear_regional_progress(
        &mut self,
        table: &BoundTableInfo,
        pair: PairId,
    ) -> StorageResult<()>;

    /// Stage the removal of every progress record of `table` (all pairs, both
    /// directions).
    fn clear_table_regional_progress(&mut self, table: &BoundTableInfo) -> StorageResult<()>;
}

/// Regional delete-marker rows of a table. Every marker write goes through
/// `stage_regional_marker`: local writes (`dynamo_write_execution.rs`),
/// receive, scan and sweep.
pub trait RegionalMarkerAttempt: Send {
    /// `Some` stores (insert or replace). `None` removes the key's marker,
    /// which this attempt observed through `observe_for_writes`; a backend may
    /// reject a removal that changes no row.
    fn stage_regional_marker(
        &mut self,
        table: &BoundTableInfo,
        key: &KeyAttributes,
        marker: Option<RegionalMarker>,
    ) -> StorageResult<()>;

    /// Markers of `table` in key order after `after` (exclusive). `more`:
    /// another page exists. Snapshot read: callers protect each key by
    /// observing it before changing it.
    fn list_regional_markers(
        &mut self,
        table: &BoundTableInfo,
        after: Option<&[u8]>,
        limit: std::num::NonZeroU32,
    ) -> impl Future<Output = StorageResult<MarkerPage>> + Send;

    /// Stage the removal of every marker of `table` (pair end, abandon).
    fn clear_regional_markers(&mut self, table: &BoundTableInfo) -> StorageResult<()>;
}

#[derive(Debug, Default)]
pub struct MarkerPage {
    /// (`regional_marker_item_key` bytes, marker).
    pub markers: Vec<(Vec<u8>, RegionalMarker)>,
    pub more: bool,
}
