use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use storage_types::{
    StorageError, StorageResult, SystemStreamPosition, SystemStreamScope, TimestampMillis,
    storage_serde,
};

use crate::{
    StreamAttempt,
    constants::{
        MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES, MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT,
        MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS,
    },
};

/// Compact source metadata; trimming never resolves an item image.
pub struct SystemStreamTrimRecord {
    pub position: SystemStreamPosition,
    pub created_at: TimestampMillis,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrimScanCursor {
    position: SystemStreamPosition,
    blocked_lineage: bool,
}

/// Stable identity of one logical SystemStream trim operation.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SystemStreamTrimOperationId {
    scope: SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
}

impl SystemStreamTrimOperationId {
    #[must_use]
    pub fn new(scope: &SystemStreamScope, cutoff_timestamp: TimestampMillis) -> Self {
        Self {
            scope: scope.clone(),
            cutoff_timestamp,
        }
    }

    #[must_use]
    pub fn scope(&self) -> &SystemStreamScope {
        &self.scope
    }

    #[must_use]
    pub const fn cutoff_timestamp(&self) -> TimestampMillis {
        self.cutoff_timestamp
    }
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct OperationIdWire<'a> {
    scope: &'a SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedOperationIdWire {
    scope: SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
}

impl Serialize for SystemStreamTrimOperationId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        OperationIdWire {
            scope: &self.scope,
            cutoff_timestamp: self.cutoff_timestamp,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamTrimOperationId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OwnedOperationIdWire::deserialize(deserializer)?;
        Ok(Self::new(&wire.scope, wire.cutoff_timestamp))
    }
}

/// Opaque provider-owned position used to resume one bounded logical trim
/// scan. The bytes are continuation state, never a caller-authoritative
/// frontier or protection value.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SystemStreamTrimContinuation {
    operation_id: SystemStreamTrimOperationId,
    sequence: u64,
    bytes: Vec<u8>,
}

impl fmt::Debug for SystemStreamTrimContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemStreamTrimContinuation")
            .field("operation_id", &"<redacted>")
            .field("sequence", &self.sequence)
            .field("length", &self.bytes.len())
            .finish()
    }
}

impl SystemStreamTrimContinuation {
    pub fn try_new(
        operation_id: SystemStreamTrimOperationId,
        sequence: u64,
        bytes: impl AsRef<[u8]>,
    ) -> StorageResult<Self> {
        if sequence == 0 {
            return Err(StorageError::validation(
                "system stream trim continuation sequence must be positive",
            ));
        }
        let bytes = bytes.as_ref();
        if bytes.is_empty() || bytes.len() > MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES {
            return Err(StorageError::validation(format!(
                "system stream trim continuation must contain 1 through {} bytes",
                MAX_SYSTEM_STREAM_TRIM_CONTINUATION_BYTES
            )));
        }
        Ok(Self {
            operation_id,
            sequence,
            bytes: bytes.to_vec(),
        })
    }

    #[must_use]
    pub fn operation_id(&self) -> &SystemStreamTrimOperationId {
        &self.operation_id
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContinuationWire {
    operation_id: SystemStreamTrimOperationId,
    sequence: u64,
    bytes: Vec<u8>,
}

impl Serialize for SystemStreamTrimContinuation {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ContinuationWire {
            operation_id: self.operation_id.clone(),
            sequence: self.sequence,
            bytes: self.bytes.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamTrimContinuation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ContinuationWire::deserialize(deserializer)?;
        Self::try_new(wire.operation_id, wire.sequence, wire.bytes).map_err(D::Error::custom)
    }
}

/// Provider-neutral bounded logical SystemStream reclamation request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamTrimPageRequest {
    operation_id: SystemStreamTrimOperationId,
    scope: SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
    page_limit: usize,
    continuation: Option<SystemStreamTrimContinuation>,
}

impl SystemStreamTrimPageRequest {
    pub fn try_new(
        scope: SystemStreamScope,
        cutoff_timestamp: TimestampMillis,
        page_limit: usize,
        continuation: Option<SystemStreamTrimContinuation>,
    ) -> StorageResult<Self> {
        if page_limit == 0 || page_limit > MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT {
            return Err(StorageError::validation(format!(
                "system stream trim page limit must be between 1 and {}",
                MAX_SYSTEM_STREAM_TRIM_PAGE_LIMIT
            )));
        }
        let operation_id = SystemStreamTrimOperationId::new(&scope, cutoff_timestamp);
        if continuation
            .as_ref()
            .is_some_and(|cursor| cursor.operation_id() != &operation_id)
        {
            return Err(StorageError::validation(
                "system stream trim continuation does not match scope and cutoff",
            ));
        }
        Ok(Self {
            operation_id,
            scope,
            cutoff_timestamp,
            page_limit,
            continuation,
        })
    }

    #[must_use]
    pub fn operation_id(&self) -> &SystemStreamTrimOperationId {
        &self.operation_id
    }

    #[must_use]
    pub fn scope(&self) -> &SystemStreamScope {
        &self.scope
    }

    #[must_use]
    pub const fn cutoff_timestamp(&self) -> TimestampMillis {
        self.cutoff_timestamp
    }

    #[must_use]
    pub const fn page_limit(&self) -> usize {
        self.page_limit
    }

    #[must_use]
    pub fn continuation(&self) -> Option<&SystemStreamTrimContinuation> {
        self.continuation.as_ref()
    }

    pub fn after_position(&self) -> StorageResult<Option<SystemStreamPosition>> {
        self.scan_cursor()
            .map(|cursor| cursor.map(|cursor| cursor.position))
    }

    /// A continuation is authority only when it is the exact output of the
    /// preceding committed page for this operation. Call this after loading
    /// that receipt in the same protected native attempt as the trim scan.
    pub fn validate_predecessor(
        &self,
        predecessor: Option<&SystemStreamTrimPageReceipt>,
    ) -> StorageResult<()> {
        let Some(continuation) = self.continuation() else {
            return Ok(());
        };
        let previous_sequence = continuation
            .sequence()
            .checked_sub(1)
            .ok_or_else(|| StorageError::validation("system stream trim sequence is invalid"))?;
        let previous = predecessor.ok_or_else(|| {
            StorageError::validation("system stream trim preceding page is missing")
        })?;
        if previous.operation_id != self.operation_id
            || previous.input_sequence != previous_sequence
            || previous.next_cursor.as_deref() != Some(continuation.as_bytes())
        {
            return Err(StorageError::validation(
                "system stream trim continuation does not match preceding page",
            ));
        }
        Ok(())
    }

    fn scan_cursor(&self) -> StorageResult<Option<TrimScanCursor>> {
        self.continuation()
            .map(|cursor| storage_serde::from_bytes(cursor.as_bytes()))
            .transpose()
    }

    pub fn permits_record(&self, created_at: TimestampMillis) -> bool {
        created_at <= self.cutoff_timestamp
    }

    /// Retire a time-qualified prefix in each lineage. Request timestamps can
    /// precede commit order, so the first unexpired row blocks its suffix.
    pub fn select_records(
        &self,
        records: Vec<SystemStreamTrimRecord>,
        more: bool,
        publication_boundaries: &[SystemStreamPosition],
    ) -> StorageResult<(Vec<SystemStreamTrimRecord>, SystemStreamTrimPageReceipt)> {
        if records.len() > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS || (more && records.is_empty()) {
            return Err(StorageError::internal(
                "SystemStream trim scan exceeded its page bound",
            ));
        }
        let input_count = records.len();
        let cursor = self.scan_cursor()?;
        let mut blocked = cursor.as_ref().is_some_and(|cursor| cursor.blocked_lineage);
        let mut previous = cursor.map(|cursor| cursor.position);
        let mut scanned = 0;
        let mut selected = Vec::new();
        for record in records {
            if let Some(last) = &previous {
                if !precedes_position(last, &record.position)? {
                    return Err(StorageError::internal(
                        "SystemStream trim scan is out of order",
                    ));
                }
                if last.shard_id() != record.position.shard_id()
                    || last.lineage_id() != record.position.lineage_id()
                {
                    blocked = false;
                }
            }
            blocked |= !self.permits_record(record.created_at)
                || !record.is_published(publication_boundaries)?;
            previous = Some(record.position.clone());
            scanned += 1;
            if !blocked {
                selected.push(record);
                if selected.len() == self.page_limit {
                    break;
                }
            }
        }
        let next_cursor = if more || scanned < input_count {
            Some(storage_serde::to_bytes(&TrimScanCursor {
                position: previous.ok_or_else(|| {
                    StorageError::internal("SystemStream trim scan cannot advance")
                })?,
                blocked_lineage: blocked,
            })?)
        } else {
            None
        };
        let receipt =
            SystemStreamTrimPageReceipt::try_new(self, selected.len(), scanned, next_cursor)?;
        Ok((selected, receipt))
    }
}

/// The stored receipt of this page and of its predecessor page.
async fn read_trim_receipts<A: StreamAttempt>(
    attempt: &mut A,
    request: &SystemStreamTrimPageRequest,
) -> StorageResult<(
    Option<SystemStreamTrimPageReceipt>,
    Option<SystemStreamTrimPageReceipt>,
)> {
    let sequence = request.continuation().map_or(0, |cursor| cursor.sequence());
    let mut sequences = vec![sequence];
    if let Some(previous) = sequence.checked_sub(1) {
        sequences.push(previous);
    }
    let receipts = attempt
        .load_trim_receipts(request.operation_id(), &sequences)
        .await?;
    let mut checked = receipts.zip(sequences).map(|(sequence, receipt)| {
        receipt
            .map(|receipt| {
                if receipt.operation_id != *request.operation_id()
                    || receipt.input_sequence != sequence
                {
                    return Err(StorageError::internal(
                        "SystemStream trim receipt is invalid",
                    ));
                }
                Ok(receipt)
            })
            .transpose()
    });
    // `sequences` always holds the current sequence first.
    let current = checked.next().transpose()?.flatten();
    let predecessor = checked.next().transpose()?.flatten();
    Ok((current, predecessor))
}

/// The greatest selected position of each lane, ordered by
/// `(shard_id, lineage_id)`.
pub fn retired_frontiers(
    selected: &[SystemStreamTrimRecord],
) -> StorageResult<Vec<SystemStreamPosition>> {
    let mut frontiers: Vec<SystemStreamPosition> = Vec::new();
    for record in selected {
        match frontiers.iter_mut().find(|frontier| {
            frontier.shard_id() == record.position.shard_id()
                && frontier.lineage_id() == record.position.lineage_id()
        }) {
            Some(frontier) => {
                if precedes_position(frontier, &record.position)? {
                    *frontier = record.position.clone();
                }
            }
            None => frontiers.push(record.position.clone()),
        }
    }
    frontiers.sort_by(|left, right| {
        (left.shard_id(), left.lineage_id()).cmp(&(right.shard_id(), right.lineage_id()))
    });
    Ok(frontiers)
}

pub async fn execute_trim_page<A: StreamAttempt>(
    attempt: &mut A,
    request: SystemStreamTrimPageRequest,
) -> StorageResult<SystemStreamTrimPageResult> {
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        let (current, predecessor) = read_trim_receipts(attempt, &request).await?;
        if let Some(current) = current {
            return current.replay(&request);
        }
        request.validate_predecessor(predecessor.as_ref())?;
        let history = attempt.read_system_page_start().await?;
        history
            .validate_scope(request.scope())
            .map_err(|_| StorageError::internal("SystemStream trim has another source"))?;
        let after = request.after_position()?;
        let origins = history
            .retirement_origins()
            .map_err(|_| StorageError::internal("Invalid SystemStream trim origins"))?;
        let retired = attempt.load_system_retired_frontiers(&origins).await?;
        let mut starts = Vec::with_capacity(origins.len());
        for (origin, retired) in retired.zip(origins) {
            let mut start = retired.unwrap_or(origin);
            if let Some(after) = &after {
                let lane = (start.shard_id(), start.lineage_id());
                let cursor_lane = (after.shard_id(), after.lineage_id());
                if lane < cursor_lane {
                    continue;
                }
                if lane == cursor_lane && precedes_position(&start, after)? {
                    start = after.clone();
                }
            }
            starts.push(start);
        }
        starts.sort_by(|left, right| {
            (left.shard_id(), left.lineage_id()).cmp(&(right.shard_id(), right.lineage_id()))
        });
        let (records, more) = attempt.scan_trim_records(&starts).await?;
        let boundaries = history
            .retirement_boundaries()
            .map_err(|_| StorageError::internal("Invalid SystemStream publication frontier"))?;
        let (selected, receipt) = request.select_records(records, more, &boundaries)?;
        let frontiers = retired_frontiers(&selected)?;
        attempt
            .apply_trim_page(&selected, &frontiers, &receipt)
            .await?;
        receipt.replay(&request)
    }
    .await;
    settlement.complete(result)
}

impl SystemStreamTrimRecord {
    fn is_published(&self, boundaries: &[SystemStreamPosition]) -> StorageResult<bool> {
        let boundary = boundaries
            .iter()
            .find(|boundary| {
                boundary.shard_id() == self.position.shard_id()
                    && boundary.lineage_id() == self.position.lineage_id()
            })
            .ok_or_else(|| {
                StorageError::internal("SystemStream record has no publication authority")
            })?;
        self.position
            .offset()
            .compare_order(boundary.offset())
            .map(|order| !order.is_gt())
            .map_err(|_| StorageError::internal("Invalid SystemStream publication position"))
    }
}

fn precedes_position(
    left: &SystemStreamPosition,
    right: &SystemStreamPosition,
) -> StorageResult<bool> {
    let left_key = (left.shard_id(), left.lineage_id());
    let right_key = (right.shard_id(), right.lineage_id());
    if left_key != right_key {
        return Ok(left_key < right_key);
    }
    let first = left
        .offset()
        .order()
        .map_err(|_| StorageError::internal("SystemStream trim cursor offset is invalid"))?;
    let second = right
        .offset()
        .order()
        .map_err(|_| StorageError::internal("SystemStream trim record offset is invalid"))?;
    if left.offset().versionstamp().is_some() {
        return left
            .offset()
            .compare_order(right.offset())
            .map(|order| order.is_lt())
            .map_err(|_| StorageError::internal("Invalid native trim ordering"));
    }
    Ok(first < second)
}

#[derive(Serialize)]
#[serde(deny_unknown_fields)]
struct RequestWire<'a> {
    scope: &'a SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
    page_limit: usize,
    continuation: &'a Option<SystemStreamTrimContinuation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnedRequestWire {
    scope: SystemStreamScope,
    cutoff_timestamp: TimestampMillis,
    page_limit: usize,
    continuation: Option<SystemStreamTrimContinuation>,
}

impl Serialize for SystemStreamTrimPageRequest {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        RequestWire {
            scope: &self.scope,
            cutoff_timestamp: self.cutoff_timestamp,
            page_limit: self.page_limit,
            continuation: &self.continuation,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamTrimPageRequest {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = OwnedRequestWire::deserialize(deserializer)?;
        Self::try_new(
            wire.scope,
            wire.cutoff_timestamp,
            wire.page_limit,
            wire.continuation,
        )
        .map_err(D::Error::custom)
    }
}

/// Durable evidence for one committed bounded trim page.
///
/// Providers persist this logical receipt beside any native scan boundary.
/// The boundary remains provider-owned, while replay identity and page-result
/// validation stay identical across backends.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemStreamTrimPageReceipt {
    pub operation_id: SystemStreamTrimOperationId,
    pub page_limit: usize,
    pub input_sequence: u64,
    pub input_cursor: Option<Vec<u8>>,
    pub reclaimed_records: u32,
    pub scanned_records: u32,
    pub next_cursor: Option<Vec<u8>>,
}

impl SystemStreamTrimPageReceipt {
    pub fn try_new(
        request: &SystemStreamTrimPageRequest,
        reclaimed_records: usize,
        scanned_records: usize,
        next_cursor: Option<Vec<u8>>,
    ) -> StorageResult<Self> {
        let receipt = Self {
            operation_id: request.operation_id().clone(),
            page_limit: request.page_limit(),
            input_sequence: request
                .continuation()
                .map_or(0, SystemStreamTrimContinuation::sequence),
            input_cursor: request
                .continuation()
                .map(|continuation| continuation.as_bytes().to_vec()),
            reclaimed_records: u32::try_from(reclaimed_records)
                .map_err(|_| StorageError::internal("system stream trim count overflow"))?,
            scanned_records: u32::try_from(scanned_records)
                .map_err(|_| StorageError::internal("system stream trim scan count overflow"))?,
            next_cursor,
        };
        receipt.replay(request)?;
        Ok(receipt)
    }

    pub fn replay(
        &self,
        request: &SystemStreamTrimPageRequest,
    ) -> StorageResult<SystemStreamTrimPageResult> {
        let expected_sequence = request
            .continuation()
            .map_or(0, SystemStreamTrimContinuation::sequence);
        let expected_cursor = request
            .continuation()
            .map(SystemStreamTrimContinuation::as_bytes);
        if self.operation_id != *request.operation_id()
            || self.page_limit != request.page_limit()
            || self.input_sequence != expected_sequence
            || self.input_cursor.as_deref() != expected_cursor
        {
            return Err(StorageError::validation(
                "system stream trim page receipt does not match request",
            ));
        }
        let next = self
            .next_cursor
            .as_ref()
            .map(|bytes| {
                let sequence = self.input_sequence.checked_add(1).ok_or_else(|| {
                    StorageError::internal("system stream trim receipt sequence overflow")
                })?;
                SystemStreamTrimContinuation::try_new(self.operation_id.clone(), sequence, bytes)
            })
            .transpose()?;
        let result = SystemStreamTrimPageResult::new_with_scan_progress(
            self.operation_id.clone(),
            self.reclaimed_records as usize,
            self.scanned_records as usize,
            next,
        );
        result.validate_for(request)?;
        Ok(result)
    }
}

/// Result of one bounded logical SystemStream reclaim transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemStreamTrimPageResult {
    operation_id: SystemStreamTrimOperationId,
    reclaimed_records: usize,
    scanned_records: usize,
    next_continuation: Option<SystemStreamTrimContinuation>,
}

impl SystemStreamTrimPageResult {
    #[must_use]
    pub const fn new(
        operation_id: SystemStreamTrimOperationId,
        reclaimed_records: usize,
        next_continuation: Option<SystemStreamTrimContinuation>,
    ) -> Self {
        Self::new_with_scan_progress(
            operation_id,
            reclaimed_records,
            reclaimed_records,
            next_continuation,
        )
    }

    #[must_use]
    pub const fn new_with_scan_progress(
        operation_id: SystemStreamTrimOperationId,
        reclaimed_records: usize,
        scanned_records: usize,
        next_continuation: Option<SystemStreamTrimContinuation>,
    ) -> Self {
        Self {
            operation_id,
            reclaimed_records,
            scanned_records,
            next_continuation,
        }
    }

    #[must_use]
    pub fn operation_id(&self) -> &SystemStreamTrimOperationId {
        &self.operation_id
    }

    #[must_use]
    pub const fn reclaimed_records(&self) -> usize {
        self.reclaimed_records
    }

    #[must_use]
    pub const fn scanned_records(&self) -> usize {
        self.scanned_records
    }

    #[must_use]
    pub fn next_continuation(&self) -> Option<&SystemStreamTrimContinuation> {
        self.next_continuation.as_ref()
    }

    pub fn validate_for(&self, request: &SystemStreamTrimPageRequest) -> StorageResult<()> {
        if self.operation_id != request.operation_id {
            return Err(StorageError::validation(
                "system stream trim result does not match operation",
            ));
        }
        if self.reclaimed_records > request.page_limit {
            return Err(StorageError::validation(
                "system stream trim result exceeds requested page limit",
            ));
        }
        self.validate_shape()?;
        if let Some(next) = self.next_continuation.as_ref() {
            let expected_sequence = match request.continuation.as_ref() {
                None => Some(1),
                Some(current) => current.sequence().checked_add(1),
            };
            if expected_sequence != Some(next.sequence()) {
                return Err(StorageError::validation(
                    "system stream trim continuation sequence did not advance",
                ));
            }
        }
        Ok(())
    }

    fn validate_shape(&self) -> StorageResult<()> {
        if self.reclaimed_records > self.scanned_records
            || self.scanned_records > MAX_SYSTEM_STREAM_TRIM_SCAN_ROWS
        {
            return Err(StorageError::validation(
                "system stream trim result has invalid scan progress",
            ));
        }
        if self.scanned_records == 0 && self.next_continuation.is_some() {
            return Err(StorageError::validation(
                "system stream trim cannot continue without scan progress",
            ));
        }
        if self
            .next_continuation
            .as_ref()
            .is_some_and(|next| next.operation_id() != &self.operation_id)
        {
            return Err(StorageError::validation(
                "system stream trim result continuation does not match operation",
            ));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultWire {
    operation_id: SystemStreamTrimOperationId,
    reclaimed_records: usize,
    scanned_records: usize,
    next_continuation: Option<SystemStreamTrimContinuation>,
}

impl Serialize for SystemStreamTrimPageResult {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ResultWire {
            operation_id: self.operation_id.clone(),
            reclaimed_records: self.reclaimed_records,
            scanned_records: self.scanned_records,
            next_continuation: self.next_continuation.clone(),
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SystemStreamTrimPageResult {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = ResultWire::deserialize(deserializer)?;
        let result = Self::new_with_scan_progress(
            wire.operation_id,
            wire.reclaimed_records,
            wire.scanned_records,
            wire.next_continuation,
        );
        result.validate_shape().map_err(D::Error::custom)?;
        Ok(result)
    }
}
