#![allow(non_snake_case)]

use async_trait::async_trait;
use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    TableName, TimestampMillis,
};

use crate::{
    StreamDurationTrimBackend, StreamDurationTrimConfig, StreamDurationTrimPageRequest,
    StreamDurationTrimPageResult, StreamDurationTrimWorker, StreamTrimBoundary,
    StreamTrimDueMarker, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite,
};

const FOREVER: i64 = -1;
const NOW_HOURS: i64 = 5;
const PAGE_SIZE: usize = 2;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct DurationCase {
    #[serde(rename = "tableHours")]
    table_hours: i64,
    #[serde(rename = "itemHours")]
    item_hours: i64,
    #[serde(rename = "markerVersion")]
    marker_version: u64,
    #[serde(rename = "currentPolicyVersion")]
    current_policy_version: u64,
    #[serde(rename = "currentVersion")]
    current_version: u64,
    #[serde(rename = "latestVersion")]
    latest_version: u64,
    #[serde(rename = "retainedTablePointerVersion")]
    retained_table_pointer_version: u64,
    #[serde(rename = "protectedBefore")]
    protected_before: u64,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct CustomStreamDurationState {
    #[serde(rename = "lastCase")]
    last_case: DurationCase,
    #[serde(rename = "lastOutcome")]
    last_outcome: String,
    #[serde(rename = "effectiveHours")]
    effective_hours: i64,
    #[serde(rename = "deleteCount")]
    delete_count: usize,
}

impl State<CustomStreamDurationDriver> for CustomStreamDurationState {
    fn from_driver(driver: &CustomStreamDurationDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_outcome: driver.last_outcome.clone(),
            effective_hours: driver.effective_hours,
            delete_count: driver.delete_count,
        })
    }
}

#[derive(Debug)]
struct CustomStreamDurationDriver {
    last_case: DurationCase,
    last_outcome: String,
    effective_hours: i64,
    delete_count: usize,
}

impl Default for CustomStreamDurationDriver {
    fn default() -> Self {
        let last_case = DurationCase {
            table_hours: 2,
            item_hours: 2,
            marker_version: 1,
            current_policy_version: 1,
            current_version: 1,
            latest_version: 1,
            retained_table_pointer_version: 0,
            protected_before: 10,
        };
        Self {
            effective_hours: effective_hours(&last_case),
            last_case,
            last_outcome: "not_checked".to_string(),
            delete_count: 0,
        }
    }
}

impl Driver for CustomStreamDurationDriver {
    type State = CustomStreamDurationState;

    fn step(&mut self, step: &Step) -> Result {
        switch!(step {
            init => {
                *self = Self::default();
            },
            Check(
                tableHours: i64,
                itemHours: i64,
                markerVersion: u64,
                currentPolicyVersion: u64,
                currentVersion: u64,
                latestVersion: u64,
                retainedTablePointerVersion: u64,
                protectedBefore: u64
            ) => {
                self.check(DurationCase {
                    table_hours: tableHours,
                    item_hours: itemHours,
                    marker_version: markerVersion,
                    current_policy_version: currentPolicyVersion,
                    current_version: currentVersion,
                    latest_version: latestVersion.max(currentVersion),
                    retained_table_pointer_version: retainedTablePointerVersion,
                    protected_before: protectedBefore,
                })?;
            },
            step(
                tableHours: i64?,
                itemHours: i64?,
                markerVersion: u64?,
                currentPolicyVersion: u64?,
                currentVersion: u64?,
                latestVersion: u64?,
                retainedTablePointerVersion: u64?,
                protectedBefore: u64?
            ) => {
                if let (
                    Some(table_hours),
                    Some(item_hours),
                    Some(marker_version),
                    Some(current_policy_version),
                    Some(current_version),
                    Some(latest_version),
                    Some(retained_table_pointer_version),
                    Some(protected_before),
                ) = (
                    tableHours,
                    itemHours,
                    markerVersion,
                    currentPolicyVersion,
                    currentVersion,
                    latestVersion,
                    retainedTablePointerVersion,
                    protectedBefore,
                ) {
                    self.check(DurationCase {
                        table_hours,
                        item_hours,
                        marker_version,
                        current_policy_version,
                        current_version,
                        latest_version: latest_version.max(current_version),
                        retained_table_pointer_version,
                        protected_before,
                    })?;
                }
            },
        })
    }
}

impl CustomStreamDurationDriver {
    fn check(&mut self, case: DurationCase) -> Result {
        let effective_hours = effective_hours(&case);
        let backend = DurationBackend::new(case.clone(), effective_hours)?;
        let marker = backend.marker.clone();
        let stats = block_on(
            StreamDurationTrimWorker::new(
                backend,
                StreamDurationTrimConfig {
                    marker_page_size: 1,
                    stream_page_size: PAGE_SIZE,
                },
            )
            .run_due_page(hours(NOW_HOURS), hours(NOW_HOURS)),
        )?;

        self.last_outcome = if case.marker_version != case.current_policy_version {
            "stale_marker_ignored".to_string()
        } else if effective_hours == FOREVER {
            "forever_scope_skipped".to_string()
        } else if stats.rows_deleted == 0 {
            "not_due_or_protected".to_string()
        } else {
            "bounded_page_deleted".to_string()
        };
        self.delete_count = stats.rows_deleted;
        self.effective_hours = effective_hours;
        self.last_case = DurationCase {
            marker_version: marker.policy_version,
            ..case
        };
        Ok(())
    }
}

#[derive(Clone)]
struct DurationBackend {
    case: DurationCase,
    marker: StreamTrimDueMarker,
    state: StreamTrimState,
}

impl DurationBackend {
    fn new(case: DurationCase, effective_hours: i64) -> Result<Self> {
        let scope = StreamTrimScope::item("item/orders/hot", TableName::new("orders"), "hot");
        let marker = StreamTrimDueMarker::new(hours(NOW_HOURS), scope.clone(), case.marker_version);
        let retention = retention_from_hours(case.item_hours)?;
        let effective_retention = retention_from_hours(effective_hours)?;
        let state = StreamTrimState {
            scope,
            policy_version: case.current_policy_version,
            retention,
            effective_retention,
            next_due_at: Some(hours(NOW_HOURS)),
            oldest_retained_version: Some(ItemStreamVersion::new(case.current_version)),
            oldest_retained_timestamp: Some(hours(i64::try_from(case.current_version)?)),
            latest_version: Some(ItemStreamVersion::new(case.latest_version)),
            latest_timestamp: Some(hours(i64::try_from(case.latest_version)?)),
            updated_at: hours(0),
        };
        Ok(Self {
            case,
            marker,
            state,
        })
    }
}

#[async_trait]
impl StreamDurationTrimBackend for DurationBackend {
    async fn list_due_stream_trim_markers(
        &self,
        _due_before: TimestampMillis,
        _limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        Ok(vec![self.marker.clone()])
    }

    async fn load_stream_trim_state(
        &self,
        _scope: &StreamTrimScope,
    ) -> StorageResult<Option<StreamTrimState>> {
        Ok(Some(self.state.clone()))
    }

    async fn load_stream_trim_boundaries(
        &self,
        _scope: &StreamTrimScope,
    ) -> StorageResult<StreamTrimScopeBoundaries> {
        Ok(StreamTrimScopeBoundaries {
            latest_item_id: (self.case.latest_version > 0)
                .then(|| stream_id(self.case.latest_version)),
            protected_boundary: Some(StreamTrimBoundary {
                item_id: stream_id(self.case.protected_before),
            }),
            retained_table_pointer_boundary: (self.case.retained_table_pointer_version > 0).then(
                || StreamTrimBoundary {
                    item_id: stream_id(self.case.retained_table_pointer_version),
                },
            ),
        })
    }

    async fn trim_table_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_page(request)
    }

    async fn trim_item_stream_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.trim_page(request)
    }

    async fn finish_stream_trim_marker(
        &self,
        _marker: StreamTrimDueMarker,
        _write: Option<StreamTrimStateWrite>,
    ) -> StorageResult<()> {
        Ok(())
    }
}

impl DurationBackend {
    fn trim_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        if self.case.current_version == 0 {
            return Ok(empty_page(self.case.current_version));
        }
        let Some(max_deleted) = request.max_deleted_item_id else {
            return Ok(empty_page(self.case.current_version));
        };
        let cutoff_version = version_from_hours(request.cutoff_timestamp);
        let max_deleted_version = item_version(max_deleted);
        let delete_through = cutoff_version.min(max_deleted_version);
        if delete_through < self.case.current_version {
            return Ok(empty_page(self.case.current_version));
        }
        let available = usize::try_from(delete_through - self.case.current_version + 1)
            .map_err(|err| StorageError::internal(&format!("delete count overflow: {err}")))?;
        let deleted_rows = available.min(request.page_limit);
        let first_remaining = self
            .case
            .current_version
            .saturating_add(u64::try_from(deleted_rows).unwrap_or(u64::MAX));
        let first_remaining =
            (first_remaining <= self.case.latest_version).then_some(first_remaining);
        Ok(StreamDurationTrimPageResult {
            deleted_rows,
            first_remaining_version: first_remaining.map(ItemStreamVersion::new),
            first_remaining_timestamp: first_remaining
                .map(|version| hours(i64::try_from(version).unwrap_or(i64::MAX))),
        })
    }
}

fn empty_page(current_version: u64) -> StreamDurationTrimPageResult {
    let first_remaining = (current_version > 0).then_some(current_version);
    StreamDurationTrimPageResult {
        deleted_rows: 0,
        first_remaining_version: first_remaining.map(ItemStreamVersion::new),
        first_remaining_timestamp: first_remaining
            .map(|version| hours(i64::try_from(version).unwrap_or(i64::MAX))),
    }
}

fn effective_hours(case: &DurationCase) -> i64 {
    if case.table_hours == FOREVER || case.item_hours == FOREVER {
        FOREVER
    } else {
        case.table_hours.max(case.item_hours)
    }
}

fn retention_from_hours(hours: i64) -> Result<StreamRetentionDuration> {
    if hours == FOREVER {
        Ok(StreamRetentionDuration::Forever)
    } else {
        Ok(StreamRetentionDuration::FiniteHours(u16::try_from(hours)?))
    }
}

fn hours(value: i64) -> TimestampMillis {
    TimestampMillis::from_timestamp(value.saturating_mul(60 * 60 * 1000))
}

fn version_from_hours(timestamp: TimestampMillis) -> u64 {
    let hours = timestamp.timestamp_millis().div_euclid(60 * 60 * 1000);
    u64::try_from(hours).unwrap_or(0)
}

fn stream_id(version: u64) -> StreamItemId {
    StreamItemId::from(ItemStreamVersion::new(version))
}

fn item_version(item_id: StreamItemId) -> u64 {
    ItemStreamVersion::from(item_id).get()
}

fn block_on<F>(future: F) -> F::Output
where F: std::future::Future {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[quint_run(
    spec = "../../quint/custom_stream_duration_mbt.qnt",
    max_samples = 96,
    max_steps = 10,
    seed = "0xc57d"
)]
fn custom_stream_duration_mbt_matches_trim_worker() -> impl Driver {
    CustomStreamDurationDriver::default()
}
