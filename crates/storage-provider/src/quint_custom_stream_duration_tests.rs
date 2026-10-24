#![allow(non_snake_case)]

use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use quint_connect::{Driver, Result, State, Step, quint_run, switch};
use serde::Deserialize;
use storage_types::{
    ItemStreamVersion, StorageError, StorageResult, StreamItemId, StreamRetentionDuration,
    TableName, TimestampMillis,
};

use crate::{
    StreamDurationTrimConfig, StreamDurationTrimPageRequest, StreamDurationTrimPageResult,
    StreamDurationTrimWorker, StreamRetentionRepository, StreamTrimBoundary, StreamTrimDueMarker,
    StreamTrimPageLimit, StreamTrimScope, StreamTrimScopeBoundaries, StreamTrimState,
    StreamTrimStateWrite,
    constants_tests::{
        CUSTOM_STREAM_DURATION_FOREVER as FOREVER, CUSTOM_STREAM_DURATION_NOW_HOURS as NOW_HOURS,
        CUSTOM_STREAM_DURATION_PAGE_SIZE as PAGE_SIZE,
    },
    maintenance::{ItemStreamDurationInput, StreamRetentionFinish, plan_item_stream_duration},
};

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
    #[serde(rename = "firstRemainingVersion")]
    first_remaining_version: u64,
    #[serde(rename = "oldestRetainedVersion")]
    oldest_retained_version: u64,
    #[serde(rename = "nextDueHours")]
    next_due_hours: i64,
    #[serde(rename = "markerState")]
    marker_state: String,
    #[serde(rename = "boundaryCeiling")]
    boundary_ceiling: u64,
}

impl State<CustomStreamDurationDriver> for CustomStreamDurationState {
    fn from_driver(driver: &CustomStreamDurationDriver) -> Result<Self> {
        Ok(Self {
            last_case: driver.last_case.clone(),
            last_outcome: driver.last_outcome.clone(),
            effective_hours: driver.effective_hours,
            delete_count: driver.delete_count,
            first_remaining_version: driver.first_remaining_version,
            oldest_retained_version: driver.oldest_retained_version,
            next_due_hours: driver.next_due_hours,
            marker_state: driver.marker_state.clone(),
            boundary_ceiling: driver.boundary_ceiling,
        })
    }
}

#[derive(Debug)]
struct CustomStreamDurationDriver {
    last_case: DurationCase,
    last_outcome: String,
    effective_hours: i64,
    delete_count: usize,
    first_remaining_version: u64,
    oldest_retained_version: u64,
    next_due_hours: i64,
    marker_state: String,
    boundary_ceiling: u64,
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
        let boundary_ceiling = boundary_limit(&last_case);
        Self {
            effective_hours: effective_hours(&last_case),
            last_case,
            last_outcome: "not_checked".to_string(),
            delete_count: 0,
            first_remaining_version: 1,
            oldest_retained_version: 1,
            next_due_hours: NOW_HOURS,
            marker_state: "pending".to_string(),
            boundary_ceiling,
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
        for (key, requested_hours) in [("hot", case.item_hours), ("cold", case.table_hours)] {
            let requested = retention_from_hours(requested_hours)?;
            let plan = plan_item_stream_duration(ItemStreamDurationInput {
                table_name: TableName::new("orders"),
                scope_id: format!("item/orders/{key}"),
                key_hash: key.into(),
                requested_retention: requested,
                table_retention: retention_from_hours(case.table_hours)?,
                updated_at: hours(0),
            });
            assert_eq!(
                plan.state.retention, requested,
                "planner lost requested retention for {key}"
            );
            assert_eq!(
                plan.state.effective_retention,
                retention_from_hours(
                    if requested_hours == FOREVER || case.table_hours == FOREVER {
                        FOREVER
                    } else {
                        requested_hours.max(case.table_hours)
                    }
                )?,
                "planner effective retention disagrees for {key}"
            );
        }
        let backend = DurationBackend::new(case.clone(), effective_hours)?;
        let marker = backend.marker.clone();
        let writes = backend.writes.clone();
        let initial_state = backend.state.clone();
        let rows = backend.rows.clone();
        let pointers = backend.table_pointers.clone();
        let requests = backend.last_requests.clone();
        let boundaries = backend.last_boundaries.clone();
        let stats = block_on(
            StreamDurationTrimWorker::new(
                &backend,
                StreamDurationTrimConfig {
                    marker_page_size: StreamTrimPageLimit::try_from(1usize)?,
                    stream_page_size: StreamTrimPageLimit::try_from(PAGE_SIZE)?,
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
        let final_state = writes
            .lock()
            .map_err(|_| std::io::Error::other("duration backend state lock poisoned"))?
            .as_ref()
            .map(|write| write.state.clone())
            .unwrap_or(initial_state);
        self.first_remaining_version = final_state
            .oldest_retained_version
            .map(|version| version.get())
            .unwrap_or(0);
        self.oldest_retained_version = self.first_remaining_version;
        self.next_due_hours = final_state
            .next_due_at
            .map(version_from_hours)
            .unwrap_or(0)
            .try_into()
            .unwrap_or(i64::MAX);
        self.marker_state = if case.marker_version != case.current_policy_version {
            "stale".to_string()
        } else if effective_hours == FOREVER {
            "forever".to_string()
        } else if stats.rows_deleted > 0 {
            "trimmed".to_string()
        } else {
            "protected_or_not_due".to_string()
        };
        self.boundary_ceiling = boundary_limit(&case);
        let rows = rows
            .lock()
            .map_err(|_| std::io::Error::other("duration rows lock poisoned"))?;
        if case.latest_version > 0 {
            assert!(
                rows.contains(&case.latest_version),
                "latest item row was deleted during cleanup"
            );
        }
        for pointer in pointers
            .lock()
            .map_err(|_| std::io::Error::other("duration pointers lock poisoned"))?
            .iter()
        {
            assert!(
                rows.contains(pointer),
                "retained table pointer {pointer} has no surviving item row"
            );
        }
        if let Some(request) = requests
            .lock()
            .map_err(|_| std::io::Error::other("duration requests lock poisoned"))?
            .as_ref()
        {
            let boundary = boundaries
                .lock()
                .map_err(|_| std::io::Error::other("duration boundaries lock poisoned"))?
                .as_ref()
                .map(copy_boundaries)
                .ok_or_else(|| std::io::Error::other("trim request had no boundaries"))?;
            let expected = boundary_limit(&case).checked_sub(1).map(stream_id);
            assert_eq!(request.max_deleted_item_id, expected);
            for ceiling in [
                boundary.latest_item_id,
                boundary.protected_boundary.map(|value| value.item_id),
                boundary
                    .retained_table_pointer_boundary
                    .map(|value| value.item_id),
            ]
            .into_iter()
            .flatten()
            {
                assert!(
                    request.max_deleted_item_id.is_none()
                        || item_version(request.max_deleted_item_id.expect("checked"))
                            < item_version(ceiling),
                    "trim crossed a protected/latest/retained-pointer ceiling"
                );
            }
        }
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
    writes: Arc<Mutex<Option<StreamTrimStateWrite>>>,
    rows: Arc<Mutex<BTreeSet<u64>>>,
    table_pointers: Arc<Mutex<Vec<u64>>>,
    last_requests: Arc<Mutex<Option<StreamDurationTrimPageRequest>>>,
    last_boundaries: Arc<Mutex<Option<StreamTrimScopeBoundaries>>>,
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
        let first_row = if case.retained_table_pointer_version > 0 {
            case.current_version
                .min(case.retained_table_pointer_version)
        } else {
            case.current_version
        };
        let rows = (first_row..=case.latest_version).collect();
        let table_pointers = (case.retained_table_pointer_version > 0)
            .then_some(vec![case.retained_table_pointer_version])
            .unwrap_or_default();
        Ok(Self {
            case,
            marker,
            state,
            writes: Arc::new(Mutex::new(None)),
            rows: Arc::new(Mutex::new(rows)),
            table_pointers: Arc::new(Mutex::new(table_pointers)),
            last_requests: Arc::new(Mutex::new(None)),
            last_boundaries: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl StreamRetentionRepository for DurationBackend {
    async fn trim_stream_marker_wave(
        &self,
        wave: crate::maintenance::StreamRetentionWave,
    ) -> StorageResult<crate::maintenance::StreamDurationTrimStats> {
        wave.execute(&mut crate::stream_retention_tests::ModelTransaction::new(
            self,
        ))
        .await
    }

    async fn list_due_stream_trim_markers(
        &self,
        _due_before: TimestampMillis,
        _limit: usize,
    ) -> StorageResult<Vec<StreamTrimDueMarker>> {
        Ok(vec![self.marker.clone()])
    }

    async fn load_stream_trim_states(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<Option<StreamTrimState>>> {
        Ok(scopes.iter().map(|_| Some(self.state.clone())).collect())
    }

    async fn load_stream_trim_boundaries(
        &self,
        scopes: &[StreamTrimScope],
    ) -> StorageResult<Vec<StreamTrimScopeBoundaries>> {
        scopes
            .iter()
            .map(|_| {
                let boundaries = StreamTrimScopeBoundaries {
                    latest_item_id: (self.case.latest_version > 0)
                        .then(|| stream_id(self.case.latest_version)),
                    protected_boundary: (self.case.protected_before > 0).then(|| {
                        StreamTrimBoundary {
                            item_id: stream_id(self.case.protected_before),
                        }
                    }),
                    retained_table_pointer_boundary: (self.case.retained_table_pointer_version > 0)
                        .then(|| StreamTrimBoundary {
                            item_id: stream_id(self.case.retained_table_pointer_version),
                        }),
                };
                self.last_boundaries
                    .lock()
                    .map_err(|_| StorageError::internal("duration boundaries lock poisoned"))?
                    .replace(copy_boundaries(&boundaries));
                Ok(boundaries)
            })
            .collect()
    }
}

impl DurationBackend {
    fn trim_page(
        &self,
        request: StreamDurationTrimPageRequest,
    ) -> StorageResult<StreamDurationTrimPageResult> {
        self.last_requests
            .lock()
            .map_err(|_| StorageError::internal("duration requests lock poisoned"))?
            .replace(request.clone());
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
        let deleted_rows = available.min(usize::from(request.page_limit));
        let first_remaining = {
            let mut rows = self
                .rows
                .lock()
                .map_err(|_| StorageError::internal("duration rows lock poisoned"))?;
            let deleted = rows
                .iter()
                .copied()
                .filter(|version| *version <= delete_through)
                .take(deleted_rows)
                .collect::<Vec<_>>();
            for version in deleted {
                rows.remove(&version);
            }
            rows.iter().next().copied()
        };
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

fn copy_boundaries(boundaries: &StreamTrimScopeBoundaries) -> StreamTrimScopeBoundaries {
    StreamTrimScopeBoundaries {
        latest_item_id: boundaries.latest_item_id,
        protected_boundary: boundaries.protected_boundary,
        retained_table_pointer_boundary: boundaries.retained_table_pointer_boundary,
    }
}

fn item_version(item_id: StreamItemId) -> u64 {
    ItemStreamVersion::from(item_id).get()
}

fn boundary_limit(case: &DurationCase) -> u64 {
    [
        Some(case.latest_version),
        (case.protected_before > 0).then_some(case.protected_before),
        (case.retained_table_pointer_version > 0).then_some(case.retained_table_pointer_version),
    ]
    .into_iter()
    .flatten()
    .min()
    .unwrap_or(999)
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
    main = "custom_stream_duration_mbt",
    init = "init",
    step = "step",
    max_samples = 96,
    max_steps = 10,
    seed = "0xc57d"
)]
fn custom_stream_duration_mbt_matches_trim_worker() -> impl Driver {
    CustomStreamDurationDriver::default()
}

impl crate::stream_retention_tests::ModelRetentionIo for DurationBackend {
    async fn trim_stream_pages(
        &self,
        requests: Vec<StreamDurationTrimPageRequest>,
    ) -> StorageResult<Vec<StreamDurationTrimPageResult>> {
        requests
            .into_iter()
            .map(|request| self.trim_page(request))
            .collect()
    }

    async fn finish_stream_trim_markers(
        &self,
        finishes: Vec<StreamRetentionFinish>,
    ) -> StorageResult<()> {
        for finish in finishes {
            if let Some(update) = finish.update {
                self.writes
                    .lock()
                    .map_err(|_| StorageError::internal("duration backend state lock poisoned"))?
                    .replace(update.write);
            }
        }
        Ok(())
    }
}
