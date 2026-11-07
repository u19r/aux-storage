//! One native wave binds markers, table incarnations, source bounds and
//! deletion progress.

use std::sync::Arc;

use storage_types::{BoundTableInfo, StorageError, StorageResult, StreamRetentionDuration};

use super::{
    StreamDurationTrimStats, StreamRetentionFinish, StreamRetentionFinishReason,
    StreamRetentionObservation, StreamRetentionPort, StreamRetentionWave, StreamTrimDueMarker,
    StreamTrimState, StreamTrimStateUpdate, StreamTrimStateWrite,
    history_reclaim::{plan_history_reclaims, select_cleanup_deletes},
    table_cleanup::TableCleanupWork,
};

pub(super) async fn execute_wave(
    wave: &StreamRetentionWave,
    attempt: &mut impl StreamRetentionPort,
) -> StorageResult<StreamDurationTrimStats> {
    let markers = wave.markers();
    let observations = attempt.load_stream_trim_observations(markers).await?;
    if observations.len() != markers.len() {
        return Err(invalid_wave());
    }
    let SelectedStates {
        mut finishes,
        pending,
    } = select_states(markers, observations, wave)?;
    if !pending.is_empty() {
        let mut works = load_cleanup_works(attempt, pending, &mut finishes).await?;
        observe_pointer_pages(attempt, &mut works, wave).await?;
        let decisions = plan_history_reclaims(attempt, &works).await?;
        let (deletes, last_lanes) = select_cleanup_deletes(&mut works, decisions)?;
        attempt.delete_history_rows(&deletes).await?;
        super::orphan_cleanup::advance_item_history_sweeps(attempt, &mut works, wave.observed_at)
            .await?;
        finishes.extend(
            works
                .into_iter()
                .zip(last_lanes)
                .map(|(work, lane)| work.finish(wave.observed_at, lane))
                .collect::<StorageResult<Vec<_>>>()?,
        );
    }
    let mut stats = StreamDurationTrimStats {
        due_markers_scanned: markers.len(),
        ..Default::default()
    };
    stats.record_finishes(&finishes);
    attempt.finish_stream_trim_markers(finishes).await?;
    Ok(stats)
}

async fn load_cleanup_works(
    attempt: &mut impl StreamRetentionPort,
    pending: Vec<(StreamTrimDueMarker, StreamTrimState)>,
    finishes: &mut Vec<StreamRetentionFinish>,
) -> StorageResult<Vec<TableCleanupWork>> {
    let source = attempt.read_system_page_start().await?;
    let tables = load_tables(attempt, &pending).await?;
    let mut works = Vec::new();
    for ((marker, state), table) in pending.into_iter().zip(tables) {
        if let Some(table) =
            table.filter(|table| table.history_id == state.frontier.table_history_id)
        {
            works.push(TableCleanupWork::prepare(marker, table, state, &source)?);
        } else {
            finishes.push(StreamRetentionFinish {
                marker,
                update: None,
                reason: StreamRetentionFinishReason::MissingState,
            });
        }
    }
    // Image admission is a bounded prefix. Give tables skipped by an earlier
    // byte limit priority over tables that already received a cleanup turn.
    works.sort_by_key(|work| work.state.updated_at);
    Ok(works)
}

struct SelectedStates {
    finishes: Vec<StreamRetentionFinish>,
    pending: Vec<(StreamTrimDueMarker, StreamTrimState)>,
}

fn select_states(
    markers: &[StreamTrimDueMarker],
    observations: Vec<StreamRetentionObservation>,
    wave: &StreamRetentionWave,
) -> StorageResult<SelectedStates> {
    let mut selected = SelectedStates {
        finishes: Vec::new(),
        pending: Vec::new(),
    };
    for (marker, observed) in markers.iter().cloned().zip(observations) {
        let StreamRetentionObservation::Present(state) = observed else {
            continue;
        };
        let Some(state) = state else {
            selected.finishes.push(StreamRetentionFinish {
                marker,
                update: None,
                reason: StreamRetentionFinishReason::MissingState,
            });
            continue;
        };
        selected.include_state(marker, state, wave)?;
    }
    Ok(selected)
}

impl SelectedStates {
    fn include_state(
        &mut self,
        marker: StreamTrimDueMarker,
        state: StreamTrimState,
        wave: &StreamRetentionWave,
    ) -> StorageResult<()> {
        state.require_valid_frontier()?;
        if !state.marker_matches(&marker) {
            self.finishes.push(StreamRetentionFinish {
                marker,
                update: None,
                reason: StreamRetentionFinishReason::StaleMarker,
            });
            return Ok(());
        }
        // A wide due window cannot authorize work before its observed deadline.
        if state
            .next_due_at
            .is_some_and(|due| due > wave.due_before.min(wave.observed_at))
        {
            return Ok(());
        }
        if state.retention == StreamRetentionDuration::Forever
            && state.frontier.item_history_sweep.is_none()
            && !state.frontier.has_pending_cleanup()?
        {
            self.finishes.push(finish_forever(marker, state));
        } else {
            self.pending.push((marker, state));
        }
        Ok(())
    }
}

fn finish_forever(
    marker: StreamTrimDueMarker,
    mut state: StreamTrimState,
) -> StreamRetentionFinish {
    let expected_state = state.clone();
    state.next_due_at = None;
    StreamRetentionFinish {
        marker,
        reason: StreamRetentionFinishReason::Forever,
        update: Some(StreamTrimStateUpdate {
            expected_state,
            write: StreamTrimStateWrite {
                state,
                next_marker: None,
                replaced_marker: None,
            },
        }),
    }
}

async fn load_tables(
    attempt: &mut impl StreamRetentionPort,
    pending: &[(StreamTrimDueMarker, StreamTrimState)],
) -> StorageResult<Vec<Option<Arc<BoundTableInfo>>>> {
    let names = pending
        .iter()
        .map(|(marker, _)| marker.scope.table_name.clone())
        .collect::<Vec<_>>();
    let stamps = attempt.read_catalog_stamps(&names).await?;
    if stamps.len() != names.len() {
        return Err(invalid_wave());
    }
    let requests = names
        .into_iter()
        .zip(&stamps)
        .filter_map(|(name, stamp)| stamp.map(|stamp| (name, stamp)))
        .collect::<Vec<_>>();
    let tables = attempt.read_tables_at_stamps(&requests).await?;
    if tables.len() != requests.len() {
        return Err(invalid_wave());
    }
    let mut tables = tables.into_iter();
    stamps
        .into_iter()
        .map(|stamp| match stamp {
            Some(_) => tables.next().map(Some).ok_or_else(invalid_wave),
            None => Ok(None),
        })
        .collect()
}

async fn observe_pointer_pages(
    attempt: &mut impl StreamRetentionPort,
    works: &mut [TableCleanupWork],
    wave: &StreamRetentionWave,
) -> StorageResult<()> {
    let groups = works
        .iter()
        .map(|work| work.reads(wave.page_limit))
        .collect::<StorageResult<Vec<_>>>()?;
    let counts = groups.iter().map(Vec::len).collect::<Vec<_>>();
    let reads = groups.into_iter().flatten().collect::<Vec<_>>();
    let mut pages = Vec::new();
    for reads in reads.chunks(crate::constants::TABLE_CLEANUP_PAGE_ROWS) {
        pages.extend(stream_provider::read_table_history_indexes(attempt, reads).await?);
    }
    let mut pages = pages.into_iter();
    for (work, count) in works.iter_mut().zip(counts) {
        work.observe_pages(pages.by_ref().take(count).collect(), wave.observed_at)?;
    }
    Ok(())
}

fn invalid_wave() -> StorageError {
    StorageError::internal("Table cleanup observation count differs")
}
