use std::ops::AddAssign;

use super::{StreamDurationTrimStats, StreamRetentionFinish, StreamRetentionFinishReason};

impl AddAssign for StreamDurationTrimStats {
    fn add_assign(&mut self, other: Self) {
        self.runtime_ms = self.runtime_ms.saturating_add(other.runtime_ms);
        self.due_markers_scanned = self
            .due_markers_scanned
            .saturating_add(other.due_markers_scanned);
        self.marker_page_full = self.marker_page_full || other.marker_page_full;
        self.stale_markers = self.stale_markers.saturating_add(other.stale_markers);
        self.forever_markers = self.forever_markers.saturating_add(other.forever_markers);
        self.missing_states = self.missing_states.saturating_add(other.missing_states);
        self.protected_markers = self
            .protected_markers
            .saturating_add(other.protected_markers);
        self.scopes_trimmed = self.scopes_trimmed.saturating_add(other.scopes_trimmed);
        self.rows_deleted = self.rows_deleted.saturating_add(other.rows_deleted);
        self.state_writes = self.state_writes.saturating_add(other.state_writes);
    }
}

impl StreamDurationTrimStats {
    pub fn did_work(&self) -> bool {
        self.rows_deleted > 0
            || self.stale_markers > 0
            || self.forever_markers > 0
            || self.missing_states > 0
    }

    pub(super) fn record_finishes(&mut self, finishes: &[StreamRetentionFinish]) {
        for finish in finishes {
            self.record_finish(finish);
        }
    }

    fn record_finish(&mut self, finish: &StreamRetentionFinish) {
        match finish.reason {
            StreamRetentionFinishReason::MissingState => {
                self.missing_states = self.missing_states.saturating_add(1);
            }
            StreamRetentionFinishReason::StaleMarker => {
                self.stale_markers = self.stale_markers.saturating_add(1);
            }
            StreamRetentionFinishReason::Forever => {
                self.forever_markers = self.forever_markers.saturating_add(1);
            }
            StreamRetentionFinishReason::Protected => {
                self.protected_markers = self.protected_markers.saturating_add(1);
            }
            StreamRetentionFinishReason::Page { deleted_rows } => {
                self.rows_deleted = self.rows_deleted.saturating_add(deleted_rows);
                self.scopes_trimmed = self
                    .scopes_trimmed
                    .saturating_add(usize::from(deleted_rows > 0));
            }
        }
        self.state_writes = self
            .state_writes
            .saturating_add(usize::from(finish.update.is_some()));
    }
}
