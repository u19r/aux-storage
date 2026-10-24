use std::{collections::HashMap, ops::Deref};

use storage_types::{
    StorageError, StorageResult, StreamName, TimestampMillis, maintenance::TargetIncarnation,
};

use super::{
    DetachedItemStreamObservation, DetachedItemStreamRange, DetachedItemStreamRanges,
    DetachedItemStreamReclaim, StreamDurationTrimPageRequest, StreamTrimDeletionBound,
    StreamTrimPageLimit, StreamTrimState,
};

impl FromIterator<DetachedItemStreamRange> for DetachedItemStreamRanges {
    fn from_iter<T: IntoIterator<Item = DetachedItemStreamRange>>(ranges: T) -> Self {
        let mut merged = Vec::<DetachedItemStreamRange>::new();
        let mut positions = HashMap::<(TargetIncarnation, StreamName), usize>::new();
        for range in ranges {
            let identity = (range.table_incarnation.clone(), range.item_stream.clone());
            if let Some(position) = positions.get(&identity).copied() {
                merged[position].max_item_id = merged[position].max_item_id.max(range.max_item_id);
                continue;
            }
            positions.insert(identity, merged.len());
            merged.push(range);
        }
        Self { ranges: merged }
    }
}

impl IntoIterator for DetachedItemStreamRanges {
    type Item = DetachedItemStreamRange;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.ranges.into_iter()
    }
}

impl Deref for DetachedItemStreamRanges {
    type Target = [DetachedItemStreamRange];

    fn deref(&self) -> &Self::Target {
        &self.ranges
    }
}

impl DetachedItemStreamObservation {
    pub fn reclaim_request(
        self,
        observed_at: TimestampMillis,
        page_limit: StreamTrimPageLimit,
    ) -> StorageResult<Option<DetachedItemStreamReclaim>> {
        if self
            .custom_state
            .as_ref()
            .is_some_and(|state| state.scope != self.scope)
        {
            return Err(StorageError::internal(
                "detached item stream state does not match its scope",
            ));
        }
        let requested_retention = self
            .custom_state
            .as_ref()
            .map_or(self.default_item_retention, |state| state.retention);
        let effective_retention = storage_types::StreamRetentionDuration::effective_item_retention(
            self.table_retention,
            requested_retention,
        );
        let Some(cutoff_timestamp) = effective_retention.cutoff_at(observed_at) else {
            return Ok(None);
        };
        let max_deleted_item_id = match StreamTrimDeletionBound::for_reclamation(
            Some(self.range.max_item_id),
            self.boundaries.latest_item_id,
            self.boundaries
                .protected_boundary
                .map(|boundary| boundary.item_id),
            self.boundaries
                .retained_table_pointer_boundary
                .map(|boundary| boundary.item_id),
        ) {
            StreamTrimDeletionBound::Unbounded => self.range.max_item_id,
            StreamTrimDeletionBound::Inclusive(boundary) => boundary,
            StreamTrimDeletionBound::DeleteNone => return Ok(None),
        };
        Ok(Some(DetachedItemStreamReclaim {
            request: StreamDurationTrimPageRequest {
                scope: self.scope,
                cutoff_timestamp,
                max_deleted_item_id: Some(max_deleted_item_id),
                page_limit,
            },
            expected_table_incarnation: self.range.table_incarnation,
            expected_custom_state: self.custom_state,
            expected_table_retention: self.table_retention,
            expected_default_item_retention: self.default_item_retention,
        }))
    }
}

impl DetachedItemStreamReclaim {
    pub fn plan_batch(
        observations: Vec<DetachedItemStreamObservation>,
        observed_at: TimestampMillis,
        page_limit: StreamTrimPageLimit,
    ) -> StorageResult<Vec<Self>> {
        observations
            .into_iter()
            .filter_map(|observation| {
                observation
                    .reclaim_request(observed_at, page_limit)
                    .transpose()
            })
            .collect()
    }

    pub fn matches_current_policy(
        &self,
        table_incarnation: &TargetIncarnation,
        custom_state: Option<&StreamTrimState>,
        table_retention: storage_types::StreamRetentionDuration,
        default_item_retention: storage_types::StreamRetentionDuration,
    ) -> bool {
        self.expected_table_incarnation == *table_incarnation
            && self.expected_custom_state.as_ref() == custom_state
            && self.expected_table_retention == table_retention
            && self.expected_default_item_retention == default_item_retention
    }
}
