//! Retention wave construction and generation invariants.
use crate::StreamTrimScope;

struct RecordedMarkerWaves {
    markers: Vec<crate::StreamTrimDueMarker>,
    waves: std::sync::Mutex<Vec<Vec<crate::StreamTrimDueMarker>>>,
}

#[async_trait::async_trait]
impl crate::StreamRetentionRepository for RecordedMarkerWaves {
    async fn trim_stream_marker_wave(
        &self,
        wave: crate::maintenance::StreamRetentionWave,
    ) -> storage_types::StorageResult<crate::maintenance::StreamDurationTrimStats> {
        self.waves.lock().unwrap().push(wave.markers().to_vec());
        Ok(crate::maintenance::StreamDurationTrimStats {
            due_markers_scanned: wave.markers().len(),
            ..Default::default()
        })
    }

    async fn list_due_stream_trim_markers(
        &self,
        _: storage_types::TimestampMillis,
        limit: usize,
    ) -> storage_types::StorageResult<Vec<crate::StreamTrimDueMarker>> {
        Ok(self.markers.iter().take(limit).cloned().collect())
    }

    async fn load_stream_trim_states(
        &self,
        _: &[crate::StreamTrimScope],
    ) -> storage_types::StorageResult<Vec<Option<crate::StreamTrimState>>> {
        panic!("worker dispatch must leave observations inside each native wave")
    }
}

#[tokio::test]
async fn given_more_markers_than_native_wave_capacity_when_worker_dispatches_then_every_scope_is_bounded_and_serial()
 {
    use storage_types::{TableName, TimestampMillis};

    use crate::{StreamDurationTrimConfig, StreamDurationTrimWorker, StreamTrimDueMarker};

    let now = TimestampMillis::from_timestamp(0);
    let markers = (1..=2)
        .flat_map(|version| {
            (0..40).map(move |id| {
                let name = format!("table-{id:02}");
                StreamTrimDueMarker::new(
                    now,
                    StreamTrimScope::table(name.clone(), TableName::new(&name)),
                    version,
                )
            })
        })
        .collect::<Vec<_>>();
    let backend = RecordedMarkerWaves {
        markers,
        waves: Default::default(),
    };
    let stats = StreamDurationTrimWorker::new(&backend, StreamDurationTrimConfig::default())
        .run_due_page(now, now)
        .await
        .unwrap();
    assert_eq!(stats.due_markers_scanned, 80);
    let waves = backend.waves.lock().unwrap();
    assert!(
        waves
            .iter()
            .all(|wave| wave.len() <= crate::constants::TABLE_CLEANUP_PAGE_ROWS)
    );
    let mut observed = std::collections::BTreeMap::<String, Vec<u64>>::new();
    for wave in waves.iter() {
        let mut scopes = std::collections::HashSet::new();
        for marker in wave {
            assert!(scopes.insert(marker.scope.scope_id.clone()));
            observed
                .entry(marker.scope.scope_id.clone())
                .or_default()
                .push(marker.policy_version);
        }
    }
    assert_eq!(observed.len(), 40);
    assert!(observed.values().all(|versions| versions == &[1, 2]));
}

#[test]
fn given_same_scope_markers_when_constructing_wave_then_all_duplicate_versions_are_rejected() {
    use storage_types::{TableName, TimestampMillis};

    use crate::{StreamTrimDueMarker, StreamTrimPageLimit, maintenance::StreamRetentionWave};

    let now = TimestampMillis::from_timestamp(1);
    let scope = StreamTrimScope::table("table/one", TableName::new("one"));
    let marker = StreamTrimDueMarker::new(now, scope, 1);
    for version in [1, 2] {
        let mut duplicate = marker.clone();
        duplicate.policy_version = version;
        let wave = StreamRetentionWave::try_new(
            vec![marker.clone(), duplicate],
            now,
            now,
            StreamTrimPageLimit::try_from(1_usize).unwrap(),
        );
        assert!(
            wave.is_err(),
            "one scope cannot appear twice even with different versions"
        );
    }
}

#[test]
fn given_distinct_scope_markers_when_constructing_wave_then_order_and_markers_are_preserved() {
    use storage_types::{TableName, TimestampMillis};

    use crate::{StreamTrimDueMarker, StreamTrimPageLimit, maintenance::StreamRetentionWave};

    let now = TimestampMillis::from_timestamp(1);
    let markers = ["b", "a"]
        .map(|name| {
            StreamTrimDueMarker::new(now, StreamTrimScope::table(name, TableName::new(name)), 1)
        })
        .to_vec();
    let wave = StreamRetentionWave::try_new(
        markers.clone(),
        now,
        now,
        StreamTrimPageLimit::try_from(1_usize).unwrap(),
    )
    .expect("distinct scopes");
    assert!(wave.markers() == markers);
}

#[test]
fn given_max_policy_version_when_advancing_retention_policy_then_overflow_is_rejected() {
    use storage_types::{StreamRetentionDuration, TableName, TimestampMillis};

    use crate::StreamTrimState;

    let scope = StreamTrimScope::table("table/overflow", TableName::new("overflow"));
    let state = StreamTrimState {
        scope,
        policy_version: u64::MAX,
        frontier: crate::retention_test_support::frontier(u64::MAX),
        retention: StreamRetentionDuration::from_hours(24).unwrap(),
        next_due_at: None,
        updated_at: TimestampMillis::from_timestamp(1),
    };

    assert!(matches!(StreamTrimState::next_policy_version(None), Ok(1)));
    assert!(StreamTrimState::next_policy_version(Some(&state)).is_err());
}
