//! Scripted observation ports test the real shared executor, not native
//! persistence.
use std::collections::VecDeque;

use storage_backfill::{
    GsiActivationState, GsiBuild, GsiBuildCandidates, GsiBuildTarget, GsiChange, GsiPageLimit,
    GsiProjection, GsiProjectionKey, GsiProjectionStateReadBatch, GsiReferencedHistoryItem,
    GsiReferencedHistoryRead, GsiScanObservation, GsiScanPageEnd, GsiScanSourcePage,
    GsiStreamObservation, GsiStreamPageEnd, GsiStreamSourcePage,
};
use storage_common::TtlConfigRecord;
use storage_types::{
    IndexName, Revision, StorageResult, SystemStreamCheckpoint, SystemStreamPosition,
    SystemStreamScope, TableName,
    maintenance::{TargetIncarnation, TaskProgress},
};

use super::{GsiBuildTransaction, GsiTransition};

#[derive(Debug, PartialEq, Eq)]
pub(super) enum Read {
    Scan(Option<u64>),
    Stream(u64, u64),
    Frontier(u64, u64),
    Tail,
    Activation(u64),
}

pub(super) struct Observations {
    pub(super) expected: VecDeque<Read>,
    pub(super) scan: Option<StorageResult<GsiScanObservation<u64>>>,
    pub(super) stream: Option<StorageResult<GsiStreamObservation>>,
    pub(super) tail: u64,
    pub(super) history_available: bool,
    pub(super) incarnation: TargetIncarnation,
}

impl Observations {
    pub(super) fn new(expected: impl IntoIterator<Item = Read>) -> Self {
        Self {
            expected: expected.into_iter().collect(),
            scan: None,
            stream: None,
            tail: 0,
            history_available: true,
            incarnation: target().incarnation,
        }
    }

    fn read(&mut self, actual: Read) {
        assert_eq!(self.expected.pop_front(), Some(actual));
    }

    pub(super) async fn evaluate(
        &mut self,
        build: &GsiBuild<u64>,
    ) -> StorageResult<GsiChange<u64>> {
        let result = self
            .evaluate_gsi_build(build, GsiPageLimit::try_from(2).unwrap())
            .await;
        assert!(
            self.expected.is_empty(),
            "unconsumed reads: {:?}",
            self.expected
        );
        result
    }
}

impl GsiBuildTransaction for Observations {
    type Cursor = u64;

    async fn read_gsi_scan(
        &mut self,
        actual: &GsiBuildTarget,
        after: Option<&u64>,
        limit: GsiPageLimit,
    ) -> StorageResult<GsiScanObservation<u64>> {
        assert!(actual == &target());
        assert_eq!(limit.including_lookahead(), 3);
        self.read(Read::Scan(after.copied()));
        self.scan.take().expect("scan observation")
    }

    async fn read_gsi_stream_observation(
        &mut self,
        actual: &GsiBuildTarget,
        after: SystemStreamCheckpoint,
        through: SystemStreamCheckpoint,
        limit: GsiPageLimit,
    ) -> StorageResult<GsiStreamObservation> {
        assert!(actual == &target());
        assert_eq!(limit.including_lookahead(), 3);
        self.read(Read::Stream(offset(&after), offset(&through)));
        self.stream.take().expect("stream observation")
    }

    async fn read_gsi_stream_tail(&mut self) -> StorageResult<SystemStreamCheckpoint> {
        self.read(Read::Tail);
        Ok(checkpoint(self.tail))
    }

    async fn validate_gsi_frontier(
        &mut self,
        current: &SystemStreamCheckpoint,
        through: &SystemStreamCheckpoint,
    ) -> StorageResult<()> {
        self.read(Read::Frontier(offset(current), offset(through)));
        if self.history_available {
            Ok(())
        } else {
            Err(storage_types::StorageEnum::HistoryUnavailable.into())
        }
    }

    async fn read_gsi_activation_state(
        &mut self,
        actual: &GsiBuildTarget,
        through: SystemStreamCheckpoint,
    ) -> StorageResult<GsiActivationState> {
        assert!(actual == &target());
        self.read(Read::Activation(offset(&through)));
        Ok(GsiActivationState {
            target_incarnation: self.incarnation.clone(),
            projected_through: through,
        })
    }

    async fn read_maintenance_identity(&mut self) -> StorageResult<TargetIncarnation> {
        unreachable!("evaluation cannot acquire scheduler authority")
    }

    async fn read_gsi_progress(
        &mut self,
    ) -> StorageResult<Option<TaskProgress<Option<GsiBuildTarget>>>> {
        unreachable!("evaluation cannot read scheduled progress")
    }

    async fn read_gsi_build_candidates(
        &mut self,
        _: Option<&GsiBuildTarget>,
    ) -> StorageResult<GsiBuildCandidates<u64>> {
        unreachable!("evaluation cannot select a different target")
    }

    async fn read_gsi_scan_source_page(
        &mut self,
        _: &GsiBuildTarget,
        _: Option<&u64>,
        _: GsiPageLimit,
    ) -> StorageResult<GsiScanSourcePage<u64>> {
        unreachable!("decoded scan observations are supplied by this unit fixture")
    }

    async fn read_gsi_stream_source_page(
        &mut self,
        _: &GsiBuildTarget,
        _: &SystemStreamCheckpoint,
        _: &SystemStreamCheckpoint,
        _: GsiPageLimit,
    ) -> StorageResult<GsiStreamSourcePage> {
        unreachable!("decoded stream observations are supplied by this unit fixture")
    }

    async fn read_gsi_projection_states(
        &mut self,
        _: GsiProjectionStateReadBatch<'_>,
    ) -> StorageResult<Vec<Option<GsiProjection<GsiProjectionKey>>>> {
        unreachable!("decoded observations include projection state")
    }

    async fn read_gsi_referenced_histories(
        &mut self,
        _: &[GsiReferencedHistoryRead<'_>],
    ) -> StorageResult<Vec<GsiReferencedHistoryItem>> {
        unreachable!("decoded observations include history")
    }

    async fn read_gsi_ttl_config(
        &mut self,
        _: &TableName,
    ) -> StorageResult<Option<TtlConfigRecord>> {
        unreachable!("evaluation cannot activate a TTL policy")
    }

    async fn write_gsi_progress(
        &mut self,
        _: Option<Revision>,
        _: TaskProgress<Option<GsiBuildTarget>>,
    ) -> StorageResult<()> {
        unreachable!("evaluation cannot persist scheduled progress")
    }

    async fn write_gsi_transition(&mut self, _: GsiTransition<'_, u64>) -> StorageResult<()> {
        unreachable!("the caller persists the decided transition")
    }
}

pub(super) fn target() -> GsiBuildTarget {
    GsiBuildTarget {
        table_name: TableName::new("observation"),
        index_name: IndexName::new("by-key"),
        incarnation: [1; 16].into(),
    }
}

pub(super) fn checkpoint(offset: u64) -> SystemStreamCheckpoint {
    SystemStreamCheckpoint::try_new(
        SystemStreamScope::try_new("observation", 1).unwrap(),
        vec![SystemStreamPosition::try_new("root", "root", format!("{offset:020}")).unwrap()],
    )
    .unwrap()
}

pub(super) fn offset(checkpoint: &SystemStreamCheckpoint) -> u64 {
    checkpoint.frontier()[0].offset().as_str().parse().unwrap()
}

pub(super) fn scan(
    after: Option<u64>,
    end: GsiScanPageEnd<u64>,
) -> StorageResult<GsiScanObservation<u64>> {
    Ok(GsiScanObservation {
        expected_after: after,
        end,
        projections: vec![],
    })
}

pub(super) fn stream(
    after: u64,
    through: u64,
    end: GsiStreamPageEnd,
) -> StorageResult<GsiStreamObservation> {
    Ok(GsiStreamObservation {
        expected_cursor: checkpoint(after),
        through: checkpoint(through),
        end,
        projections: vec![],
    })
}

pub(super) fn catching_up(low: u64, high: u64) -> GsiBuild<u64> {
    let mut build = GsiBuild::new(target(), checkpoint(low));
    let change = build
        .decide_next_change(storage_backfill::GsiObservation::Scan(
            scan(None, GsiScanPageEnd::Complete(checkpoint(high))).unwrap(),
        ))
        .unwrap();
    persist(&mut build, change);
    build
}

pub(super) fn persist(build: &mut GsiBuild<u64>, change: GsiChange<u64>) {
    let GsiChange::Persist {
        projections,
        next_state,
    } = change
    else {
        panic!("expected persisted change")
    };
    assert_eq!(projections.into_iter().count(), 0);
    build.state = next_state;
}
