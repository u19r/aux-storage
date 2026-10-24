use storage_backfill::{
    GsiBuild, GsiChange, GsiFailureReason, GsiProjection, GsiProjectionKey, GsiScanPageEnd,
    GsiState, GsiStreamPageEnd,
};
use storage_types::StorageError;

use super::observation_test_support::{
    Observations, Read, catching_up, checkpoint, persist, scan, stream, target,
};

#[tokio::test]
async fn given_scan_and_stream_pages_when_evaluated_then_activation_requires_complete_drain() {
    let mut build = GsiBuild::new(target(), checkpoint(1));
    let mut first = Observations::new([Read::Scan(None)]);
    first.scan = Some(scan(None, GsiScanPageEnd::Continue(7)));
    let change = first.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(!build.state.permits_queries());
    assert!(build.state.required_stream_history() == &checkpoint(1));

    let mut last = Observations::new([Read::Scan(Some(7)), Read::Frontier(1, 4)]);
    last.scan = Some(scan(Some(7), GsiScanPageEnd::Complete(checkpoint(4))));
    let change = last.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(matches!(build.state, GsiState::CatchingUp(_)));

    for (after, end) in [
        (1, GsiStreamPageEnd::Continue(checkpoint(3))),
        (3, GsiStreamPageEnd::ReachedThrough),
    ] {
        let mut page = Observations::new([Read::Stream(after, 4)]);
        page.stream = Some(stream(after, 4, end));
        let change = page.evaluate(&build).await.unwrap();
        persist(&mut build, change);
        assert!(!build.state.permits_queries());
    }
    assert!(build.state.required_stream_history() == &checkpoint(4));
    let mut activation = Observations::new([Read::Frontier(4, 4), Read::Activation(4)]);
    let change = activation.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(build.state.permits_queries());
    assert!(build.state.required_stream_history() == &checkpoint(4));
}

#[tokio::test]
async fn given_active_partial_page_when_tail_changes_then_captured_high_is_drained_first() {
    let mut build = GsiBuild::new_active(target(), checkpoint(1));
    let mut first = Observations::new([Read::Tail, Read::Stream(1, 4)]);
    first.tail = 4;
    first.stream = Some(stream(1, 4, GsiStreamPageEnd::Continue(checkpoint(2))));
    let change = first.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(build.state.required_stream_history() == &checkpoint(2));

    let mut last = Observations::new([Read::Stream(2, 4)]);
    last.tail = 9;
    last.stream = Some(stream(2, 4, GsiStreamPageEnd::ReachedThrough));
    let change = last.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(build.state.required_stream_history() == &checkpoint(4));

    let mut next = Observations::new([Read::Tail, Read::Stream(4, 9)]);
    next.tail = 9;
    next.stream = Some(stream(4, 9, GsiStreamPageEnd::ReachedThrough));
    let change = next.evaluate(&build).await.unwrap();
    persist(&mut build, change);
    assert!(build.state.required_stream_history() == &checkpoint(9));
    assert!(build.state.permits_queries());
}

#[tokio::test]
async fn given_missing_history_when_scan_finishes_then_build_blocks_at_retained_frontier() {
    let build = GsiBuild::new(target(), checkpoint(1));
    let mut observation = Observations::new([Read::Scan(None), Read::Frontier(1, 4)]);
    observation.scan = Some(scan(None, GsiScanPageEnd::Complete(checkpoint(4))));
    observation.history_available = false;
    let GsiChange::Persist {
        next_state: GsiState::Blocked(blocked),
        ..
    } = observation.evaluate(&build).await.unwrap()
    else {
        panic!("must block")
    };
    assert!(blocked.reason == GsiFailureReason::HistoryUnavailable);
    assert!(blocked.retained_from == checkpoint(1));
}

#[tokio::test]
async fn given_missing_history_when_activation_is_inspected_then_no_readiness_read_occurs() {
    let build = catching_up(4, 4);
    let mut observation = Observations::new([Read::Frontier(4, 4)]);
    observation.history_available = false;
    let GsiChange::Persist {
        next_state: GsiState::Blocked(blocked),
        ..
    } = observation.evaluate(&build).await.unwrap()
    else {
        panic!("must block")
    };
    assert!(blocked.retained_from == checkpoint(4));
}

#[tokio::test]
async fn given_recreated_target_when_activation_is_inspected_then_old_build_is_superseded() {
    let build = catching_up(4, 4);
    let mut observation = Observations::new([Read::Frontier(4, 4), Read::Activation(4)]);
    observation.incarnation = [2; 16].into();
    assert!(matches!(
        observation.evaluate(&build).await.unwrap(),
        GsiChange::Superseded
    ));
}

#[tokio::test]
async fn given_nonhistory_read_failure_when_evaluated_then_error_propagates_without_transition() {
    let build = GsiBuild::new(target(), checkpoint(1));
    let mut observation = Observations::new([Read::Scan(None)]);
    observation.scan = Some(Err(StorageError::validation("invalid observation")));
    let error = observation
        .evaluate(&build)
        .await
        .err()
        .expect("read must fail");
    assert!(
        matches!(error.as_ref(), storage_types::StorageEnum::Validation { message, .. } if message == "invalid observation")
    );
    assert!(matches!(build.state, GsiState::Scanning(_)));
}

#[tokio::test]
async fn given_missing_stream_history_when_evaluated_then_catchup_and_active_builds_retain_progress()
 {
    for build in [
        catching_up(2, 4),
        GsiBuild::new_active(target(), checkpoint(2)),
    ] {
        let reads = if build.state.permits_queries() {
            vec![Read::Tail, Read::Stream(2, 4)]
        } else {
            vec![Read::Stream(2, 4)]
        };
        let mut observation = Observations::new(reads);
        observation.tail = 4;
        observation.stream = Some(Err(storage_types::StorageEnum::HistoryUnavailable.into()));
        let GsiChange::Persist {
            next_state: GsiState::Blocked(blocked),
            projections,
        } = observation.evaluate(&build).await.unwrap()
        else {
            panic!("must block")
        };
        assert!(blocked.reason == GsiFailureReason::HistoryUnavailable);
        assert!(blocked.retained_from == checkpoint(2));
        assert_eq!(projections.into_iter().count(), 0);
    }
}

#[tokio::test]
async fn given_active_build_at_tail_when_empty_page_is_read_then_no_transition_is_written() {
    let build = GsiBuild::new_active(target(), checkpoint(4));
    let mut observation = Observations::new([Read::Tail, Read::Stream(4, 4)]);
    observation.tail = 4;
    observation.stream = Some(stream(4, 4, GsiStreamPageEnd::ReachedThrough));
    assert!(matches!(
        observation.evaluate(&build).await.unwrap(),
        GsiChange::Idle
    ));
    assert!(build.state.required_stream_history() == &checkpoint(4));
}

#[tokio::test]
async fn given_wrong_scan_cursor_when_evaluated_then_progress_conflict_cannot_advance_build() {
    let build = GsiBuild::new(target(), checkpoint(1));
    let mut observation = Observations::new([Read::Scan(None)]);
    observation.scan = Some(scan(Some(7), GsiScanPageEnd::Continue(8)));
    let error = observation
        .evaluate(&build)
        .await
        .err()
        .expect("cursor must conflict");
    assert!(
        matches!(error.as_ref(), storage_types::StorageEnum::InternalServerError { message } if message == "GSI lifecycle progress changed before commit")
    );
    assert!(build.state.required_stream_history() == &checkpoint(1));
}

#[tokio::test]
async fn given_versioned_projection_when_stream_delete_is_observed_then_shared_version_policy_decides()
 {
    let mut index_key = storage_types::KeyAttributes::new();
    index_key.insert("group", storage_types::AttributeValue::S("old".to_owned()));
    let mut table_item_key = storage_types::KeyAttributes::new();
    table_item_key.insert("pk", storage_types::AttributeValue::S("item".to_owned()));
    let key = GsiProjectionKey {
        index_name: target().index_name,
        index_key,
        table_item_key,
        ttl_index_key: None,
    };
    for version in [2_u64, 3, 4] {
        let build = catching_up(1, 4);
        let mut observation = Observations::new([Read::Stream(1, 4)]);
        let mut page = stream(1, 4, GsiStreamPageEnd::ReachedThrough).unwrap();
        let revision = storage_types::ItemRevision::try_from(version).unwrap();
        page.projections
            .push(storage_backfill::GsiBuildProjectionObservation {
                source: storage_backfill::GsiSourceProjection::Missing {
                    item_version: revision,
                    previous_key: Some(Box::new(key.clone())),
                },
                current: Some(GsiProjection {
                    item_version: storage_types::ItemRevision::try_from(3_u64).unwrap(),
                    projection: Some(key.clone()),
                }),
            });
        observation.stream = Some(Ok(page));
        let GsiChange::Persist {
            next_state,
            projections,
        } = observation.evaluate(&build).await.unwrap()
        else {
            panic!("expected decision")
        };
        let writes: Vec<_> = projections.into_iter().collect();
        if version == 3 {
            assert!(
                matches!(next_state, GsiState::Blocked(ref blocked) if blocked.reason == GsiFailureReason::ProjectionConflict)
            );
            assert!(next_state.required_stream_history() == &checkpoint(1));
            assert!(writes.is_empty());
        } else {
            assert!(matches!(next_state, GsiState::CatchingUp(_)));
            assert!(next_state.required_stream_history() == &checkpoint(4));
            assert_eq!(writes.len(), usize::from(version == 4));
            if let Some(write) = writes.first() {
                assert!(write.key == key);
                assert!(write.row.is_none());
                assert_eq!(write.item_version, revision);
            }
        }
    }
}
