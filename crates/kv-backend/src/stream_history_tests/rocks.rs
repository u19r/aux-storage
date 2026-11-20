use std::num::NonZeroU64;

use storage_types::{AttemptPlan, DeadlineClass, SystemStreamScope};

use super::lineage_rows::sealed_row;
use crate::{
    AttemptMode, KvAttempt as _, KvBackend as _, KvMutation, ReadProtection,
    constants::SYSTEM_STREAM_LINEAGE_PREFIX as LINEAGE_PREFIX, rocksdb::RocksDbBackend,
    stream_history::*,
};

#[tokio::test]
async fn given_multiple_native_positions_when_flushed_then_next_snapshot_covers_every_offset() {
    let directory = tempfile::tempdir().unwrap();
    let backend = RocksDbBackend::initialize(
        directory.path().to_path_buf(),
        storage_types::AttemptDeadlines::default(),
        256,
    )
    .unwrap();
    backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    initialize_system_stream(attempt, storage_types::InitialStreamLineages::FOUR)
                        .await
                })
            },
        )
        .await
        .unwrap();
    let offsets = backend
        .with_attempt(
            AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write)
                .start(backend.attempt_deadlines()),
            |attempt| {
                Box::pin(async move {
                    let routes = read_writer_routes(attempt).await?;
                    attempt.install_writer_routes(routes)?;
                    let snapshot = attempt.native_snapshot_sequence().await?.unwrap();
                    let mut offsets = Vec::new();
                    for ordinal in 0..4u64 {
                        let event_number = attempt.reserve_system_event_number()?;
                        let routes = attempt.writer_routes().unwrap();
                        let position = routes
                            .writer_lineage(&ordinal.to_be_bytes())?
                            .position(format!("{:020}-ns", snapshot + u64::from(event_number) + 1))
                            .unwrap();
                        assert_eq!(
                            position.offset().as_str(),
                            format!("{:020}-ns", snapshot + ordinal + 1)
                        );
                        offsets.push(snapshot + ordinal + 1);
                        attempt.stage(vec![KvMutation::Put {
                            key: format!("entry-{ordinal}").into_bytes(),
                            value: vec![1; 128 * 1024],
                        }])?;
                        // A dependent read flushes each entry into the same
                        // native batch.
                        attempt.get(b"missing", ReadProtection::Conflict).await?;
                    }
                    Ok(offsets)
                })
            },
        )
        .await
        .unwrap();
    backend.with_attempt(AttemptPlan::new(AttemptMode::ReadWrite, DeadlineClass::Write).start(backend.attempt_deadlines()), move |attempt| Box::pin(async move {
        let snapshot = attempt.native_snapshot_sequence().await?.unwrap();
        assert!(offsets.iter().all(|offset| *offset <= snapshot));
        let page = attempt.scan(prefix_range(b"\x7fs/q/")?, ReadProtection::Conflict).await?;
        assert!(page.entries.is_empty());
        let high = capture_high(attempt).await?;
        assert!(high.frontier().iter().all(|position| position.offset().as_str() == format!("{snapshot:020}")));
        Ok(())
    })).await.unwrap();
}

#[test]
fn given_sparse_lineage_when_snapshot_or_end_is_invalid_then_history_is_rejected() {
    let scope = SystemStreamScope::try_new("scope", 1).unwrap();
    let mut row = sealed_row();
    row.sealed_after_offset = Some(43);
    let key = segmented_key(LINEAGE_PREFIX, &row.lineage_id).unwrap();
    assert!(decode_sparse_history(scope.clone(), vec![(key.clone(), row)], 41).is_err());
    let mut row = sealed_row();
    row.sealed = false;
    row.initial_sequence = NonZeroU64::new(43).unwrap();
    assert!(decode_sparse_history(scope, vec![(key, row)], 41).is_err());
}
