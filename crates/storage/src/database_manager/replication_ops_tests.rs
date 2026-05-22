use std::time::Instant;

use alloc_counter::AllocationGuard;
use storage_common::provider_perf::emit_runtime_report;
use storage_types::{
    ItemStreamVersion, ReplicationEventMetadata, ReplicationHybridLogicalClock,
    ReplicationWriteSource, StreamItemId, StreamName, TableName,
};
use stream::{EmbeddedStreamItem, StoredStreamPointer, StreamDataType};

use crate::{
    ReplicationMutationApplyOutcome,
    database_manager::replication_ops::{
        evaluate_replication_apply_outcome, replication_pointer_view,
    },
};

const POINTER_DECODE_ITERATIONS: usize = 512;

#[test]
fn given_embedded_stream_pointer_when_decoding_replication_view_then_embedded_items_are_skipped() {
    let item_stream_name = StreamName::new(b"item-stream");
    let item_stream_version = ItemStreamVersion::new(42);
    let replication = ReplicationEventMetadata {
        origin_region: "region-a".to_string(),
        origin_sequence: StreamItemId::default().increment(),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: 123.into(),
            logical: 4,
        },
        origin_commit_ts: 125.into(),
        table_replica_epoch: 2,
        write_source: ReplicationWriteSource::Replicated,
    };
    let pointer = StoredStreamPointer::embedded(
        item_stream_name.clone(),
        TableName::new("table"),
        item_stream_version,
        vec![EmbeddedStreamItem {
            data: vec![b'x'; 32 * 1024],
            data_type: StreamDataType::DynamoDbJson,
        }],
    )
    .with_replication_metadata(replication.clone());
    let pointer_data = storage_types::storage_serde::to_bytes(&pointer).unwrap();

    let view = replication_pointer_view(&pointer_data).unwrap();

    assert!(view.matches_item_version(&item_stream_name, item_stream_version));
    assert_eq!(view.replication_metadata(), Some(&replication));
}

#[test]
fn replication_pointer_view_reduces_embedded_pointer_decode_work() {
    let pointer_data = embedded_pointer_data(128 * 1024);

    let full_report = measure_full_pointer_decode(&pointer_data);
    alloc_counter::emit_report(&full_report);
    let view_report = measure_pointer_view_decode(&pointer_data);
    alloc_counter::emit_report(&view_report);

    assert!(
        view_report.allocation_count < full_report.allocation_count,
        "view allocation count should be lower: view={} full={}",
        view_report.allocation_count,
        full_report.allocation_count
    );
    assert!(
        view_report.allocated_bytes < full_report.allocated_bytes,
        "view allocated bytes should be lower: view={} full={}",
        view_report.allocated_bytes,
        full_report.allocated_bytes
    );
}

#[test]
fn given_fast_clock_current_when_slow_clock_causally_later_write_arrives_then_lww_skips_it() {
    let fast_clock_current = replication_metadata("region-fast", 1, 2_000);
    let slow_clock_later = replication_metadata("region-slow", 2, 1_000);

    let outcome = evaluate_replication_apply_outcome(Some(&fast_clock_current), &slow_clock_later);

    assert_eq!(outcome, ReplicationMutationApplyOutcome::SkippedStale);
}

#[test]
fn given_slow_clock_current_when_fast_clock_write_arrives_then_lww_applies_it() {
    let slow_clock_current = replication_metadata("region-slow", 1, 1_000);
    let fast_clock_incoming = replication_metadata("region-fast", 2, 2_000);

    let outcome =
        evaluate_replication_apply_outcome(Some(&slow_clock_current), &fast_clock_incoming);

    assert_eq!(outcome, ReplicationMutationApplyOutcome::Applied);
}

fn measure_full_pointer_decode(data: &[u8]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "replication_pointer_view_reduces_embedded_pointer_decode_work",
        file!(),
        line!(),
        Some("full_stored_stream_pointer"),
    );
    let started = Instant::now();
    for _ in 0..POINTER_DECODE_ITERATIONS {
        let pointer = storage_types::storage_serde::from_bytes::<StoredStreamPointer>(data)
            .expect("decode full pointer");
        assert!(pointer.embedded_items().is_some());
    }
    let report = guard.finish();
    emit_runtime_report(
        module_path!(),
        "replication_pointer_view_reduces_embedded_pointer_decode_work",
        "full_stored_stream_pointer",
        POINTER_DECODE_ITERATIONS,
        started.elapsed(),
    );
    report
}

fn measure_pointer_view_decode(data: &[u8]) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "replication_pointer_view_reduces_embedded_pointer_decode_work",
        file!(),
        line!(),
        Some("replication_pointer_view"),
    );
    let started = Instant::now();
    for _ in 0..POINTER_DECODE_ITERATIONS {
        let view = replication_pointer_view(data).expect("decode pointer view");
        assert!(view.replication_metadata().is_some());
    }
    let report = guard.finish();
    emit_runtime_report(
        module_path!(),
        "replication_pointer_view_reduces_embedded_pointer_decode_work",
        "replication_pointer_view",
        POINTER_DECODE_ITERATIONS,
        started.elapsed(),
    );
    report
}

fn embedded_pointer_data(payload_bytes: usize) -> Vec<u8> {
    let pointer = StoredStreamPointer::embedded(
        StreamName::new(b"item-stream"),
        TableName::new("table"),
        ItemStreamVersion::new(42),
        vec![EmbeddedStreamItem {
            data: vec![b'x'; payload_bytes],
            data_type: StreamDataType::DynamoDbJson,
        }],
    )
    .with_replication_metadata(ReplicationEventMetadata {
        origin_region: "region-a".to_string(),
        origin_sequence: StreamItemId::default().increment(),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: 123.into(),
            logical: 4,
        },
        origin_commit_ts: 125.into(),
        table_replica_epoch: 2,
        write_source: ReplicationWriteSource::Replicated,
    });
    storage_types::storage_serde::to_bytes(&pointer).unwrap()
}

fn replication_metadata(
    region_name: &str,
    sequence_suffix: u64,
    physical_ms: i64,
) -> ReplicationEventMetadata {
    let mut sequence_bytes = [0_u8; 12];
    sequence_bytes[4..].copy_from_slice(&sequence_suffix.to_be_bytes());
    ReplicationEventMetadata {
        origin_region: region_name.to_string(),
        origin_sequence: StreamItemId::from(sequence_bytes),
        origin_hlc: ReplicationHybridLogicalClock {
            physical_ms: physical_ms.into(),
            logical: 0,
        },
        origin_commit_ts: physical_ms.into(),
        table_replica_epoch: 1,
        write_source: ReplicationWriteSource::Replicated,
    }
}
