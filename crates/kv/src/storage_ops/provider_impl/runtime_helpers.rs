use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use storage_common::apply_gsi_write_pressure as apply_shared_gsi_write_pressure;
use storage_provider::ChangeIndexMarker;
use storage_types::{ItemStreamVersion, StorageResult, StreamItemId, TableName, TimestampMillis};

use crate::{
    SortedKvDbStorageProvider, keyspace::compact::KeyRange, sorted_kv_store::DirectWriteOperation,
};

static NEXT_STREAM_ITEM_VERSION: AtomicU64 = AtomicU64::new(0);

pub(crate) fn now_ms_u64() -> u64 {
    let now = *TimestampMillis::now();
    u64::try_from(now).unwrap_or(0)
}

pub(super) fn next_stream_item_id() -> StreamItemId {
    let now_component = now_ms_u64().checked_shl(20).unwrap_or(u64::MAX);
    let mut observed = NEXT_STREAM_ITEM_VERSION.load(Ordering::Relaxed);
    loop {
        let candidate = now_component.max(observed.saturating_add(1));
        match NEXT_STREAM_ITEM_VERSION.compare_exchange_weak(
            observed,
            candidate,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return StreamItemId::from(ItemStreamVersion::new(candidate)),
            Err(current) => observed = current,
        }
    }
}

pub(super) fn delete_range(range: KeyRange) -> DirectWriteOperation {
    DirectWriteOperation::DeleteRange {
        start: range.start,
        exclusive_end: range.end,
    }
}

pub(super) fn parse_change_index_key(
    slot: u16,
    prefix: &[u8],
    key: &[u8],
) -> Option<ChangeIndexMarker> {
    let suffix = key.strip_prefix(prefix)?;
    let suffix = std::str::from_utf8(suffix).ok()?;
    let (versionstamp, table_id) = suffix.rsplit_once('/')?;
    Some(ChangeIndexMarker {
        slot,
        versionstamp: versionstamp.to_owned(),
        table_id: TableName::new(table_id),
    })
}

pub(super) fn change_index_marker_created_at_ms(versionstamp: &str) -> Option<i64> {
    let stream_item_id = StreamItemId::from_str(versionstamp).ok()?;
    let version = ItemStreamVersion::from(stream_item_id).get();
    i64::try_from(version >> 20).ok()
}

pub(crate) fn should_log_job(last_log_ms: &AtomicU64, now_ms: u64, interval_ms: u64) -> bool {
    let last = last_log_ms.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last) >= interval_ms {
        last_log_ms.store(now_ms, Ordering::Relaxed);
        true
    } else {
        false
    }
}

pub(super) async fn apply_gsi_write_pressure<
    S: crate::partition_family::PartitionFamilyKvStore + 'static,
>(
    provider: &SortedKvDbStorageProvider<S>,
) -> StorageResult<()> {
    apply_shared_gsi_write_pressure(
        provider.immediate_gsi_consistency,
        &provider.gsi_propagation_governor,
        now_ms_u64(),
    )
    .await
}
