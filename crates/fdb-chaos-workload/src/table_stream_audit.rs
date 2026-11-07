//! Exact table references resolve to immutable history and qualified source
//! rows.

use kv_backend::{
    AttemptMode, KvAttempt, KvBackend, KvRange, ReadProtection,
    catalog::KvCatalogAttempt,
    stream_write::key_layout::{item_key, item_row_prefix, record_key},
};
use storage_provider::TableHistoryRetentionAttempt;
use storage_types::{
    ItemHistoryPointer, ItemHistoryPublication, StorageError, StorageResult, StreamItemId,
    SystemHistoryRecord, TableHistoryId, TableHistoryLane, TableHistoryTarget,
};
use stream_provider::{StreamRowValue, SystemHistoryReadAttempt};

pub(crate) async fn audit_stream_pointers<B: KvBackend>(
    driver: B,
    target: TableHistoryTarget,
    cache: std::sync::Arc<storage_provider::TableMetadataCache>,
    clock: &dyn storage_types::Clock,
) -> StorageResult<(u64, u64)> {
    storage_common::retry::retry_native_conflicts(
        clock,
        storage_provider::DYNAMO_WRITE_RETRY,
        || {
            let target = target.clone();
            let cache = cache.clone();
            driver.with_attempt(AttemptMode::ReadOnly, move |attempt| {
                Box::pin(async move {
                    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Snapshot);
                    let (table, _) = storage_provider::resolve_table_in_attempt(
                        &mut catalog,
                        &cache,
                        &target.table_name,
                    )
                    .await?;
                    if table.history_id != target.history_id {
                        return Err(StorageError::table_not_found(&target.table_name));
                    }
                    audit_pointer_rows(attempt, &table).await
                })
            })
        },
    )
    .await
}

async fn audit_pointer_rows(
    attempt: &mut impl KvAttempt,
    table: &storage_types::BoundTableInfo,
) -> StorageResult<(u64, u64)> {
    let (pages, frontier) = load_table_reference_pages(attempt, table).await?;
    let mut rows = 0;
    let mut decoupled = 0;
    for row in pages.into_iter().flat_map(|page| page.entries) {
        let (id, pointer) = decode_table_reference(row)?;
        let keys = reference_keys(table.history_id, id, &pointer)?;
        if frontier.is_retired(pointer.metadata())? {
            continue;
        }
        verify_reference(attempt, &pointer, keys).await?;
        rows += 1;
        decoupled += u64::from(id != StreamItemId::from(pointer.metadata().version()));
    }
    Ok((rows, decoupled))
}

async fn load_table_reference_pages(
    attempt: &mut impl KvAttempt,
    table: &storage_types::BoundTableInfo,
) -> StorageResult<(
    Vec<kv_backend::KvRangePage>,
    storage_provider::maintenance::TableHistoryTrimFrontier,
)> {
    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Snapshot);
    let history = catalog.read_system_page_start().await?;
    let state = catalog
        .load_table_history_retention(table, ReadProtection::Snapshot)
        .await?
        .ok_or_else(|| invalid("table history has no retention authority"))?;
    let source = history.as_ref();
    if state.frontier.table_history_id != table.history_id
        || &state.frontier.source_scope != source.source_scope()
    {
        return Err(invalid("table retention authority differs from its source"));
    }
    let ranges = source
        .entries()
        .iter()
        .map(|lineage| {
            let position = lineage
                .position("0")
                .map_err(|_| invalid("invalid audit lineage"))?;
            let lane =
                TableHistoryLane::try_from((table.history_id, source.source_scope(), &position))?;
            bounded_range(item_row_prefix(&lane.physical_stream_name()?)?)
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = attempt.scan_batch(ranges, ReadProtection::Snapshot).await?;
    if pages.len() != source.entries().len() || pages.iter().any(|page| page.more) {
        return Err(invalid("history audit exceeded complete-scan bound"));
    }
    Ok((pages, state.frontier))
}

fn decode_table_reference(
    row: kv_backend::KvEntry,
) -> StorageResult<(StreamItemId, ItemHistoryPointer)> {
    let id = decode_row_id(&row.key)?;
    let envelope = stream_provider::decode_table_history_index_row(id, &row.value)?;
    let mut pointer: ItemHistoryPointer = storage_types::storage_serde::from_bytes(&envelope.data)?;
    if matches!(pointer.metadata().publication(), ItemHistoryPublication::Event { table_position, .. } if table_position.offset().versionstamp().is_some())
    {
        pointer = pointer.with_versionstamp(*id.as_bytes())?;
    }
    Ok((id, pointer))
}

async fn verify_reference(
    attempt: &mut impl KvAttempt,
    pointer: &ItemHistoryPointer,
    keys: Vec<Vec<u8>>,
) -> StorageResult<()> {
    let values = attempt.get_batch(keys, ReadProtection::Snapshot).await?;
    if values.len() != 2 + usize::from(pointer.metadata().previous_version().is_some()) {
        return Err(invalid("history audit returned another value count"));
    }
    let mut values = values.into_iter();
    let body = values
        .next()
        .flatten()
        .ok_or_else(|| invalid("history body is absent"))?;
    verify_materialized_body(pointer, &body)?;
    verify_system_reference(attempt, pointer, values.next().flatten()).await?;
    if let Some(previous) = pointer.metadata().previous_version() {
        let body = values
            .next()
            .flatten()
            .ok_or_else(|| invalid("exact predecessor is absent"))?;
        StreamRowValue::decode_history_header_prefix(
            &body,
            body.len(),
            (pointer.metadata().address(), previous),
        )?;
    }
    Ok(())
}

fn reference_keys(
    history_id: TableHistoryId,
    id: StreamItemId,
    pointer: &ItemHistoryPointer,
) -> StorageResult<Vec<Vec<u8>>> {
    let metadata = pointer.metadata();
    let ItemHistoryPublication::Event { table_position, .. } = metadata.publication() else {
        return Err(invalid("table reference has no source publication"));
    };
    if metadata.address().history_id != history_id
        || storage_types::TableStreamSequence::try_from(table_position.offset())?.native_key() != id
    {
        return Err(invalid(
            "table reference identity differs from its physical key",
        ));
    }
    let stream = metadata.address().physical_stream_name()?;
    let mut keys = vec![
        item_key(&stream, metadata.version().into())?,
        record_key(table_position)?,
    ];
    if let Some(previous) = metadata.previous_version() {
        keys.push(item_key(&stream, previous.into())?);
    }
    Ok(keys)
}

fn verify_materialized_body(pointer: &ItemHistoryPointer, body: &[u8]) -> StorageResult<()> {
    let metadata = pointer.metadata();
    let header = StreamRowValue::decode_history_header_prefix(
        body,
        body.len(),
        (metadata.address(), metadata.version()),
    )?;
    if header.metadata() != metadata {
        return Err(invalid("history metadata differs from its reference"));
    }
    stream_provider::decode_materialized_stream_row(metadata.version().into(), body)?;
    Ok(())
}

async fn verify_system_reference(
    attempt: &mut impl KvAttempt,
    pointer: &ItemHistoryPointer,
    bytes: Option<Vec<u8>>,
) -> StorageResult<()> {
    let ItemHistoryPublication::Event { table_position, .. } = pointer.metadata().publication()
    else {
        return Err(invalid("table reference has no source publication"));
    };
    let Some(bytes) = bytes else {
        let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Snapshot);
        let frontiers = catalog
            .load_system_retired_frontiers(std::slice::from_ref(table_position))
            .await?;
        let [Some(retired)]: [Option<storage_types::SystemStreamPosition>; 1] = frontiers
            .try_into()
            .map_err(|_| invalid("system retirement audit returned another frontier count"))?
        else {
            return Err(invalid("system source row has no retirement authority"));
        };
        if table_position
            .offset()
            .compare_order(retired.offset())
            .map_err(|_| invalid("incomparable system retirement"))?
            .is_gt()
        {
            return Err(invalid("system source row disappeared before retirement"));
        }
        return Ok(());
    };
    let mut source: SystemHistoryRecord = storage_types::storage_serde::from_bytes(&bytes)?;
    if let Some(stamp) = table_position.offset().versionstamp() {
        source = source.with_versionstamp(stamp)?;
    }
    if source.pointer() != Some(pointer) {
        return Err(invalid("system and table references differ"));
    }
    Ok(())
}

fn decode_row_id(key: &[u8]) -> StorageResult<StreamItemId> {
    let length = StreamItemId::default().as_bytes().len();
    StreamItemId::try_from(
        key.get(key.len().saturating_sub(length)..)
            .ok_or_else(|| invalid("stream row key is truncated"))?,
    )
    .map_err(|_| invalid("stream row ID is invalid"))
}

fn bounded_range(begin: Vec<u8>) -> StorageResult<KvRange> {
    KvRange::try_for_prefix(
        begin,
        std::num::NonZeroU32::new(10_001).expect("nonzero audit bound"),
        std::num::NonZeroUsize::new(64 * 1024 * 1024).expect("nonzero audit bytes"),
    )
}

fn invalid(message: &str) -> StorageError {
    StorageError::internal(message)
}
