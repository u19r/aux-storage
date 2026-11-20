//! Canonical table entries and SYSTEM membership share one native snapshot.

use kv_backend::{AttemptMode, KvAttempt, KvBackend, ReadProtection, catalog::KvCatalogAttempt};
use storage_provider::maintenance::StreamRetentionPort;
use storage_types::{
    AttemptPlan, DeadlineClass, StorageError, StorageResult, TableHistoryLane, TableHistoryTarget,
    TableStreamSequence,
};
use stream_provider::{
    PositionedStreamEntry, ReadDirection, StreamAttempt, SystemStreamHistoryOrdering,
    SystemStreamHistoryRead, TableHistoryIndexRead, read_table_history_indexes,
};

pub(crate) async fn audit_stream_entries<B: KvBackend>(
    driver: B,
    target: TableHistoryTarget,
    cache: std::sync::Arc<storage_provider::TableMetadataCache>,
    clock: &dyn storage_types::Clock,
) -> StorageResult<(u64, u64)> {
    storage_common::retry::retry_native_conflicts(
        clock,
        storage_provider::DYNAMO_WRITE_RETRY,
        driver.attempt_deadlines(),
        AttemptPlan::new(AttemptMode::ReadOnly, DeadlineClass::PointRead),
        |attempt_request| {
            let target = target.clone();
            let cache = cache.clone();
            driver.with_attempt(attempt_request, move |attempt| {
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
                    audit_entry_rows(attempt, &table).await
                })
            })
        },
    )
    .await
}

async fn audit_entry_rows(
    attempt: &mut impl KvAttempt,
    table: &storage_types::BoundTableInfo,
) -> StorageResult<(u64, u64)> {
    let mut catalog = KvCatalogAttempt::new(attempt, ReadProtection::Snapshot);
    let history = catalog.read_system_page_start_snapshot().await?;
    let source = history.as_ref();
    let state = catalog
        .load_table_history_retention(table, ReadProtection::Snapshot)
        .await?
        .ok_or_else(|| invalid("table history has no retention authority"))?;
    if state.frontier.table_history_id != table.history_id
        || &state.frontier.source_scope != source.source_scope()
    {
        return Err(invalid("table retention authority differs from its source"));
    }
    let origins = history
        .retirement_origins()
        .map_err(|_| invalid("invalid lineage origins"))?;
    let boundaries = history
        .retirement_boundaries()
        .map_err(|_| invalid("invalid lineage boundaries"))?;
    let retired = catalog.load_system_retired_frontiers(&origins).await?;
    if retired.len() != origins.len() {
        return Err(invalid("retirement observation count differs"));
    }
    let reads = boundaries
        .iter()
        .map(|position| {
            Ok(TableHistoryIndexRead {
                lane: TableHistoryLane::try_from((
                    table.history_id,
                    source.source_scope(),
                    position,
                ))?,
                exclusive_start: None,
                through: TableStreamSequence::try_from(position.offset())?,
                direction: ReadDirection::Forward,
                limit: std::num::NonZeroU32::new(10_001).expect("nonzero audit bound"),
            })
        })
        .collect::<StorageResult<Vec<_>>>()?;
    let pages = read_table_history_indexes(&mut catalog, &reads).await?;
    let mut entries = Vec::new();
    for (((page, origin), boundary), retired) in pages
        .into_iter()
        .zip(&origins)
        .zip(&boundaries)
        .zip(retired)
    {
        if page.more {
            return Err(invalid("table audit exceeded complete-scan bound"));
        }
        let mut after = retired.as_ref().unwrap_or(origin).clone();
        if after
            .offset()
            .compare_order(boundary.offset())
            .map_err(|_| invalid("invalid retirement boundary"))?
            .is_gt()
        {
            return Err(invalid("system retirement exceeds publication boundary"));
        }
        let mut feed = Vec::new();
        while after
            .offset()
            .compare_order(boundary.offset())
            .map_err(|_| invalid("incomparable audit bounds"))?
            .is_lt()
        {
            let before = after.clone();
            let read = SystemStreamHistoryRead {
                after: after.clone(),
                through: boundary
                    .offset()
                    .order()
                    .map_err(|_| invalid("invalid native high"))?,
                initial_sequence: std::num::NonZeroU64::new(
                    origin
                        .offset()
                        .order()
                        .map_err(|_| invalid("invalid native origin"))?
                        + 1,
                )
                .ok_or_else(|| invalid("invalid native origin"))?,
                limit: 10_001,
                ordering: if history.uses_commit_versionstamps() {
                    SystemStreamHistoryOrdering::Versionstamped
                } else {
                    SystemStreamHistoryOrdering::SparseNumeric
                },
            };
            let mut result = catalog.read_system_history_pages(&[read]).await?;
            if result.len() != 1 {
                return Err(invalid("system page count differs"));
            }
            let result = result.pop().expect("one page");
            for (entry, _) in result.records {
                if entry.position.shard_id() != origin.shard_id()
                    || entry.position.lineage_id() != origin.lineage_id()
                    || entry
                        .position
                        .offset()
                        .compare_order(after.offset())
                        .map_err(|_| invalid("incomparable feed position"))?
                        .is_le()
                    || entry
                        .position
                        .offset()
                        .compare_order(boundary.offset())
                        .map_err(|_| invalid("incomparable feed boundary"))?
                        .is_gt()
                {
                    return Err(invalid("feed position differs from exact native range"));
                }
                after = entry.position.clone();
                if entry.history_id == table.history_id {
                    feed.push(entry);
                }
            }
            if !result.more {
                break;
            }
            if after == before {
                return Err(invalid("system audit made no progress"));
            }
        }
        for entry in &page.entries {
            require_table_identity(entry, table, source.source_scope())?;
            let needs_feed = retired
                .as_ref()
                .map(|floor| {
                    entry
                        .position
                        .offset()
                        .compare_order(floor.offset())
                        .map(|order| order.is_gt())
                })
                .transpose()
                .map_err(|_| invalid("incomparable retirement"))?
                .unwrap_or(true);
            if needs_feed && !feed.iter().any(|record| record.position == entry.position) {
                return Err(invalid("table entry has no surviving system membership"));
            }
        }
        for record in feed {
            require_table_identity(&record, table, source.source_scope())?;
            let entry = page
                .entries
                .iter()
                .find(|entry| entry.position == record.position)
                .ok_or_else(|| invalid("system membership has no table entry"))?;
            if entry.encode_value()? != record.encode_value()? {
                return Err(invalid("system and table entries differ"));
            }
        }
        entries.extend(page.entries);
    }
    // Parent lineages precede descendants. Independent roots or sibling
    // branches may not publish successive mutations of the same complete
    // logical key.
    let mut ordered = Vec::new();
    let mut remaining = source.entries().iter().collect::<Vec<_>>();
    while !remaining.is_empty() {
        let index = remaining
            .iter()
            .position(|lineage| {
                lineage
                    .parent_lineage_id()
                    .is_none_or(|parent| ordered.contains(&parent))
            })
            .ok_or_else(|| invalid("lineage ancestry is cyclic"))?;
        ordered.push(remaining.remove(index).lineage_id());
    }
    entries.sort_by_key(|entry| {
        (
            ordered
                .iter()
                .position(|id| *id == entry.position.lineage_id())
                .expect("qualified lineage"),
            TableStreamSequence::try_from(entry.position.offset())
                .expect("validated native sequence"),
        )
    });
    let comparisons = verify_image_chains(&entries, source, table)?;
    Ok((entries.len() as u64, comparisons))
}

fn require_table_identity(
    entry: &PositionedStreamEntry,
    table: &storage_types::BoundTableInfo,
    scope: &storage_types::SystemStreamScope,
) -> StorageResult<()> {
    if entry.history_id != table.history_id
        || entry.source_table_name != table.table_name
        || &entry.source_scope != scope
    {
        return Err(invalid(
            "canonical entry differs from table incarnation/source",
        ));
    }
    Ok(())
}

pub(super) fn verify_image_chains(
    entries: &[PositionedStreamEntry],
    source: &stream_provider::SystemStreamLineageCatalog,
    table: &storage_types::BoundTableInfo,
) -> StorageResult<u64> {
    let mut previous: Vec<(Vec<u8>, &PositionedStreamEntry)> = Vec::new();
    let mut comparisons = 0;
    for entry in entries {
        let key = storage_types::ItemKey::from_key_schema(
            table.table_name.clone(),
            &table.key_schema,
            entry.entry.keys(),
        )
        .and_then(|key| key.sorted_storage_suffix())
        .map_err(|_| invalid("invalid logical item key"))?;
        if let Some(index) = previous.iter().position(|(previous, _)| previous == &key) {
            let prior = previous[index].1;
            let mut lineage = source
                .lineage(entry.position.lineage_id())
                .ok_or_else(|| invalid("unknown lineage"))?;
            while lineage.lineage_id() != prior.position.lineage_id() {
                lineage = lineage
                    .parent_lineage_id()
                    .and_then(|parent| source.lineage(parent))
                    .ok_or_else(|| invalid("item chain spans unrelated lineages"))?;
            }
            let old = entry
                .entry
                .old_image()
                .map(storage_types::storage_serde::to_bytes)
                .transpose()?;
            let new = prior
                .entry
                .new_image()
                .map(storage_types::storage_serde::to_bytes)
                .transpose()?;
            if old != new {
                return Err(invalid(
                    "successor old image differs from previous new image",
                ));
            }
            previous[index] = (key, entry);
            comparisons += 1;
        } else {
            // Retention may have removed the predecessor of the first witness.
            previous.push((key, entry));
        }
    }
    Ok(comparisons)
}

fn invalid(message: &str) -> StorageError {
    StorageError::internal(message)
}
