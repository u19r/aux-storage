//! Protected logical history pages for SQL GSI builds.

use storage_backfill::{GsiStreamPageAssembly, GsiStreamSourcePage};
use storage_provider::maintenance::GsiStreamSource;
use storage_types::{StorageEnum, StorageResult};
use stream_provider::{StreamReadKind, SystemStreamReadKind};

use crate::{ReadProtection, SqlAttempt, SqlLayout, constants::GSI_STREAM_PAGE_ROWS, stream_read};

pub(crate) async fn read_stream_source_page<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
    source: GsiStreamSource<'_>,
) -> StorageResult<GsiStreamSourcePage> {
    let GsiStreamSource {
        table,
        target: _,
        after,
        through,
        limit,
    } = source;
    let plan = stream_read::read_stream(
        attempt,
        engine,
        ReadProtection::Conflict,
        StreamReadKind::System(SystemStreamReadKind::Catalog),
    )
    .await?
    .into_system_catalog()?
    .plan_page(after, through, u32::from(limit).min(GSI_STREAM_PAGE_ROWS))
    .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let pages = stream_read::read_system_history_pages(attempt, engine, plan.as_ref()).await?;
    let mut assembly = GsiStreamPageAssembly::new(&plan);
    assembly.push_pages(plan.as_ref(), pages)?;
    assembly.finish(table, through)
}
