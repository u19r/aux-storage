//! Protected logical history pages for KV GSI builds.

use storage_backfill::{GsiStreamPageAssembly, GsiStreamSourcePage};
use storage_provider::maintenance::GsiStreamSource;
use storage_types::{StorageEnum, StorageResult};
use stream_provider::{StreamReadKind, SystemStreamReadKind};

use super::read::target_index;
use crate::{
    KvAttempt, ReadProtection, catalog::KvTableIdentity, constants::GSI_STREAM_PAGE_ROWS,
    stream_read,
};

pub(crate) async fn read_stream_source_page<A: KvAttempt>(
    attempt: &mut A,
    identity: &KvTableIdentity,
    source: GsiStreamSource<'_>,
) -> StorageResult<GsiStreamSourcePage> {
    let GsiStreamSource {
        table,
        target,
        after,
        through,
        limit,
    } = source;
    target_index(identity, target)?;
    let plan = stream_read::read_stream(
        attempt,
        ReadProtection::Conflict,
        StreamReadKind::System(SystemStreamReadKind::Catalog),
    )
    .await?
    .into_system_catalog()?
    .plan_page(after, through, u32::from(limit).min(GSI_STREAM_PAGE_ROWS))
    .map_err(|_| StorageEnum::HistoryUnavailable)?;
    let pages = stream_read::read_system_history_pages(attempt, plan.as_ref()).await?;
    let mut assembly = GsiStreamPageAssembly::new(&plan);
    assembly.push_pages(plan.as_ref(), pages)?;
    assembly.finish(table, through)
}
