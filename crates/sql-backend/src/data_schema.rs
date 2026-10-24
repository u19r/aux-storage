//! Initialize one physical SQL data address space with the canonical family
//! schemas.

use storage_types::StorageResult;

use crate::{
    SqlAttempt, SqlLayout, catalog::initialize_catalog, gsi::initialize_gsi,
    item_tombstone::initialize_item_tombstones, maintenance::initialize_maintenance,
    pubsub_delivery::initialize_pubsub_deliveries,
    pubsub_publication::initialize_pubsub_publications,
    pubsub_subscription::initialize_pubsub_subscriptions, pubsub_topic::initialize_pubsub_topics,
    queue_catalog::initialize_queue_catalog, queue_message::initialize_queue_messages,
    retention::initialize_retention, stream_catalog::initialize_stream_catalog,
    stream_checkpoint::initialize_checkpoints, stream_cursor::initialize_stream_cursors,
    stream_history::initialize_system_stream, stream_trim::initialize_trim_receipts,
    stream_write::initialize_stream_rows, table_reclaim::initialize_table_reclaim,
    transact_write::initialize_transact_receipts,
};

pub(crate) async fn initialize_data_schema<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    initialize_catalog(attempt, engine).await?;
    initialize_table_reclaim(attempt, engine).await?;
    initialize_queue_catalog(attempt, engine).await?;
    initialize_queue_messages(attempt, engine).await?;
    initialize_pubsub_topics(attempt, engine).await?;
    initialize_pubsub_subscriptions(attempt, engine).await?;
    initialize_pubsub_publications(attempt, engine).await?;
    initialize_pubsub_deliveries(attempt, engine).await?;
    initialize_item_tombstones(attempt, engine).await?;
    initialize_maintenance(attempt, engine).await?;
    initialize_stream_rows(attempt, engine).await?;
    initialize_system_stream(attempt, engine).await?;
    initialize_checkpoints(attempt, engine).await?;
    initialize_stream_catalog(attempt, engine).await?;
    initialize_stream_cursors(attempt, engine).await?;
    initialize_trim_receipts(attempt, engine).await?;
    initialize_retention(attempt, engine).await?;
    initialize_gsi(attempt, engine).await?;
    initialize_transact_receipts(attempt, engine).await
}
