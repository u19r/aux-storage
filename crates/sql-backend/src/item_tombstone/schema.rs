//! Deleted-item revision authority shared by DeleteItem and transactions.

use storage_types::StorageResult;

use crate::{SqlAttempt, SqlLayout, SqlStatement};

/// Install the current deleted-item authority before publishing a SQL store.
pub async fn initialize_item_tombstones<A: SqlAttempt>(
    attempt: &mut A,
    engine: impl SqlLayout,
) -> StorageResult<()> {
    attempt
        .execute(SqlStatement::new(tombstone_schema(engine), vec![]))
        .await?;
    Ok(())
}

fn tombstone_schema(engine: impl SqlLayout) -> String {
    let text = engine.engine().text_type();
    let int = engine.engine().integer_type();
    let blob = engine.engine().blob_type();
    format!(
        "CREATE TABLE IF NOT EXISTS {relation} (table_id {blob} NOT NULL, key_json {text} NOT \
         NULL, revision {int} NOT NULL CHECK (revision > 0), provenance {blob} NOT NULL, PRIMARY \
         KEY (table_id, key_json)){}",
        engine.engine().table_suffix(),
        relation = engine.relation_name("item_tombstones"),
    )
}
