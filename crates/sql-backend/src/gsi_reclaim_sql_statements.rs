//! Native SQL statements for exact index incarnation cleanup.

use storage_backfill::GsiBuildTarget;

use crate::{SqlLayout, SqlStatement, SqlValue};

pub(crate) fn load_columns(
    engine: impl SqlLayout,
    history: storage_types::TableHistoryId,
    columns: &[storage_types::KeyColumn],
) -> SqlStatement {
    let mut binds = vec![SqlValue::Text(engine.item_table_name(history))];
    let placeholders = columns
        .iter()
        .map(|column| {
            binds.push(SqlValue::Text(column.physical_name()));
            engine.engine().placeholder(binds.len()).into_owned()
        })
        .collect::<Vec<_>>()
        .join(", ");
    // Another retired index can still reference a shared column. Its marker
    // removes that index concurrently before the last marker removes columns.
    SqlStatement::new(
        format!(
            "SELECT attname::text FROM pg_attribute AS a WHERE attrelid = to_regclass($1) AND NOT \
             attisdropped AND attname IN ({placeholders}) AND NOT EXISTS (SELECT 1 FROM pg_index \
             AS i WHERE i.indrelid = a.attrelid AND a.attnum = ANY(i.indkey))"
        ),
        binds,
    )
}

pub(crate) fn load_incarnation(engine: impl SqlLayout, target: &GsiBuildTarget) -> SqlStatement {
    SqlStatement::new(
        format!(
            "SELECT incarnation FROM {} WHERE table_id = {} AND index_name = {}",
            engine.relation_name("sys_gsi_builds"),
            engine.engine().placeholder(1),
            engine.engine().placeholder(2)
        ),
        vec![
            SqlValue::Blob(target.history_id.as_ref().to_vec()),
            SqlValue::Text(target.index_name.to_string()),
        ],
    )
}
