//! Native SQL statements for exact index incarnation cleanup.

use storage_backfill::GsiBuildTarget;

use crate::{SqlLayout, SqlStatement, SqlValue};

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

pub(crate) fn drop_relation(engine: impl SqlLayout, target: &GsiBuildTarget) -> SqlStatement {
    let relation = engine.gsi_table_name(&target.incarnation);
    SqlStatement::new(format!("DROP TABLE IF EXISTS \"{relation}\""), vec![])
}
