//! Shared GSI schema for SQLite, Turso, and Postgres.

use crate::{SqlEngine, SqlLayout};

pub(in crate::gsi) fn gsi_schema(engine: impl SqlLayout) -> [String; 5] {
    let text = engine.engine().text_type();
    let blob = engine.engine().blob_type();
    let int = engine.engine().integer_type();
    let length = if engine.engine() == SqlEngine::Postgres {
        "OCTET_LENGTH"
    } else {
        "LENGTH"
    };
    [
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_gsi_builds} (table_name {text} NOT NULL, \
             index_name {text} NOT NULL, incarnation {blob} NOT NULL CHECK ({length}(incarnation) \
             > 0), phase {text} NOT NULL CHECK (phase IN ('scanning', 'catching-up', 'active', \
             'blocked')), state {blob} NOT NULL, PRIMARY KEY (table_name, index_name)){}",
            engine.engine().table_suffix(),
            relation_sys_gsi_builds = engine.relation_name("sys_gsi_builds"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_gsi_history_holds} (table_name {text} NOT \
             NULL, index_name {text} NOT NULL, table_incarnation {text} NOT NULL, \
             index_incarnation {blob} NOT NULL, source_scope {text} NOT NULL, source_generation \
             {int} NOT NULL, shard_id {text} NOT NULL, lineage_id {text} NOT NULL, \
             consumed_offset {int} NOT NULL CHECK (consumed_offset >= 0), PRIMARY KEY \
             (table_name, index_name, index_incarnation, lineage_id)){}",
            engine.engine().table_suffix(),
            relation_sys_gsi_history_holds = engine.relation_name("sys_gsi_history_holds"),
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {relation_sys_gsi_name_locks} (table_id {text} NOT NULL, \
             index_name {text} NOT NULL, locked_until {int} NOT NULL, PRIMARY KEY (table_id, \
             index_name)){}",
            engine.engine().table_suffix(),
            relation_sys_gsi_name_locks = engine.relation_name("sys_gsi_name_locks"),
        ),
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["idx_gsi_builds_schedule"],
            " ON "["sys_gsi_builds"],
            " (table_name, index_name) WHERE phase <> 'blocked'"
        )
        .into_owned(),
        crate::sql_template::sql_template!(
            engine,
            "CREATE INDEX IF NOT EXISTS "["idx_gsi_history_holds_min"],
            " ON "["sys_gsi_history_holds"],
            " (source_scope, source_generation, shard_id, lineage_id, consumed_offset, \
             index_incarnation)"
        )
        .into_owned(),
    ]
}
