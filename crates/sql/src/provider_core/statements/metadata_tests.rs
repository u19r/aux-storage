use crate::{
    dialect::{PostgresDialect, SqliteDialect, TursoDialect},
    provider_core::statements::metadata,
};

#[test]
fn sqlite_and_turso_metadata_sql_matches_when_dialect_has_no_override() {
    let sqlite = SqliteDialect;
    let turso = TursoDialect;

    let sqlite_statements = [
        metadata::create_tables_table(&sqlite),
        metadata::create_gsi_backfill_table(&sqlite),
        metadata::create_ttl_config_table(&sqlite),
        metadata::create_item_revisions_table(&sqlite),
        metadata::table_exists(&sqlite, "users"),
        metadata::get_table_info(&sqlite, "users"),
        metadata::insert_table(&sqlite, "1", "users", 42, "[]", "[]", None, None, false),
        metadata::update_table_status(&sqlite, "ACTIVE", "users"),
        metadata::list_all_tables(&sqlite, 10),
        metadata::list_tables_after(&sqlite, 10, "accounts"),
        metadata::delete_table_metadata(&sqlite, "users"),
    ];
    let turso_statements = [
        metadata::create_tables_table(&turso),
        metadata::create_gsi_backfill_table(&turso),
        metadata::create_ttl_config_table(&turso),
        metadata::create_item_revisions_table(&turso),
        metadata::table_exists(&turso, "users"),
        metadata::get_table_info(&turso, "users"),
        metadata::insert_table(&turso, "1", "users", 42, "[]", "[]", None, None, false),
        metadata::update_table_status(&turso, "ACTIVE", "users"),
        metadata::list_all_tables(&turso, 10),
        metadata::list_tables_after(&turso, 10, "accounts"),
        metadata::delete_table_metadata(&turso, "users"),
    ];

    for (sqlite_statement, turso_statement) in sqlite_statements.iter().zip(turso_statements) {
        assert_eq!(sqlite_statement.sql, turso_statement.sql);
        assert_eq!(sqlite_statement.params, turso_statement.params);
    }
}

#[test]
fn postgres_metadata_sql_uses_postgres_placeholders() {
    let statement = metadata::list_tables_after(&PostgresDialect, 25, "users");

    assert!(statement.sql.contains("table_name > $1"));
    assert!(statement.sql.contains("LIMIT $2"));
}
