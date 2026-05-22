const DIALECT_RS: &str = include_str!("dialect.rs");
const DRIVER_RS: &str = include_str!("driver.rs");
const PROVIDER_RS: &str = include_str!("provider.rs");
const PROVIDER_CORE_METADATA_RS: &str = include_str!("provider_core/statements/metadata.rs");
const SQL_TYPES_RS: &str = include_str!("sql_types.rs");

#[test]
fn shared_modules_do_not_depend_on_sqlite_types_or_crates() {
    let checks = [
        DIALECT_RS,
        DRIVER_RS,
        PROVIDER_RS,
        PROVIDER_CORE_METADATA_RS,
        SQL_TYPES_RS,
    ];
    let forbidden_tokens = [
        "rusqlite",
        "tokio_rusqlite",
        "SQLiteStorageProvider",
        "backends::sqlite",
        "sqlite::",
    ];

    for module_source in checks {
        for token in forbidden_tokens {
            assert!(
                !module_source.contains(token),
                "shared sql module must not contain sqlite-specific token `{token}`"
            );
        }
    }
}
