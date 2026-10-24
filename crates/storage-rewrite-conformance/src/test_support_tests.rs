use crate::test_support::required_backend_setting;

/// Validation rule: remote backend selection names the missing setting and a
/// runnable example.
#[cfg(feature = "postgres")]
#[test]
fn given_missing_postgres_dsn_when_backend_is_selected_then_error_explains_setup() {
    let error = required_backend_setting(
        "TEST_POSTGRES_DSN",
        None,
        "TEST_POSTGRES_DSN='postgresql://user@localhost/testdb' just test '-p \
         storage-rewrite-conformance --no-default-features --features postgres'",
    )
    .expect_err("missing DSN must fail");

    assert_eq!(
        error,
        "TEST_POSTGRES_DSN is required for this backend; set it before running tests, for \
         example: TEST_POSTGRES_DSN='postgresql://user@localhost/testdb' just test '-p \
         storage-rewrite-conformance --no-default-features --features postgres'"
    );
    assert_eq!(
        required_backend_setting("TEST_POSTGRES_DSN", Some("  "), "example")
            .expect_err("blank DSN must fail"),
        "TEST_POSTGRES_DSN is required for this backend; set it before running tests, for \
         example: example"
    );
}

/// Validation rule: a missing cluster file never silently selects a local FDB
/// default.
#[cfg(feature = "foundationdb")]
#[test]
fn given_missing_foundationdb_cluster_file_when_backend_is_selected_then_error_explains_setup() {
    let error = required_backend_setting(
        "FDB_CLUSTER_FILE",
        None,
        "FDB_CLUSTER_FILE=/path/to/fdb.cluster just test '-p storage-rewrite-conformance \
         --no-default-features --features foundationdb'",
    )
    .expect_err("missing cluster file must fail");

    assert_eq!(
        error,
        "FDB_CLUSTER_FILE is required for this backend; set it before running tests, for example: \
         FDB_CLUSTER_FILE=/path/to/fdb.cluster just test '-p storage-rewrite-conformance \
         --no-default-features --features foundationdb'"
    );
}
