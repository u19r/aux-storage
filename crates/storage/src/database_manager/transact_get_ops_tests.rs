use storage_types::{StorageEnum, context::WrappedError};

use crate::database_manager::transact_get_ops::common_transact_get_connection_id;

#[test]
fn transact_get_requires_one_storage_connection() {
    assert_eq!(
        common_transact_get_connection_id(["replica-a", "replica-a"])
            .expect("same connection is compatible"),
        "replica-a"
    );
    let error = common_transact_get_connection_id(["replica-a", "replica-b"])
        .expect_err("different connections cannot share one snapshot");
    let StorageEnum::Unsupported { message } = error.to_enum() else {
        panic!("expected unsupported capability error: {error:?}");
    };
    assert!(message.contains("cannot guarantee one atomic snapshot"));
}
