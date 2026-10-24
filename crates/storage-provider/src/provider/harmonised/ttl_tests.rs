use storage_common::TtlIndexWriteBatch;
use storage_types::TableName;

use super::models::TtlWriteInput;

#[test]
fn given_empty_batch_when_construct_then_error() {
    assert!(
        TtlWriteInput::try_new(TableName::new("orders"), TtlIndexWriteBatch::empty()).is_err(),
        "empty batch rejects"
    );
}
