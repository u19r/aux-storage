use serde_json::json;

use crate::serde_types::reject_unknown_fields;

#[test]
fn reject_unknown_fields_reports_first_unknown_key() {
    let err = reject_unknown_fields(
        &json!({ "TableName": "users", "Unexpected": true }),
        &["TableName"],
    )
    .expect_err("unknown field should fail");

    assert_eq!(err, "Invalid request format: unknown field 'Unexpected'");
}
