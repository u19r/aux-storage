use crate::provider::implementation::remote_request_context;

#[test]
fn given_query_body_when_remote_request_context_read_then_table_and_index_are_returned() {
    let body = br#"{"TableName":"tenant_data","IndexName":"gsi1"}"#;

    let context = remote_request_context(body);

    assert_eq!(context.table_name.as_deref(), Some("tenant_data"));
    assert_eq!(context.index_name.as_deref(), Some("gsi1"));
}

#[test]
fn given_invalid_body_when_remote_request_context_read_then_context_is_empty() {
    let context = remote_request_context(b"not-json");

    assert_eq!(context, Default::default());
}

#[test]
fn given_put_item_body_when_remote_request_context_read_then_key_is_returned() {
    let body = br#"{
        "TableName": "tenant_data",
        "Item": {
            "pk": {"S": "ZC"},
            "sk": {"S": "CURV"}
        }
    }"#;

    let context = remote_request_context(body);

    assert_eq!(context.table_name.as_deref(), Some("tenant_data"));
    assert_eq!(context.item_pk.as_deref(), Some("ZC"));
    assert_eq!(context.item_sk.as_deref(), Some("CURV"));
}
