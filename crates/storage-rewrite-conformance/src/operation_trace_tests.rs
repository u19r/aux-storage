//! Provider-independent customer operation context for native failure traces.

use serde_json::json;
use storage_observability_test_support::observation::observe;
use storage_types::GetItemRequest;

use super::test_support::TestStore;

#[tokio::test]
async fn given_standard_table_when_customer_gets_item_then_trace_omits_identifiers_and_payload() {
    let store = TestStore::new().await;
    let table = store.create_standard_table().await;
    let request = GetItemRequest::try_from(json!({
        "TableName": table.as_ref(),
        "Key": {"pk": {"S": "private-customer-key"}}
    }))
    .unwrap();
    let observed = observe(store.operations.get_item(request)).await;
    assert_eq!(
        serde_json::to_value(observed.result.as_ref().unwrap()).unwrap(),
        json!({})
    );
    observed.assert_no_warning_or_error();
    let span = observed.one_span_with("dynamo_operation", &[("operation", "GetItem")]);
    assert_eq!(
        span.fields.len(),
        1,
        "operation span contains only safe context"
    );
    let traces = format!("{:?}{:?}", observed.events, observed.spans);
    assert!(!traces.contains("private-customer-key"));
    assert!(!traces.contains(table.as_ref()));
    store.finish().await;
}
