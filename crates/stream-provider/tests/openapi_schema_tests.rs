#![allow(clippy::needless_for_each)]

use serde_json::Value;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(components(schemas(stream_provider::CreateStreamRequest)))]
struct StreamProviderApi;

#[test]
fn create_stream_request_name_rules_are_in_schema() {
    let doc = StreamProviderApi::openapi();
    let spec = serde_json::to_value(doc).expect("openapi json");
    let schema = find_property_schema(&spec, "CreateStreamRequest", "StreamName", "string");
    assert_eq!(schema["minLength"].as_u64(), Some(1));
    assert_eq!(schema["maxLength"].as_u64(), Some(255));
    assert_eq!(
        schema["pattern"],
        "^[a-zA-Z0-9][a-zA-Z0-9_.-]*[a-zA-Z0-9]$|^[a-zA-Z0-9]$"
    );
}

fn find_property_schema<'a>(
    spec: &'a Value,
    schema_name: &str,
    property: &str,
    expected_type: &str,
) -> &'a Value {
    let prop = &spec["components"]["schemas"][schema_name]["properties"][property];
    if let Some(any_of) = prop.get("anyOf").and_then(|value| value.as_array()) {
        return any_of
            .iter()
            .find(|entry| entry.get("type").and_then(Value::as_str) == Some(expected_type))
            .expect("expected schema variant");
    }
    prop
}
