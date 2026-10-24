use serde_json::Value;
use uuid::Uuid;

pub(crate) fn digest_value(value: &Value) -> String {
    let canonical = serialize(value);
    format!(
        "uuid5:oid:{}",
        Uuid::new_v5(&Uuid::NAMESPACE_OID, canonical.as_bytes())
    )
}

pub(crate) fn serialize(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::String(value) => serde_json::to_string(value).expect("JSON string encoding"),
        Value::Array(values) => {
            let mut output = String::from("[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serialize(value));
            }
            output.push(']');
            output
        }
        Value::Object(values) => {
            let mut entries: Vec<(&String, &Value)> = values.iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
            let mut output = String::from("{");
            for (index, (key, value)) in entries.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&serde_json::to_string(key).expect("JSON object key encoding"));
                output.push(':');
                output.push_str(&serialize(value));
            }
            output.push('}');
            output
        }
    }
}
