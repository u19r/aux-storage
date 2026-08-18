use serde::Serialize;
use serde_json::{Map, Value};

/// Serialize a value with object keys in lexical order at every nesting level.
///
/// Request and continuation digests must be stable when their wire maps were
/// built from hash maps. The regular serde serializer is allowed to preserve
/// hash-map iteration order, so digest callers use this small canonical form.
pub fn to_vec<T: Serialize>(value: &T) -> Result<Vec<u8>, serde_json::Error> {
    serde_json::to_vec(&canonicalize(serde_json::to_value(value)?))
}

fn canonicalize(value: Value) -> Value {
    match value {
        Value::Object(fields) => {
            let mut entries = fields.into_iter().collect::<Vec<_>>();
            entries.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let mut canonical = Map::with_capacity(entries.len());
            for (name, value) in entries {
                canonical.insert(name, canonicalize(value));
            }
            Value::Object(canonical)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonicalize).collect()),
        other => other,
    }
}
