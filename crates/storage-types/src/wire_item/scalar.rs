use std::fmt::Display;

use serde::{Serialize, Serializer, ser::SerializeMap};

/// Borrowed scalar fields for entity-to-wire encoding without a JSON value
/// tree.
pub enum WireScalar<'a> {
    String(&'a str),
    Bool(bool),
    Number(&'a dyn Display),
}

impl Serialize for WireScalar<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let mut map = serializer.serialize_map(Some(1))?;
        match self {
            Self::String(value) => map.serialize_entry("S", value)?,
            Self::Bool(value) => map.serialize_entry("BOOL", value)?,
            Self::Number(value) => map.serialize_entry("N", &NumberRef(*value))?,
        }
        map.end()
    }
}

struct NumberRef<'a>(&'a dyn Display);

impl Serialize for NumberRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        serializer.collect_str(self.0)
    }
}
