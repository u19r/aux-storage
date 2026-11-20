//! Integer members: a JSON number, or a JSON string that holds an integer
//! (accepted before W14 and kept). The range check is the validation's.

use serde::{Deserialize, Deserializer, de::Error as _};

#[derive(Deserialize)]
#[serde(untagged)]
enum Integer {
    Number(i64),
    Text(String),
}

pub(crate) fn required<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
    match Integer::deserialize(deserializer)? {
        Integer::Number(value) => Ok(value),
        Integer::Text(text) => text
            .trim()
            .parse()
            .map_err(|_| D::Error::custom("an integer member is not an integer")),
    }
}

pub(crate) fn optional<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<i64>, D::Error> {
    Option::<serde_json::Value>::deserialize(deserializer)?
        .map(|value| required(value).map_err(D::Error::custom))
        .transpose()
}
