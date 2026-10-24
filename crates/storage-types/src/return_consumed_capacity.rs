use serde::{Deserialize, Serialize, de::DeserializeOwned};
use utoipa::ToSchema;

pub(crate) fn deserialize_request_value<T: DeserializeOwned>(
    mut value: serde_json::Value,
) -> Result<(T, Option<String>), serde_json::Error> {
    let invalid_capacity = value
        .get("ReturnConsumedCapacity")
        .and_then(serde_json::Value::as_str)
        .filter(|value| ReturnConsumedCapacity::parse(value).is_none())
        .map(str::to_owned);
    if invalid_capacity.is_some() {
        value["ReturnConsumedCapacity"] =
            serde_json::Value::String(ReturnConsumedCapacity::None.as_str().to_owned());
    }
    let request = serde_json::from_value(value)?;
    Ok((request, invalid_capacity))
}

/// Capacity detail requested from a DynamoDB-compatible operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReturnConsumedCapacity {
    None,
    Total,
    Indexes,
}

impl ReturnConsumedCapacity {
    /// Exact DynamoDB wire spellings in validation-error order.
    pub const VALID_WIRE_VALUES: [&'static str; 3] = ["INDEXES", "TOTAL", "NONE"];

    /// Parses an exact DynamoDB wire spelling.
    #[must_use]
    pub const fn parse(value: &str) -> Option<Self> {
        match value.as_bytes() {
            b"NONE" => Some(Self::None),
            b"TOTAL" => Some(Self::Total),
            b"INDEXES" => Some(Self::Indexes),
            _ => None,
        }
    }

    /// The exact DynamoDB wire spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Total => "TOTAL",
            Self::Indexes => "INDEXES",
        }
    }
}
