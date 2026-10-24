use std::borrow::Cow;

use storage_types::AttributeValue;

// Helper to extract a scalar string representation from an AttributeValue
pub(crate) fn attribute_value_scalar_to_string(value: &AttributeValue) -> String {
    attribute_value_scalar(value).into_owned()
}

fn attribute_value_scalar(value: &AttributeValue) -> Cow<'_, str> {
    match value {
        AttributeValue::S(s) | AttributeValue::N(s) | AttributeValue::B(s) => Cow::Borrowed(s),
        AttributeValue::BOOL(b) => Cow::Owned(b.to_string()),
        AttributeValue::NULL(_) => Cow::Borrowed("null"),
        AttributeValue::SS(v) | AttributeValue::NS(v) | AttributeValue::BS(v) => {
            Cow::Owned(v.first().cloned().unwrap_or_default())
        }
        AttributeValue::L(v) => Cow::Owned(serde_json::to_string(v).unwrap_or_default()),
        AttributeValue::M(v) => Cow::Owned(serde_json::to_string(v).unwrap_or_default()),
    }
}

pub(crate) fn attribute_value_scalar_to_string_owned(value: AttributeValue) -> String {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => value,
        value => attribute_value_scalar(&value).into_owned(),
    }
}
