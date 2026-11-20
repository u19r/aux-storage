use std::borrow::Cow;

use crate::AttributeValue;

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

pub(crate) fn between_upper_is_less_than_lower(
    lower: &AttributeValue,
    upper: &AttributeValue,
) -> bool {
    match (lower, upper) {
        (AttributeValue::N(lower), AttributeValue::N(upper)) => {
            crate::compare_dynamodb_numbers(upper, lower).is_ok_and(std::cmp::Ordering::is_lt)
        }
        (AttributeValue::S(lower), AttributeValue::S(upper)) => upper < lower,
        (AttributeValue::B(lower), AttributeValue::B(upper)) => {
            match (
                crate::dynamodb_binary::decode_base64_string(lower),
                crate::dynamodb_binary::decode_base64_string(upper),
            ) {
                (Ok(lower), Ok(upper)) => upper < lower,
                _ => upper < lower,
            }
        }
        _ => false,
    }
}

pub(crate) fn dynamodb_attribute_value_display(value: &AttributeValue) -> String {
    match value {
        AttributeValue::S(value) => format!("AttributeValue: {{S:{value}}}"),
        AttributeValue::N(value) => format!("AttributeValue: {{N:{value}}}"),
        AttributeValue::B(value) => format!("AttributeValue: {{B:{value}}}"),
        AttributeValue::BOOL(value) => format!("AttributeValue: {{BOOL:{value}}}"),
        AttributeValue::NULL(value) => format!("AttributeValue: {{NULL:{value}}}"),
        AttributeValue::SS(values) => format!("AttributeValue: {{SS:{values:?}}}"),
        AttributeValue::NS(values) => format!("AttributeValue: {{NS:{values:?}}}"),
        AttributeValue::BS(values) => format!("AttributeValue: {{BS:{values:?}}}"),
        // BETWEEN only reports ordered scalar bounds, so documents never reach
        // this message.
        AttributeValue::L(values) => format!("AttributeValue: {{L:<{} elements>}}", values.len()),
        AttributeValue::M(values) => format!("AttributeValue: {{M:<{} entries>}}", values.len()),
    }
}
