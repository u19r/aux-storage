use storage_types::AttributeValue;

pub(crate) fn str_to_f64(s: &str) -> f64 {
    s.parse::<f64>().unwrap_or(f64::NAN)
}

// Helper to extract a scalar string representation from an AttributeValue
pub(crate) fn attribute_value_scalar_to_string(value: &AttributeValue) -> String {
    match value {
        AttributeValue::S(s) => s.clone(),
        AttributeValue::N(n) => n.clone(),
        AttributeValue::B(b) => b.clone(),
        AttributeValue::BOOL(b) => b.to_string(),
        AttributeValue::NULL(_) => "null".to_string(),
        AttributeValue::SS(v) | AttributeValue::NS(v) | AttributeValue::BS(v) => {
            v.first().cloned().unwrap_or_default()
        }
        AttributeValue::L(v) => serde_json::to_string(v).unwrap_or_default(),
        AttributeValue::M(v) => serde_json::to_string(v).unwrap_or_default(),
    }
}
