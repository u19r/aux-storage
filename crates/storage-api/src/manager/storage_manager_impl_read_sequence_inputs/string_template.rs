use std::collections::BTreeMap;

use storage_types::{
    AttributeValue, ReadSequenceStringTemplatePart, ReadSequenceStringTemplateParts,
    ReadSequenceValidationError,
};

use super::ResolvedInput;

pub(in crate::manager) fn bind_string_template(
    template: &str,
    inputs: &BTreeMap<String, ResolvedInput>,
) -> Result<AttributeValue, ReadSequenceValidationError> {
    let output_len = ReadSequenceStringTemplateParts::new(template)
        .try_fold(0usize, |length, part| {
            Ok(length + part_value(part, inputs)?.len())
        })?;
    let mut output = String::with_capacity(output_len);
    for part in ReadSequenceStringTemplateParts::new(template) {
        output.push_str(part_value(part, inputs)?);
    }
    Ok(AttributeValue::S(output))
}

fn part_value<'a>(
    part: Result<
        ReadSequenceStringTemplatePart<'a>,
        storage_types::ReadSequenceStringTemplateError,
    >,
    inputs: &'a BTreeMap<String, ResolvedInput>,
) -> Result<&'a str, ReadSequenceValidationError> {
    let part = part.map_err(|_| ReadSequenceValidationError::InvalidStringTemplate {
        node: "<operation>".to_string(),
    })?;
    let name = match part {
        ReadSequenceStringTemplatePart::Literal(literal) => return Ok(literal),
        ReadSequenceStringTemplatePart::Input(name) => name,
    };
    let input = inputs
        .get(name)
        .ok_or_else(|| ReadSequenceValidationError::UnknownInput {
            node: "<operation>".to_string(),
            input: name.to_string(),
        })?;
    let AttributeValue::S(value) = &input.value else {
        return Err(ReadSequenceValidationError::InputType {
            node: "<operation>".to_string(),
            input: name.to_string(),
            expected: "S".to_string(),
            actual: attribute_value_type_name(&input.value).to_string(),
        });
    };
    Ok(value)
}

const fn attribute_value_type_name(value: &AttributeValue) -> &'static str {
    match value {
        AttributeValue::S(_) => "S",
        AttributeValue::N(_) => "N",
        AttributeValue::B(_) => "B",
        AttributeValue::SS(_) => "SS",
        AttributeValue::NS(_) => "NS",
        AttributeValue::BS(_) => "BS",
        AttributeValue::BOOL(_) => "BOOL",
        AttributeValue::NULL(_) => "NULL",
        AttributeValue::L(_) => "L",
        AttributeValue::M(_) => "M",
    }
}
