use std::collections::BTreeMap;

use super::inputs::ResolvedInput;
use crate::{
    AttributeValue, ReadSequenceStringTemplatePart, ReadSequenceStringTemplateParts,
    ReadSequenceValidationError,
};

pub fn bind_string_template(
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
            actual: input.value.variant_name().to_string(),
        });
    };
    Ok(value)
}
