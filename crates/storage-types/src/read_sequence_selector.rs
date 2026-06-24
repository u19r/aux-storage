use std::collections::BTreeMap;

use crate::{AttributeMap, AttributeValue, ReadSequenceSelector, ReadSequenceValidationError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedReadSequenceSelector {
    raw: String,
    root: ReadSequenceSelectorRoot,
    segments: Vec<ReadSequenceSelectorSegment>,
}

impl ParsedReadSequenceSelector {
    pub fn parse(selector: &ReadSequenceSelector) -> Result<Self, ReadSequenceValidationError> {
        parse_selector(&selector.0)
    }

    pub fn depth(&self) -> u32 {
        self.segments.len() as u32
    }

    pub fn dependency_root(&self) -> Option<&str> {
        match &self.root {
            ReadSequenceSelectorRoot::CurrentItem => None,
            ReadSequenceSelectorRoot::Named(name) => Some(name.as_str()),
        }
    }

    pub fn evaluate_item(
        &self,
        item: &AttributeMap,
    ) -> Result<Option<AttributeValue>, ReadSequenceValidationError> {
        let mut current = AttributeValue::M(item.clone().into());
        for segment in &self.segments {
            current = match segment {
                ReadSequenceSelectorSegment::Attribute(name) => match current {
                    AttributeValue::M(mut values) => {
                        let Some(value) = values.remove(name) else {
                            return Ok(None);
                        };
                        value
                    }
                    other => {
                        return Err(ReadSequenceValidationError::SelectorTypeMismatch {
                            selector: self.raw.clone(),
                            expected: "map",
                            actual: attribute_value_type_name(&other),
                        });
                    }
                },
                ReadSequenceSelectorSegment::Index(index) => match current {
                    AttributeValue::L(values) => {
                        let Some(value) = values.get(*index).cloned() else {
                            return Ok(None);
                        };
                        value
                    }
                    other => {
                        return Err(ReadSequenceValidationError::SelectorTypeMismatch {
                            selector: self.raw.clone(),
                            expected: "list",
                            actual: attribute_value_type_name(&other),
                        });
                    }
                },
                ReadSequenceSelectorSegment::Wildcard => current,
                ReadSequenceSelectorSegment::AttributeValueType(value_type) => {
                    return scalar_for_type(&self.raw, current, *value_type).map(Some);
                }
            };
        }
        Ok(Some(current))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadSequenceSelectorRoot {
    CurrentItem,
    Named(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadSequenceSelectorSegment {
    Attribute(String),
    AttributeValueType(ReadSequenceAttributeValueType),
    Index(usize),
    Wildcard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadSequenceAttributeValueType {
    S,
    N,
    B,
    SS,
    NS,
    BS,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReadSequenceSelectedContext {
    values: BTreeMap<String, AttributeValue>,
}

impl ReadSequenceSelectedContext {
    pub fn insert(&mut self, name: impl Into<String>, value: AttributeValue) {
        self.values.insert(name.into(), value);
    }

    pub fn get(&self, name: &str) -> Option<&AttributeValue> {
        self.values.get(name)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub fn bind_read_sequence_attribute_value(
    value: &AttributeValue,
    context: &ReadSequenceSelectedContext,
) -> Result<AttributeValue, ReadSequenceValidationError> {
    match value {
        AttributeValue::S(value) => bind_scalar(value, context).map(AttributeValue::S),
        AttributeValue::N(value) => bind_scalar(value, context).map(AttributeValue::N),
        AttributeValue::B(value) => bind_scalar(value, context).map(AttributeValue::B),
        AttributeValue::SS(values) => bind_string_set(values, context, SetKind::String),
        AttributeValue::NS(values) => bind_string_set(values, context, SetKind::Number),
        AttributeValue::BS(values) => bind_string_set(values, context, SetKind::Binary),
        AttributeValue::BOOL(value) => Ok(AttributeValue::BOOL(*value)),
        AttributeValue::NULL(value) => Ok(AttributeValue::NULL(*value)),
        AttributeValue::L(values) => values
            .iter()
            .map(|value| bind_read_sequence_attribute_value(value, context))
            .collect::<Result<Vec<_>, _>>()
            .map(AttributeValue::L),
        AttributeValue::M(values) => values
            .iter()
            .map(|(name, value)| {
                bind_read_sequence_attribute_value(value, context)
                    .map(|bound| (name.clone(), bound))
            })
            .collect::<Result<std::collections::HashMap<_, _>, _>>()
            .map(AttributeValue::M),
    }
}

pub fn validate_selector_depth(
    selector: &ReadSequenceSelector,
    max_depth: u32,
) -> Result<ParsedReadSequenceSelector, ReadSequenceValidationError> {
    let parsed = ParsedReadSequenceSelector::parse(selector)?;
    if parsed.depth() > max_depth {
        Err(ReadSequenceValidationError::SelectorPathTooDeep {
            selector: selector.0.clone(),
            depth: parsed.depth(),
            limit: max_depth,
        })
    } else {
        Ok(parsed)
    }
}

fn parse_selector(raw: &str) -> Result<ParsedReadSequenceSelector, ReadSequenceValidationError> {
    if raw.trim().is_empty() {
        return Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.to_string(),
        });
    }
    let mut parts = raw.split('.').peekable();
    let Some(root_part) = parts.next() else {
        return Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.to_string(),
        });
    };
    let root = if root_part == "$" {
        ReadSequenceSelectorRoot::CurrentItem
    } else {
        ReadSequenceSelectorRoot::Named(root_part.to_string())
    };
    let mut segments = Vec::new();
    for part in parts {
        parse_selector_part(raw, part, &mut segments)?;
    }
    Ok(ParsedReadSequenceSelector {
        raw: raw.to_string(),
        root,
        segments,
    })
}

fn parse_selector_part(
    raw: &str,
    part: &str,
    segments: &mut Vec<ReadSequenceSelectorSegment>,
) -> Result<(), ReadSequenceValidationError> {
    if part.is_empty() {
        return Err(ReadSequenceValidationError::SelectorFailure {
            selector: raw.to_string(),
        });
    }
    if let Some((name, rest)) = part.split_once('[') {
        if !name.is_empty() {
            push_named_segment(name, segments);
        }
        let mut remainder = rest;
        loop {
            let Some((index, rest)) = remainder.split_once(']') else {
                return Err(ReadSequenceValidationError::SelectorFailure {
                    selector: raw.to_string(),
                });
            };
            if index == "*" {
                segments.push(ReadSequenceSelectorSegment::Wildcard);
            } else {
                let parsed = index.parse::<usize>().map_err(|_| {
                    ReadSequenceValidationError::SelectorFailure {
                        selector: raw.to_string(),
                    }
                })?;
                segments.push(ReadSequenceSelectorSegment::Index(parsed));
            }
            if rest.is_empty() {
                break;
            }
            let Some(next) = rest.strip_prefix('[') else {
                return Err(ReadSequenceValidationError::SelectorFailure {
                    selector: raw.to_string(),
                });
            };
            remainder = next;
        }
    } else {
        push_named_segment(part, segments);
    }
    Ok(())
}

fn push_named_segment(name: &str, segments: &mut Vec<ReadSequenceSelectorSegment>) {
    if let Some(value_type) = parse_attribute_value_type(name) {
        segments.push(ReadSequenceSelectorSegment::AttributeValueType(value_type));
    } else {
        segments.push(ReadSequenceSelectorSegment::Attribute(name.to_string()));
    }
}

fn parse_attribute_value_type(name: &str) -> Option<ReadSequenceAttributeValueType> {
    match name {
        "S" => Some(ReadSequenceAttributeValueType::S),
        "N" => Some(ReadSequenceAttributeValueType::N),
        "B" => Some(ReadSequenceAttributeValueType::B),
        "SS" => Some(ReadSequenceAttributeValueType::SS),
        "NS" => Some(ReadSequenceAttributeValueType::NS),
        "BS" => Some(ReadSequenceAttributeValueType::BS),
        _ => None,
    }
}

fn scalar_for_type(
    selector: &str,
    value: AttributeValue,
    value_type: ReadSequenceAttributeValueType,
) -> Result<AttributeValue, ReadSequenceValidationError> {
    match (value_type, value) {
        (ReadSequenceAttributeValueType::S, AttributeValue::S(value)) => {
            Ok(AttributeValue::S(value))
        }
        (ReadSequenceAttributeValueType::N, AttributeValue::N(value)) => {
            Ok(AttributeValue::N(value))
        }
        (ReadSequenceAttributeValueType::B, AttributeValue::B(value)) => {
            Ok(AttributeValue::B(value))
        }
        (ReadSequenceAttributeValueType::SS, AttributeValue::SS(value)) => {
            Ok(AttributeValue::SS(value))
        }
        (ReadSequenceAttributeValueType::NS, AttributeValue::NS(value)) => {
            Ok(AttributeValue::NS(value))
        }
        (ReadSequenceAttributeValueType::BS, AttributeValue::BS(value)) => {
            Ok(AttributeValue::BS(value))
        }
        (_, other) => Err(ReadSequenceValidationError::SelectorTypeMismatch {
            selector: selector.to_string(),
            expected: "matching AttributeValue type",
            actual: attribute_value_type_name(&other),
        }),
    }
}

fn attribute_value_type_name(value: &AttributeValue) -> &'static str {
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

#[derive(Debug, Clone, Copy)]
enum SetKind {
    String,
    Number,
    Binary,
}

fn bind_string_set(
    values: &[String],
    context: &ReadSequenceSelectedContext,
    kind: SetKind,
) -> Result<AttributeValue, ReadSequenceValidationError> {
    if values.len() == 1
        && let Some(name) = whole_template_name(&values[0])
        && let Some(value) = context.get(name)
    {
        return match (kind, value) {
            (SetKind::String, AttributeValue::SS(values)) => Ok(AttributeValue::SS(values.clone())),
            (SetKind::Number, AttributeValue::NS(values)) => Ok(AttributeValue::NS(values.clone())),
            (SetKind::Binary, AttributeValue::BS(values)) => Ok(AttributeValue::BS(values.clone())),
            (_, other) => Err(ReadSequenceValidationError::SelectorTypeMismatch {
                selector: name.to_string(),
                expected: "matching set AttributeValue type",
                actual: attribute_value_type_name(other),
            }),
        };
    }
    values
        .iter()
        .map(|value| bind_scalar(value, context))
        .collect::<Result<Vec<_>, _>>()
        .map(|values| match kind {
            SetKind::String => AttributeValue::SS(values),
            SetKind::Number => AttributeValue::NS(values),
            SetKind::Binary => AttributeValue::BS(values),
        })
}

fn bind_scalar(
    template: &str,
    context: &ReadSequenceSelectedContext,
) -> Result<String, ReadSequenceValidationError> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some((prefix, after_start)) = rest.split_once("${") {
        output.push_str(prefix);
        let Some((name, after_end)) = after_start.split_once('}') else {
            return Err(ReadSequenceValidationError::TemplateFailure {
                template: template.to_string(),
            });
        };
        let value =
            context
                .get(name)
                .ok_or_else(|| ReadSequenceValidationError::TemplateFailure {
                    template: template.to_string(),
                })?;
        output.push_str(scalar_value(name, value)?);
        rest = after_end;
    }
    output.push_str(rest);
    Ok(output)
}

fn scalar_value<'a>(
    name: &str,
    value: &'a AttributeValue,
) -> Result<&'a str, ReadSequenceValidationError> {
    match value {
        AttributeValue::S(value) | AttributeValue::N(value) | AttributeValue::B(value) => Ok(value),
        other => Err(ReadSequenceValidationError::SelectorTypeMismatch {
            selector: name.to_string(),
            expected: "S/N/B scalar",
            actual: attribute_value_type_name(other),
        }),
    }
}

fn whole_template_name(template: &str) -> Option<&str> {
    template
        .strip_prefix("${")
        .and_then(|rest| rest.strip_suffix('}'))
        .filter(|name| !name.is_empty() && !name.contains("${"))
}
