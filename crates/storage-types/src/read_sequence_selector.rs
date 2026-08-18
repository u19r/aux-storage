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

    #[must_use]
    pub fn segments(&self) -> &[ReadSequenceSelectorSegment] {
        &self.segments
    }

    pub fn evaluate_item(
        &self,
        item: &AttributeMap,
    ) -> Result<Option<AttributeValue>, ReadSequenceValidationError> {
        let mut first = None;
        self.for_each_item_value(item, |value| {
            if first.is_none() {
                first = Some(value);
            }
        })?;
        Ok(first)
    }

    pub fn evaluate_item_values(
        &self,
        item: &AttributeMap,
    ) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
        let mut values = Vec::new();
        self.for_each_item_value(item, |value| values.push(value))?;
        Ok(values)
    }

    pub fn for_each_item_value(
        &self,
        item: &AttributeMap,
        mut visit: impl FnMut(AttributeValue),
    ) -> Result<(), ReadSequenceValidationError> {
        let Some((first, remaining)) = self.segments.split_first() else {
            visit(AttributeValue::M(item.clone().into()));
            return Ok(());
        };
        let ReadSequenceSelectorSegment::Attribute(name) = first else {
            for value in self.evaluate_values(&AttributeValue::M(item.clone().into()))? {
                visit(value);
            }
            return Ok(());
        };
        let Some(value) = item.get(name).cloned() else {
            return Ok(());
        };
        if remaining.is_empty() {
            visit(value);
            return Ok(());
        }
        let mut values = vec![value];
        for segment in remaining {
            values = evaluate_selector_segment(&self.raw, values, segment)?;
        }
        for value in values {
            visit(value);
        }
        Ok(())
    }

    pub fn evaluate_values(
        &self,
        root: &AttributeValue,
    ) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
        let mut values = vec![root.clone()];
        for segment in &self.segments {
            values = evaluate_selector_segment(&self.raw, values, segment)?;
        }
        Ok(values)
    }
}

fn evaluate_selector_segment(
    selector: &str,
    values: Vec<AttributeValue>,
    segment: &ReadSequenceSelectorSegment,
) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
    values
        .into_iter()
        .try_fold(Vec::new(), |mut next, current| {
            next.extend(evaluate_selector_value(selector, current, segment)?);
            Ok(next)
        })
}

fn evaluate_selector_value(
    selector: &str,
    current: AttributeValue,
    segment: &ReadSequenceSelectorSegment,
) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
    match segment {
        ReadSequenceSelectorSegment::Attribute(name) => evaluate_attribute(selector, current, name),
        ReadSequenceSelectorSegment::Index(index) => evaluate_index(selector, current, *index),
        ReadSequenceSelectorSegment::Wildcard => evaluate_wildcard(selector, current),
        ReadSequenceSelectorSegment::AttributeValueType(value_type) => {
            scalar_for_type(selector, current, *value_type).map(|value| vec![value])
        }
    }
}

fn evaluate_attribute(
    selector: &str,
    current: AttributeValue,
    name: &str,
) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
    match current {
        AttributeValue::M(values) => Ok(values.get(name).cloned().into_iter().collect()),
        other => Err(selector_type_mismatch(selector, "map", &other)),
    }
}

fn evaluate_index(
    selector: &str,
    current: AttributeValue,
    index: usize,
) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
    match current {
        AttributeValue::L(values) => Ok(values.get(index).cloned().into_iter().collect()),
        other => Err(selector_type_mismatch(selector, "list", &other)),
    }
}

fn evaluate_wildcard(
    selector: &str,
    current: AttributeValue,
) -> Result<Vec<AttributeValue>, ReadSequenceValidationError> {
    match current {
        AttributeValue::L(values) => Ok(values),
        AttributeValue::SS(values) => Ok(values.into_iter().map(AttributeValue::S).collect()),
        AttributeValue::NS(values) => Ok(values.into_iter().map(AttributeValue::N).collect()),
        AttributeValue::BS(values) => Ok(values.into_iter().map(AttributeValue::B).collect()),
        other => Err(selector_type_mismatch(selector, "list", &other)),
    }
}

fn selector_type_mismatch(
    selector: &str,
    expected: &'static str,
    actual: &AttributeValue,
) -> ReadSequenceValidationError {
    ReadSequenceValidationError::SelectorTypeMismatch {
        selector: selector.to_string(),
        expected,
        actual: attribute_value_type_name(actual),
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

pub fn validate_selector_depth(
    selector: &ReadSequenceSelector,
    max_depth: u32,
) -> Result<ParsedReadSequenceSelector, ReadSequenceValidationError> {
    let parsed = ParsedReadSequenceSelector::parse(selector)?;
    if parsed.dependency_root().is_some() {
        return Err(ReadSequenceValidationError::SelectorFailure {
            selector: selector.0.clone(),
        });
    }
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
    let Some((name, remainder)) = part.split_once('[') else {
        push_named_segment(part, segments);
        return Ok(());
    };
    if !name.is_empty() {
        push_named_segment(name, segments);
    }
    parse_selector_indexes(raw, remainder, segments)
}

fn parse_selector_indexes(
    raw: &str,
    mut remainder: &str,
    segments: &mut Vec<ReadSequenceSelectorSegment>,
) -> Result<(), ReadSequenceValidationError> {
    loop {
        let Some((index, rest)) = remainder.split_once(']') else {
            return Err(ReadSequenceValidationError::SelectorFailure {
                selector: raw.to_string(),
            });
        };
        segments.push(parse_selector_index(raw, index)?);
        if rest.is_empty() {
            return Ok(());
        }
        remainder =
            rest.strip_prefix('[')
                .ok_or_else(|| ReadSequenceValidationError::SelectorFailure {
                    selector: raw.to_string(),
                })?;
    }
}

fn parse_selector_index(
    raw: &str,
    index: &str,
) -> Result<ReadSequenceSelectorSegment, ReadSequenceValidationError> {
    if index == "*" {
        Ok(ReadSequenceSelectorSegment::Wildcard)
    } else {
        index
            .parse::<usize>()
            .map(ReadSequenceSelectorSegment::Index)
            .map_err(|_| ReadSequenceValidationError::SelectorFailure {
                selector: raw.to_string(),
            })
    }
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
