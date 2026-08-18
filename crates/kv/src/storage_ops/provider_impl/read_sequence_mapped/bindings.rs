use storage_provider::ReadSequenceUnsupportedReason;
use storage_types::{
    GetItemRequest, QueryRequest, ReadSequenceInputCardinality, ReadSequenceNode,
    ReadSequenceOnMissing, ReadSequenceStringTemplatePart, ReadSequenceStringTemplateParts,
};

use super::MappedParentOperation;

pub(in crate::storage_ops::provider_impl) struct MappedInput<'a> {
    pub(in crate::storage_ops::provider_impl) name: &'a str,
    pub(in crate::storage_ops::provider_impl) attribute_name: &'a str,
    pub(in crate::storage_ops::provider_impl) item: MappedInputItem,
    pub(in crate::storage_ops::provider_impl) indexer: Option<u8>,
}

#[derive(Clone, Copy)]
pub(in crate::storage_ops::provider_impl) enum MappedInputItem {
    Each,
    First,
}

pub(in crate::storage_ops::provider_impl) struct MappedKeyBinding<'a> {
    pub(in crate::storage_ops::provider_impl) target_name: &'a str,
    pub(in crate::storage_ops::provider_impl) source_name: &'a str,
    pub(in crate::storage_ops::provider_impl) template: Option<&'a str>,
    pub(in crate::storage_ops::provider_impl) direct_input: Option<&'a str>,
    pub(in crate::storage_ops::provider_impl) literal: Option<&'a storage_types::AttributeValue>,
}

pub(super) struct MappedChildBinding<'a> {
    pub(super) inputs: Vec<MappedInput<'a>>,
    pub(super) keys: Vec<MappedKeyBinding<'a>>,
    pub(super) iterates: bool,
}

pub(super) fn mapped_child_binding<'a>(
    parent_query: &QueryRequest,
    parent_name: &str,
    child: &'a ReadSequenceNode,
    child_get: &'a GetItemRequest,
) -> Result<MappedChildBinding<'a>, ReadSequenceUnsupportedReason> {
    if parent_query.return_consumed_capacity.is_some()
        || child_get.return_consumed_capacity.is_some()
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    let inputs = mapped_inputs(child, parent_name, MappedParentOperation::Query)?;
    let keys = mapped_keys(child_get, &inputs)?;
    if inputs
        .iter()
        .any(|input| !keys.iter().any(|key| key_mentions_input(key, input.name)))
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    Ok(MappedChildBinding {
        inputs,
        keys,
        iterates: child.iterate.is_some(),
    })
}

pub(super) fn mapped_child_query_binding<'a>(
    parent_get: &GetItemRequest,
    parent_name: &str,
    child: &'a ReadSequenceNode,
    child_query: &'a QueryRequest,
) -> Result<MappedChildBinding<'a>, ReadSequenceUnsupportedReason> {
    if parent_get.return_consumed_capacity.is_some()
        || child_query.return_consumed_capacity.is_some()
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    if child.iterate.is_some() {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    if child_query.conditional_operator.is_some() || child_query.query_filter.is_some() {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    let inputs = mapped_inputs(child, parent_name, MappedParentOperation::Get)?;
    let keys = mapped_query_keys(child_query, &inputs)?;
    let key_value_name = query_hash_condition(child_query)
        .map(|(_, value_name, _)| value_name)
        .ok_or(ReadSequenceUnsupportedReason::OperationShape)?;
    if child_query
        .expression_attribute_values
        .as_ref()
        .is_some_and(|values| {
            values
                .iter()
                .any(|(name, value)| name != key_value_name && contains_dynamic_value(value))
        })
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    if inputs
        .iter()
        .any(|input| !keys.iter().any(|key| key_mentions_input(key, input.name)))
    {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    Ok(MappedChildBinding {
        inputs,
        keys,
        iterates: false,
    })
}

fn mapped_keys<'a>(
    child_get: &'a GetItemRequest,
    inputs: &[MappedInput<'a>],
) -> Result<Vec<MappedKeyBinding<'a>>, ReadSequenceUnsupportedReason> {
    child_get
        .key
        .iter()
        .map(|(target_name, value)| {
            if storage_types::read_sequence_input_literal_name(value).is_some() {
                return Err(ReadSequenceUnsupportedReason::OperationShape);
            }
            let template = storage_types::read_sequence_string_template_name(value);
            let direct_input = storage_types::read_sequence_input_marker_name(value);
            let literal = (template.is_none() && direct_input.is_none()).then_some(value);
            let source_name = direct_input
                .and_then(|name| inputs.iter().find(|input| input.name == name))
                .map_or(target_name, |input| input.attribute_name);
            Ok(MappedKeyBinding {
                target_name,
                source_name,
                template,
                direct_input,
                literal,
            })
        })
        .collect()
}

fn mapped_query_keys<'a>(
    child_query: &'a QueryRequest,
    inputs: &[MappedInput<'a>],
) -> Result<Vec<MappedKeyBinding<'a>>, ReadSequenceUnsupportedReason> {
    let Some((target_name, _, value)) = query_hash_condition(child_query) else {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    };
    let template = storage_types::read_sequence_string_template_name(value);
    if storage_types::read_sequence_input_literal_name(value).is_some() {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    let direct_input = storage_types::read_sequence_input_marker_name(value);
    let literal = (template.is_none() && direct_input.is_none()).then_some(value);
    let source_name = direct_input
        .and_then(|name| inputs.iter().find(|input| input.name == name))
        .map_or(target_name, |input| input.attribute_name);
    let key = MappedKeyBinding {
        target_name,
        source_name,
        template,
        direct_input,
        literal,
    };
    if key.template.is_some() {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    Ok(vec![key])
}

fn query_hash_condition(
    query: &QueryRequest,
) -> Option<(&str, &str, &storage_types::AttributeValue)> {
    let expression = query.key_condition_expression.trim();
    let (attribute, value_name) = expression.split_once('=')?;
    let attribute = attribute.trim();
    let attribute = if let Some(alias) = attribute.strip_prefix('#') {
        query
            .expression_attribute_names
            .as_ref()?
            .get(attribute)
            .or_else(|| {
                query
                    .expression_attribute_names
                    .as_ref()?
                    .get(&format!("#{alias}"))
            })?
            .as_str()
    } else {
        attribute
    };
    let value_name = value_name.trim();
    if !value_name.starts_with(':') {
        return None;
    }
    Some((
        attribute,
        value_name,
        query
            .expression_attribute_values
            .as_ref()?
            .get(value_name)?,
    ))
}

fn contains_dynamic_value(value: &storage_types::AttributeValue) -> bool {
    storage_types::read_sequence_input_marker_name(value).is_some()
        || storage_types::read_sequence_string_template_name(value).is_some()
        || storage_types::read_sequence_input_literal_name(value).is_some()
        || match value {
            storage_types::AttributeValue::L(values) => values.iter().any(contains_dynamic_value),
            storage_types::AttributeValue::M(values) => values.values().any(contains_dynamic_value),
            _ => false,
        }
}

fn mapped_inputs<'a>(
    child: &'a ReadSequenceNode,
    parent_name: &str,
    parent_operation: MappedParentOperation,
) -> Result<Vec<MappedInput<'a>>, ReadSequenceUnsupportedReason> {
    child
        .inputs()
        .iter()
        .map(|(name, input)| {
            if input.from.node != parent_name {
                return Err(ReadSequenceUnsupportedReason::OperationShape);
            }
            mapped_input(name, child.iterate.as_deref(), input, parent_operation)
        })
        .collect()
}

fn mapped_input<'a>(
    name: &'a str,
    iterate: Option<&str>,
    input: &'a storage_types::ReadSequenceNodeInput,
    parent_operation: MappedParentOperation,
) -> Result<MappedInput<'a>, ReadSequenceUnsupportedReason> {
    if matches!(parent_operation, MappedParentOperation::Get) {
        if iterate.is_some()
            || input.cardinality != ReadSequenceInputCardinality::One
            || input.on_missing == ReadSequenceOnMissing::Null
        {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        let attribute_name = input.from.select.0.strip_prefix("$.Get.Item.");
        let attribute_name = attribute_name
            .filter(|attribute| !attribute.is_empty() && !attribute.contains(['.', '[', ']']))
            .ok_or(ReadSequenceUnsupportedReason::OperationShape)?;
        let indexer = input.mapped_key_source.as_ref().map(|source| {
            (source.attribute_name() == attribute_name)
                .then_some(source.indexer())
                .ok_or(ReadSequenceUnsupportedReason::PhysicalLayout)
        });
        return Ok(MappedInput {
            name,
            attribute_name,
            item: MappedInputItem::First,
            indexer: indexer.transpose()?,
        });
    }
    let (item, attribute_name) = if iterate == Some(name) {
        if input.cardinality != ReadSequenceInputCardinality::Many
            || input.on_missing != ReadSequenceOnMissing::Skip
        {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        (
            MappedInputItem::Each,
            input.from.select.0.strip_prefix("$.Query.Items[*]."),
        )
    } else {
        if input.cardinality != ReadSequenceInputCardinality::One
            || input.on_missing == ReadSequenceOnMissing::Null
        {
            return Err(ReadSequenceUnsupportedReason::OperationShape);
        }
        (
            MappedInputItem::First,
            input.from.select.0.strip_prefix("$.Query.Items[0]."),
        )
    };
    let attribute_name = attribute_name.ok_or(ReadSequenceUnsupportedReason::OperationShape)?;
    if attribute_name.is_empty() || attribute_name.contains(['.', '[', ']']) {
        return Err(ReadSequenceUnsupportedReason::OperationShape);
    }
    let indexer = input.mapped_key_source.as_ref().map(|source| {
        (source.attribute_name() == attribute_name)
            .then_some(source.indexer())
            .ok_or(ReadSequenceUnsupportedReason::PhysicalLayout)
    });
    Ok(MappedInput {
        name,
        attribute_name,
        item,
        indexer: indexer.transpose()?,
    })
}

fn key_mentions_input(key: &MappedKeyBinding<'_>, name: &str) -> bool {
    if key.direct_input == Some(name) {
        return true;
    }
    key.template.is_some_and(|template| {
        ReadSequenceStringTemplateParts::new(template).any(|part| {
            matches!(part, Ok(ReadSequenceStringTemplatePart::Input(input)) if input == name)
        })
    })
}
