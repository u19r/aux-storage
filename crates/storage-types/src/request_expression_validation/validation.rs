use super::{
    ExpressionKind, SmallVec, UpdateExpressionSection,
    collect_expression_attribute_placeholder_refs, split_top_level_args,
    update_expression_sections, validate_attribute_type_value_operands,
    validate_begins_with_value_operands, validate_between_bounds, validate_expression_shape,
    validate_projection_expression_paths, validate_update_expression_paths,
};

pub(crate) fn validate_expression_set<'a, I>(
    expressions: I,
    names: Option<&crate::VecMap<String, String>>,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String>
where
    I: IntoIterator<Item = (Option<&'a str>, ExpressionKind)>,
{
    let expressions = expressions
        .into_iter()
        .filter_map(|(expression, kind)| expression.map(|expression| (expression, kind)))
        .map(ExpressionValidationContext::new)
        .collect::<SmallVec<[_; 2]>>();

    for context in &expressions {
        validate_expression_shape(context, names, prefixed)?;
        validate_expression_names(context, names, prefixed)?;
        validate_projection_expression_paths(context.expression, context.kind, names, prefixed)?;
        validate_update_expression_paths(context, names, prefixed)?;
        validate_expression_values(context, values, prefixed)?;
        validate_update_add_delete_value_types(context, values, prefixed)?;
    }

    validate_unused_expression_names(names, &expressions, prefixed)?;
    validate_unused_expression_values(values, &expressions, prefixed)
}

pub(super) struct ExpressionValidationContext<'a> {
    pub(super) expression: &'a str,
    pub(super) kind: ExpressionKind,
    used_names: SmallVec<[&'a str; 8]>,
    used_values: SmallVec<[&'a str; 8]>,
    pub(super) update_sections: Option<SmallVec<[UpdateExpressionSection<'a>; 4]>>,
}

impl<'a> ExpressionValidationContext<'a> {
    fn new((expression, kind): (&'a str, ExpressionKind)) -> Self {
        let mut used_names = SmallVec::new();
        let mut used_values = SmallVec::new();
        collect_expression_attribute_placeholder_refs(
            expression,
            &mut used_names,
            &mut used_values,
        );
        let update_sections =
            (kind == ExpressionKind::Update).then(|| update_expression_sections(expression));
        Self {
            expression,
            kind,
            used_names,
            used_values,
            update_sections,
        }
    }
}

fn validate_expression_names(
    context: &ExpressionValidationContext<'_>,
    names: Option<&crate::VecMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    for &name in &context.used_names {
        if names.is_none_or(|names| !names.contains_key(name)) {
            return Err(validation_message(
                format!(
                    "Invalid {}: An expression attribute name used in the document path is not \
                     defined; attribute name: {name}",
                    context.kind
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn validate_update_add_delete_value_types(
    context: &ExpressionValidationContext<'_>,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    if context.kind != ExpressionKind::Update {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };

    for section in context
        .update_sections
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter(|section| matches!(section.keyword, "ADD" | "DELETE"))
    {
        for action in split_top_level_args(section.body) {
            let mut parts = action.split_whitespace();
            let _path = parts.next();
            let Some(value_ref) = parts.next() else {
                continue;
            };
            let Some(value) = values.get(value_ref) else {
                continue;
            };
            if !update_action_value_type_allowed(section.keyword, value) {
                return Err(validation_message(
                    format!(
                        "Invalid UpdateExpression: Incorrect operand type for operator or \
                         function; operator: {}, operand type: {}, typeSet: \
                         ALLOWED_FOR_ADD_OPERAND",
                        section.keyword,
                        update_action_operand_type_name(value)
                    ),
                    prefixed,
                ));
            }
        }
    }
    Ok(())
}

fn update_action_value_type_allowed(keyword: &str, value: &crate::AttributeValue) -> bool {
    match keyword {
        "ADD" => matches!(
            value,
            crate::AttributeValue::N(_)
                | crate::AttributeValue::SS(_)
                | crate::AttributeValue::NS(_)
                | crate::AttributeValue::BS(_)
        ),
        "DELETE" => matches!(
            value,
            crate::AttributeValue::SS(_)
                | crate::AttributeValue::NS(_)
                | crate::AttributeValue::BS(_)
        ),
        _ => true,
    }
}

fn update_action_operand_type_name(value: &crate::AttributeValue) -> &'static str {
    match value {
        crate::AttributeValue::S(_) => "STRING",
        crate::AttributeValue::N(_) => "NUMBER",
        crate::AttributeValue::B(_) => "BINARY",
        crate::AttributeValue::SS(_) => "STRING_SET",
        crate::AttributeValue::NS(_) => "NUMBER_SET",
        crate::AttributeValue::BS(_) => "BINARY_SET",
        crate::AttributeValue::BOOL(_) => "BOOL",
        crate::AttributeValue::NULL(_) => "NULL",
        crate::AttributeValue::L(_) => "LIST",
        crate::AttributeValue::M(_) => "MAP",
    }
}

fn validate_expression_values(
    context: &ExpressionValidationContext<'_>,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    for &value in &context.used_values {
        if values.is_none_or(|values| !values.contains_key(value)) {
            return Err(validation_message(
                format!(
                    "Invalid {}: An expression attribute value used in expression is not defined; \
                     attribute value: {value}",
                    context.kind
                ),
                prefixed,
            ));
        }
    }
    validate_expression_value_operands(context.expression, context.kind, values, prefixed)
}

/// Input binding preserves expression structure but can change operand values.
pub(crate) fn validate_expression_value_operands(
    expression: &str,
    kind: ExpressionKind,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    validate_expression_attribute_value_payloads(kind, values, prefixed)?;
    validate_begins_with_value_operands(expression, kind, values, prefixed)?;
    validate_attribute_type_value_operands(expression, kind, values, prefixed)?;
    validate_between_bounds(expression, kind, values, prefixed)?;
    Ok(())
}

fn validate_expression_attribute_value_payloads(
    kind: ExpressionKind,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    if kind != ExpressionKind::Update {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };
    for value in values.values() {
        crate::validate_attribute_value_for_write(value, "ExpressionAttributeValues")
            .map_err(|message| validation_message(message, prefixed))?;
    }
    Ok(())
}

fn validate_unused_expression_names(
    names: Option<&crate::VecMap<String, String>>,
    expressions: &[ExpressionValidationContext<'_>],
    prefixed: bool,
) -> Result<(), String> {
    let Some(names) = names else {
        return Ok(());
    };
    let mut unused_names = names
        .keys()
        .filter(|key| {
            !expressions
                .iter()
                .any(|context| context.used_names.contains(&key.as_str()))
        })
        .cloned()
        .collect::<Vec<_>>();
    unused_names.sort();
    if unused_names.is_empty() {
        return Ok(());
    }
    Err(validation_message(
        format!(
            "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{}}}",
            unused_names.join(", ")
        ),
        prefixed,
    ))
}

fn validate_unused_expression_values(
    values: Option<&crate::AttributeMap>,
    expressions: &[ExpressionValidationContext<'_>],
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    let mut unused_values = values
        .keys()
        .filter(|key| {
            !expressions
                .iter()
                .any(|context| context.used_values.contains(key))
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    unused_values.sort();
    if unused_values.is_empty() {
        return Ok(());
    }
    Err(validation_message(
        format!(
            "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{}}}",
            unused_values.join(", ")
        ),
        prefixed,
    ))
}

pub(super) fn validation_message(message: String, prefixed: bool) -> String {
    if prefixed {
        format!("1 validation error detected: {message}")
    } else {
        message
    }
}
