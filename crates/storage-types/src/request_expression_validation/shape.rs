use super::{
    ExpressionKind, ExpressionValidationContext, attribute_type_literal_operand_error,
    contains_same_operand_error, document_path_index_syntax_error, function_context_error,
    function_syntax_or_arity_error, reserved_word_in_expression, split_nonempty_top_level_args,
    update_expression_grammar_error_for_sections, validation_message,
};

pub(super) fn validate_expression_shape(
    context: &ExpressionValidationContext<'_>,
    names: Option<&crate::VecMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    let expression = context.expression;
    let kind = context.kind;
    if let Some(error) = invalid_function_name_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = document_path_index_syntax_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = function_context_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if kind == ExpressionKind::Update
        && let Some(error) = update_expression_grammar_error_for_sections(
            context.update_sections.as_deref().unwrap_or_default(),
        )
    {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = function_syntax_or_arity_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = contains_same_operand_error(expression, kind, names) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = attribute_type_literal_operand_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(error) = in_operand_count_error(expression, kind) {
        return Err(validation_message(error, prefixed));
    }
    if let Some(reserved_word) = reserved_word_in_expression(expression, kind) {
        return Err(validation_message(
            format!(
                "Invalid {kind}: Attribute name is a reserved keyword; reserved keyword: \
                 {reserved_word}"
            ),
            prefixed,
        ));
    }
    if kind == ExpressionKind::Condition && has_redundant_parentheses(expression) {
        return Err(validation_message(
            "Invalid ConditionExpression: The expression has redundant parentheses;".to_string(),
            prefixed,
        ));
    }
    if expression.trim_end().ends_with('=') {
        return Err(validation_message(
            format!("Invalid {kind}: Syntax error; token: \"<EOF>\", near: \"=\""),
            prefixed,
        ));
    }
    if expression.trim_end().ends_with('(') {
        return Err(validation_message(
            format!("Invalid {kind}: Syntax error; token: \"<EOF>\", near: \"(\""),
            prefixed,
        ));
    }
    Ok(())
}
fn invalid_function_name_error(expression: &str, kind: ExpressionKind) -> Option<String> {
    let allowed = match kind {
        ExpressionKind::Condition | ExpressionKind::Filter | ExpressionKind::KeyCondition => &[
            "attribute_exists",
            "attribute_not_exists",
            "attribute_type",
            "begins_with",
            "contains",
            "size",
        ][..],
        ExpressionKind::Update => &["if_not_exists", "list_append"][..],
        _ => return None,
    };

    for call in expression_function_names(expression) {
        if allowed
            .iter()
            .any(|function_name| call.eq_ignore_ascii_case(function_name) && call != *function_name)
        {
            return Some(format!(
                "Invalid {kind}: Invalid function name; function: {call}"
            ));
        }
    }
    None
}

pub(super) fn expression_function_names(expression: &str) -> impl Iterator<Item = &str> {
    let mut index = 0usize;
    std::iter::from_fn(move || {
        loop {
            let (relative_start, _) = expression
                .get(index..)?
                .char_indices()
                .find(|(_, ch)| ch.is_ascii_alphabetic() || *ch == '_')?;
            let start = index + relative_start;
            let end = expression
                .get(start..)?
                .char_indices()
                .find(|(_, ch)| !ch.is_ascii_alphanumeric() && *ch != '_')
                .map_or(expression.len(), |(offset, _)| start + offset);
            index = end;
            if expression.get(end..)?.trim_start().starts_with('(') {
                return expression.get(start..end);
            }
        }
    })
}

fn in_operand_count_error(expression: &str, kind: ExpressionKind) -> Option<String> {
    if !matches!(
        kind,
        ExpressionKind::Condition | ExpressionKind::Filter | ExpressionKind::KeyCondition
    ) {
        return None;
    }
    for operands in in_operand_lists(expression) {
        let count = split_nonempty_top_level_args(operands).len();
        if count > 100 {
            return Some(format!(
                "Invalid {kind}: The IN operator is provided with too many operands; number of \
                 operands: {count}"
            ));
        }
    }
    None
}

pub(super) fn in_operand_lists(expression: &str) -> impl Iterator<Item = &str> {
    let mut offset = 0usize;
    std::iter::from_fn(move || {
        let relative = expression
            .get(offset..)?
            .match_indices(' ')
            .find_map(|(index, _)| {
                expression
                    .as_bytes()
                    .get(offset + index..offset + index + 4)
                    .filter(|word| word.eq_ignore_ascii_case(b" IN "))
                    .map(|_| index)
            })?;
        let after_in = offset + relative + 4;
        let open = after_in + expression.get(after_in..)?.find('(')?;
        let args_start = open + 1;
        let mut depth = 1usize;
        let close = expression
            .as_bytes()
            .get(args_start..)?
            .iter()
            .position(|byte| {
                match byte {
                    b'(' => depth += 1,
                    b')' => depth -= 1,
                    _ => {}
                }
                depth == 0
            })?
            + args_start;
        offset = close + 1;
        expression.get(args_start..close)
    })
}

pub(super) fn validate_between_bounds(
    expression: &str,
    kind: ExpressionKind,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    if kind != ExpressionKind::Condition {
        return Ok(());
    }
    let Some(values) = values else {
        return Ok(());
    };

    for (lower_key, upper_key) in between_bound_value_keys(expression) {
        let (Some(lower), Some(upper)) = (values.get(lower_key), values.get(upper_key)) else {
            continue;
        };
        if between_upper_is_less_than_lower(lower, upper) {
            return Err(validation_message(
                format!(
                    "Invalid ConditionExpression: The BETWEEN operator requires upper bound to be \
                     greater than or equal to lower bound; lower bound operand: {}, upper bound \
                     operand: {}",
                    dynamodb_attribute_value_display(lower),
                    dynamodb_attribute_value_display(upper)
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn between_bound_value_keys(expression: &str) -> Vec<(&str, &str)> {
    let mut bounds = Vec::new();
    let mut index = 0;
    while let Some((between_index, between_len)) = next_word_matching(expression, index, "BETWEEN")
    {
        let Some((lower_key, after_lower)) =
            read_expression_value_key(expression, between_index + between_len)
        else {
            index = between_index + between_len;
            continue;
        };
        let Some((and_index, and_len)) = next_word_matching(expression, after_lower, "AND") else {
            index = after_lower;
            continue;
        };
        let Some((upper_key, after_upper)) =
            read_expression_value_key(expression, and_index + and_len)
        else {
            index = and_index + and_len;
            continue;
        };
        bounds.push((lower_key, upper_key));
        index = after_upper;
    }
    bounds
}

fn next_word_matching(expression: &str, start: usize, expected: &str) -> Option<(usize, usize)> {
    let mut search_start = start;
    while let Some(relative_index) = expression.get(search_start..)?.find(expected) {
        let index = search_start + relative_index;
        let end = index + expected.len();
        if expression_word_boundary(expression, index, end) {
            return Some((index, expected.len()));
        }
        search_start = end;
    }
    None
}

fn expression_word_boundary(expression: &str, start: usize, end: usize) -> bool {
    let before = start
        .checked_sub(1)
        .and_then(|index| expression.as_bytes().get(index));
    let after = expression.as_bytes().get(end);
    !matches!(before, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
        && !matches!(after, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
}

fn read_expression_value_key(expression: &str, start: usize) -> Option<(&str, usize)> {
    let bytes = expression.as_bytes();
    let mut index = start;
    while matches!(bytes.get(index), Some(b' ' | b'\t' | b'\n' | b'\r')) {
        index += 1;
    }
    if bytes.get(index) != Some(&b':') {
        return None;
    }
    let key_start = index;
    index += 1;
    while matches!(
        bytes.get(index),
        Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')
    ) {
        index += 1;
    }
    if index <= key_start + 1 {
        return None;
    }
    Some((expression.get(key_start..index)?, index))
}

fn between_upper_is_less_than_lower(
    lower: &crate::AttributeValue,
    upper: &crate::AttributeValue,
) -> bool {
    match (lower, upper) {
        (crate::AttributeValue::N(lower), crate::AttributeValue::N(upper)) => {
            crate::compare_dynamodb_numbers(upper, lower).is_ok_and(std::cmp::Ordering::is_lt)
        }
        (crate::AttributeValue::S(lower), crate::AttributeValue::S(upper)) => upper < lower,
        (crate::AttributeValue::B(lower), crate::AttributeValue::B(upper)) => {
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

fn dynamodb_attribute_value_display(value: &crate::AttributeValue) -> String {
    match value {
        crate::AttributeValue::S(value) => format!("AttributeValue: {{S:{value}}}"),
        crate::AttributeValue::N(value) => format!("AttributeValue: {{N:{value}}}"),
        crate::AttributeValue::B(value) => format!("AttributeValue: {{B:{value}}}"),
        crate::AttributeValue::BOOL(value) => format!("AttributeValue: {{BOOL:{value}}}"),
        crate::AttributeValue::NULL(value) => format!("AttributeValue: {{NULL:{value}}}"),
        crate::AttributeValue::SS(values) => format!("AttributeValue: {{SS:{values:?}}}"),
        crate::AttributeValue::NS(values) => format!("AttributeValue: {{NS:{values:?}}}"),
        crate::AttributeValue::BS(values) => format!("AttributeValue: {{BS:{values:?}}}"),
        crate::AttributeValue::L(values) => format!("AttributeValue: {{L:{values:?}}}"),
        crate::AttributeValue::M(values) => format!("AttributeValue: {{M:{values:?}}}"),
    }
}

fn has_redundant_parentheses(expression: &str) -> bool {
    let Some((start, end)) = trimmed_bounds(expression) else {
        return false;
    };
    let Some(expression) = expression.get(start..end) else {
        return false;
    };
    if fully_parenthesized(expression)
        && let Some(inner) = inner_parenthesized_expression(expression)
        && fully_parenthesized(inner.trim())
    {
        return true;
    }
    contains_double_parenthesized_segment(expression)
}

fn contains_double_parenthesized_segment(expression: &str) -> bool {
    let mut stack = Vec::new();
    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => stack.push(index),
            ')' => {
                let Some(start) = stack.pop() else {
                    continue;
                };
                let Some(inner) = expression.get(start + 1..index) else {
                    continue;
                };
                if fully_parenthesized(inner.trim()) {
                    return true;
                }
            }
            _ => {}
        }
    }
    false
}

fn fully_parenthesized(expression: &str) -> bool {
    let Some(inner) = inner_parenthesized_expression(expression) else {
        return false;
    };
    inner.len() + 2 == expression.len()
}

fn inner_parenthesized_expression(expression: &str) -> Option<&str> {
    if !expression.starts_with('(') || !expression.ends_with(')') {
        return None;
    }
    let mut depth = 0usize;
    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return (index == expression.len() - 1)
                        .then(|| expression.get(1..index))
                        .flatten();
                }
            }
            _ => {}
        }
    }
    None
}

fn trimmed_bounds(value: &str) -> Option<(usize, usize)> {
    let start = value
        .char_indices()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))?;
    let end = value
        .char_indices()
        .rev()
        .find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index + ch.len_utf8()))?;
    Some((start, end))
}
