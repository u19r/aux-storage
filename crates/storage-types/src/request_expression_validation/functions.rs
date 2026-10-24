use super::{ExpressionKind, validation_message};

pub(super) fn disallowed_function_error(
    expression: &str,
    kind: ExpressionKind,
    function_names: &[&str],
    expression_context: &str,
) -> Option<String> {
    function_names
        .iter()
        .find(|function_name| !function_calls(expression, function_name).is_empty())
        .map(|function_name| {
            format!(
                "Invalid {kind}: The function is not allowed in {expression_context} expression; \
                 function: {function_name}"
            )
        })
}
pub(super) fn nested_update_function_error(
    expression: &str,
    kind: ExpressionKind,
) -> Option<String> {
    for call in function_calls(expression, "if_not_exists") {
        let args = split_nonempty_top_level_args(call.args);
        if let Some(first_arg) = args.first()
            && nested_update_function_name(first_arg).is_some()
        {
            return Some(format!(
                "Invalid {kind}: Operator or function requires a document path; operator or \
                 function: if_not_exists"
            ));
        }
    }

    for call in function_calls(expression, "list_append") {
        let args = split_nonempty_top_level_args(call.args);
        if args
            .iter()
            .any(|arg| nested_update_function_name(arg) == Some("list_append"))
        {
            return Some(format!(
                "Invalid {kind}: The function is not allowed to be used this way in an \
                 expression; function: list_append"
            ));
        }
    }

    None
}

fn nested_update_function_name(argument: &str) -> Option<&str> {
    let argument = argument.trim_start();
    ["if_not_exists", "list_append"]
        .into_iter()
        .find(|function_name| {
            argument
                .get(function_name.len()..)
                .is_some_and(|tail| argument.starts_with(function_name) && tail.starts_with('('))
        })
}

pub(super) fn function_syntax_or_arity_error(
    expression: &str,
    kind: ExpressionKind,
) -> Option<String> {
    for function_name in ["contains", "begins_with", "attribute_type", "size"] {
        for call in function_calls(expression, function_name) {
            let args = split_nonempty_top_level_args(call.args);
            if args.is_empty() {
                return Some(format!(
                    "Invalid {kind}: Syntax error; token: \")\", near: \"{}\"",
                    empty_function_near(expression, call.open_paren, call.close_paren)
                ));
            }
            let expected = if function_name == "size" { 1 } else { 2 };
            if args.len() != expected {
                return Some(format!(
                    "Invalid {kind}: Incorrect number of operands for operator or function; \
                     operator or function: {function_name}, number of operands: {}",
                    args.len()
                ));
            }
            if function_name == "size" {
                if let Some(nested_function) = nested_function_name(args[0]) {
                    return Some(format!(
                        "Invalid {kind}: The function is not allowed to be used this way in an \
                         expression; function: {nested_function}"
                    ));
                }
                if !size_function_has_comparison(expression, call.close_paren) {
                    return Some(format!(
                        "Invalid {kind}: The function is not allowed to be used this way in an \
                         expression; function: size"
                    ));
                }
            }
        }
    }
    None
}

#[derive(Clone, Copy)]
struct FunctionCall<'a> {
    args: &'a str,
    open_paren: usize,
    close_paren: usize,
}

fn function_calls<'a>(expression: &'a str, function_name: &str) -> Vec<FunctionCall<'a>> {
    let mut calls = Vec::new();
    let mut offset = 0usize;
    let needle = format!("{function_name}(");
    let lower_expression = expression.to_ascii_lowercase();
    while let Some(relative_start) = lower_expression
        .get(offset..)
        .and_then(|tail| tail.find(&needle))
    {
        let open_paren = offset + relative_start + function_name.len();
        let args_start = open_paren + 1;
        let mut depth = 1usize;
        let mut found_close = false;
        let Some(args_tail) = expression.get(args_start..) else {
            break;
        };
        for (relative_index, ch) in args_tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let close_paren = args_start + relative_index;
                        if let Some(args) = expression.get(args_start..close_paren) {
                            calls.push(FunctionCall {
                                args,
                                open_paren,
                                close_paren,
                            });
                        }
                        offset = close_paren + 1;
                        found_close = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !found_close {
            break;
        }
    }
    calls
}

fn empty_function_near(expression: &str, open_paren: usize, close_paren: usize) -> String {
    let mut near = expression
        .get(open_paren..=close_paren)
        .unwrap_or("()")
        .to_string();
    let tail = expression.get(close_paren + 1..).unwrap_or_default();
    let tail = tail.trim_start();
    if let Some(next) = tail.chars().next()
        && matches!(next, '=' | '<' | '>')
    {
        near.push(' ');
        near.push(next);
    }
    near
}

pub(super) fn split_nonempty_top_level_args(args: &str) -> Vec<&str> {
    split_top_level_args(args)
        .into_iter()
        .map(str::trim)
        .filter(|arg| !arg.is_empty())
        .collect()
}

fn nested_function_name(argument: &str) -> Option<&str> {
    let argument = argument.trim_start();
    ["contains", "begins_with", "attribute_type", "size"]
        .into_iter()
        .find(|function_name| {
            argument
                .get(function_name.len()..)
                .is_some_and(|tail| argument.starts_with(function_name) && tail.starts_with('('))
        })
}

fn size_function_has_comparison(expression: &str, close_paren: usize) -> bool {
    let tail = expression
        .get(close_paren + 1..)
        .unwrap_or_default()
        .trim_start();
    tail.starts_with('=')
        || tail.starts_with("<>")
        || tail.starts_with("<=")
        || tail.starts_with(">=")
        || tail.starts_with('<')
        || tail.starts_with('>')
        || tail
            .get(..7)
            .is_some_and(|token| token.eq_ignore_ascii_case("BETWEEN"))
}

pub(super) fn contains_same_operand_error(
    expression: &str,
    kind: ExpressionKind,
    names: Option<&crate::VecMap<String, String>>,
) -> Option<String> {
    for args in function_argument_lists(expression, "contains") {
        let parts = split_top_level_args(args);
        if parts.len() == 2 && parts[0].trim() == parts[1].trim() {
            let operand = resolve_expression_path_for_message(parts[0].trim(), names);
            return Some(format!(
                "Invalid {kind}: The first operand must be distinct from the remaining operands \
                 for this operator or function; operator: contains, first operand: [{operand}]"
            ));
        }
    }
    None
}

fn resolve_expression_path_for_message(
    path: &str,
    names: Option<&crate::VecMap<String, String>>,
) -> String {
    let Some(names) = names else {
        return path.to_string();
    };
    names.get(path).cloned().unwrap_or_else(|| path.to_string())
}

pub(super) fn attribute_type_literal_operand_error(
    expression: &str,
    kind: ExpressionKind,
) -> Option<String> {
    for args in function_argument_lists(expression, "attribute_type") {
        let parts = split_top_level_args(args);
        let Some(attribute_type_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !attribute_type_operand.starts_with(':') {
            return Some(attribute_type_operand_error(kind));
        }
    }
    None
}

pub(super) fn validate_attribute_type_value_operands(
    expression: &str,
    kind: ExpressionKind,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    for args in function_argument_lists(expression, "attribute_type") {
        let parts = split_top_level_args(args);
        let Some(attribute_type_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !attribute_type_operand.starts_with(':') {
            continue;
        }
        let Some(value) = values.get(attribute_type_operand) else {
            continue;
        };
        let crate::AttributeValue::S(attribute_type) = value else {
            return Err(validation_message(
                attribute_type_operand_error(kind),
                prefixed,
            ));
        };
        if !matches!(
            attribute_type.as_str(),
            "S" | "SS" | "N" | "NS" | "B" | "BS" | "BOOL" | "NULL" | "L" | "M"
        ) {
            let valid_types = if prefixed {
                "{S,SS,N,NS,B,BS,BOOL,NULL,L,M}"
            } else {
                "{N,BS,L,B,NULL,M,S,SS,NS,BOOL}"
            };
            return Err(validation_message(
                format!(
                    "Invalid {kind}: Invalid attribute type name found; type: {attribute_type}, \
                     valid types: {valid_types}"
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

fn attribute_type_operand_error(kind: ExpressionKind) -> String {
    format!(
        "Invalid {kind}: Incorrect operand type for operator or function; operator or function: \
         attribute_type, operand type: \
         {{FS,NULL,DICT,DOUBLESET,M,DOUBLE,SS,DECIMALSET,S,INT,HDS,FLOAT,B,INTSET,L,DECIMAL,N,NS,\
         BOOL,BS,HD}}"
    )
}

pub(super) fn validate_begins_with_value_operands(
    expression: &str,
    kind: ExpressionKind,
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    for args in function_argument_lists(expression, "begins_with") {
        let parts = split_top_level_args(args);
        let Some(prefix_operand) = parts.get(1).map(|part| part.trim()) else {
            continue;
        };
        if !prefix_operand.starts_with(':') {
            continue;
        }
        let Some(value) = values.get(prefix_operand) else {
            continue;
        };
        if !matches!(
            value,
            crate::AttributeValue::S(_) | crate::AttributeValue::B(_)
        ) {
            return Err(validation_message(
                format!(
                    "Invalid {kind}: Incorrect operand type for operator or function; operator or \
                     function: begins_with, operand type: {}",
                    value.variant_name()
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

pub(super) fn function_argument_lists<'a>(
    expression: &'a str,
    function_name: &'a str,
) -> impl Iterator<Item = &'a str> {
    let mut offset = 0;
    std::iter::from_fn(move || {
        let relative_start = find_function_start(expression.get(offset..)?, function_name)?;
        let start = offset + relative_start + function_name.len() + 1;
        let length = find_argument_end(expression.get(start..)?)?;
        offset = start + length + 1;
        expression.get(start..start + length)
    })
}

fn find_function_start(expression: &str, function_name: &str) -> Option<usize> {
    expression.match_indices('(').find_map(|(opening, _)| {
        let start = opening.checked_sub(function_name.len())?;
        expression
            .get(start..opening)?
            .eq_ignore_ascii_case(function_name)
            .then_some(start)
    })
}

fn find_argument_end(arguments: &str) -> Option<usize> {
    let mut depth = 1usize;
    for (index, byte) in arguments.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn split_top_level_args(args: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for (index, ch) in args.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            ',' if paren_depth == 0 && bracket_depth == 0 => {
                if let Some(part) = args.get(start..index) {
                    parts.push(part);
                }
                start = index + 1;
            }
            _ => {}
        }
    }
    if let Some(part) = args.get(start..) {
        parts.push(part);
    }
    parts
}
