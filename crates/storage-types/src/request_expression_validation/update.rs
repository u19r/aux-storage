use super::{
    ExpressionKind, ExpressionValidationContext, SmallVec, disallowed_function_error,
    nested_update_function_error, split_top_level_args, validation_message,
};

pub(super) fn update_expression_grammar_error_for_sections(
    sections: &[UpdateExpressionSection<'_>],
) -> Option<String> {
    for keyword in ["SET", "REMOVE", "ADD", "DELETE"] {
        if sections
            .iter()
            .filter(|section| section.keyword == keyword)
            .count()
            > 1
        {
            return Some(format!(
                "Invalid UpdateExpression: The \"{keyword}\" section can only be used once in an \
                 update expression;"
            ));
        }
    }

    set_action_syntax_error(sections).or_else(|| add_delete_action_syntax_error(sections))
}

pub(super) fn function_context_error(expression: &str, kind: ExpressionKind) -> Option<String> {
    match kind {
        ExpressionKind::Condition => disallowed_function_error(
            expression,
            kind,
            &["if_not_exists", "list_append"],
            "a condition",
        ),
        ExpressionKind::Update => disallowed_function_error(
            expression,
            kind,
            &["contains", "begins_with", "attribute_type", "size"],
            "an update",
        )
        .or_else(|| nested_update_function_error(expression, kind)),
        _ => None,
    }
}

#[derive(Clone, Copy)]
pub(super) struct UpdateExpressionSection<'a> {
    pub(super) keyword: &'a str,
    pub(super) body: &'a str,
}

pub(super) fn update_expression_sections(
    expression: &str,
) -> SmallVec<[UpdateExpressionSection<'_>; 4]> {
    let mut sections = SmallVec::new();
    let mut current_keyword: Option<(&'static str, usize)> = None;
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;

    for (index, ch) in expression.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {}
        }
        if paren_depth != 0 || bracket_depth != 0 || !is_identifier_start(ch) {
            continue;
        }

        let Some(token) = identifier_at(expression, index) else {
            continue;
        };
        let Some(keyword) = update_section_keyword(token) else {
            continue;
        };
        if !is_top_level_update_keyword(expression, index, token.len()) {
            continue;
        }

        if let Some((keyword, body_start)) = current_keyword.take()
            && let Some(body) = expression.get(body_start..index)
        {
            sections.push(UpdateExpressionSection {
                keyword,
                body: body.trim(),
            });
        }
        current_keyword = Some((keyword, index + token.len()));
    }

    if let Some((keyword, body_start)) = current_keyword
        && let Some(body) = expression.get(body_start..)
    {
        sections.push(UpdateExpressionSection {
            keyword,
            body: body.trim(),
        });
    }

    sections
}

fn update_section_keyword(token: &str) -> Option<&'static str> {
    if token.eq_ignore_ascii_case("SET") {
        Some("SET")
    } else if token.eq_ignore_ascii_case("REMOVE") {
        Some("REMOVE")
    } else if token.eq_ignore_ascii_case("ADD") {
        Some("ADD")
    } else if token.eq_ignore_ascii_case("DELETE") {
        Some("DELETE")
    } else {
        None
    }
}

fn is_top_level_update_keyword(expression: &str, start: usize, len: usize) -> bool {
    let bytes = expression.as_bytes();
    let previous = start.checked_sub(1).and_then(|index| bytes.get(index));
    if matches!(
        previous,
        Some(b'#' | b':' | b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9')
    ) {
        return false;
    }
    let next = bytes.get(start + len);
    !matches!(next, Some(b'_' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9'))
}

fn identifier_at(expression: &str, start: usize) -> Option<&str> {
    let mut end = start;
    for (relative_index, ch) in expression.get(start..)?.char_indices() {
        if relative_index == 0 {
            if !is_identifier_start(ch) {
                return None;
            }
            end = start + ch.len_utf8();
            continue;
        }
        if !is_identifier_continue(ch) {
            break;
        }
        end = start + relative_index + ch.len_utf8();
    }
    expression.get(start..end)
}

fn is_identifier_start(ch: char) -> bool {
    ch.is_ascii_alphabetic() || ch == '_'
}

fn is_identifier_continue(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn add_delete_action_syntax_error(sections: &[UpdateExpressionSection<'_>]) -> Option<String> {
    for section in sections
        .iter()
        .filter(|section| matches!(section.keyword, "ADD" | "DELETE"))
    {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let mut parts = action.split_whitespace();
            let path = parts.next().unwrap_or_default();
            let Some(value) = parts.next() else {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"{path}\""
                ));
            };
            if !value.starts_with(':') {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"{value}\", near: \"{path} \
                     {value}\""
                ));
            }
        }
    }
    None
}

fn set_action_syntax_error(sections: &[UpdateExpressionSection<'_>]) -> Option<String> {
    for section in sections.iter().filter(|section| section.keyword == "SET") {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let Some((_, value)) = top_level_split_once(action, '=') else {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \"{action}\""
                ));
            };
            let value = value.trim_end();
            if let Some(operator) = trailing_arithmetic_operator(value) {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"<EOF>\", near: \
                     \"{operator}\""
                ));
            }
            if let Some((operator, near)) = unsupported_arithmetic_operator(value) {
                return Some(format!(
                    "Invalid UpdateExpression: Syntax error; token: \"{operator}\", near: \
                     \"{near}\""
                ));
            }
        }
    }
    None
}

fn unsupported_arithmetic_operator(value: &str) -> Option<(char, &str)> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;
    for ch in value.chars() {
        if in_string {
            escaped = ch == '\\' && !escaped;
            if ch == '"' && !escaped {
                in_string = false;
            }
            if ch != '\\' {
                escaped = false;
            }
            continue;
        }
        match ch {
            '"' => in_string = true,
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            '*' | '/' | '^' if paren_depth == 0 && bracket_depth == 0 => {
                return Some((ch, value.trim()));
            }
            _ => {}
        }
    }
    None
}

fn trailing_arithmetic_operator(value: &str) -> Option<char> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for ch in value.chars().rev() {
        match ch {
            ')' => paren_depth += 1,
            '(' => paren_depth = paren_depth.saturating_sub(1),
            ']' => bracket_depth += 1,
            '[' => bracket_depth = bracket_depth.saturating_sub(1),
            '+' | '-' if paren_depth == 0 && bracket_depth == 0 => return Some(ch),
            ch if ch.is_whitespace() => continue,
            _ => return None,
        }
    }
    None
}

pub(super) fn validate_update_expression_paths(
    context: &ExpressionValidationContext<'_>,
    names: Option<&crate::VecMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    if context.kind != ExpressionKind::Update {
        return Ok(());
    }

    let paths = update_expression_paths(
        context.update_sections.as_deref().unwrap_or_default(),
        names,
    );
    for (left_index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(left_index + 1) {
            if document_paths_overlap(&left.parts, &right.parts) {
                return Err(validation_message(
                    format!(
                        "Invalid UpdateExpression: Two document paths overlap with each other; \
                         must remove or rewrite one of these paths; path one: [{}], path two: [{}]",
                        left.parts.join(", "),
                        right.parts.join(", ")
                    ),
                    prefixed,
                ));
            }
        }
    }
    Ok(())
}

struct UpdatePath {
    parts: Vec<String>,
}

pub(super) fn validate_projection_expression_paths(
    expression: &str,
    kind: ExpressionKind,
    names: Option<&crate::VecMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    if kind != ExpressionKind::Projection {
        return Ok(());
    }

    let paths = split_top_level_args(expression)
        .into_iter()
        .filter_map(|path| document_path_parts(path.trim(), names))
        .collect::<Vec<_>>();
    for (left_index, left) in paths.iter().enumerate() {
        for right in paths.iter().skip(left_index + 1) {
            if document_paths_overlap(left, right) {
                return Err(validation_message(
                    format!(
                        "Invalid ProjectionExpression: Two document paths overlap with each \
                         other; must remove or rewrite one of these paths; path one: [{}], path \
                         two: [{}]",
                        left.join(", "),
                        right.join(", ")
                    ),
                    prefixed,
                ));
            }
        }
    }
    Ok(())
}

fn update_expression_paths(
    sections: &[UpdateExpressionSection<'_>],
    names: Option<&crate::VecMap<String, String>>,
) -> Vec<UpdatePath> {
    let mut paths = Vec::new();
    for section in sections {
        for action in split_top_level_args(section.body) {
            let action = action.trim();
            if action.is_empty() {
                continue;
            }
            let path = match section.keyword {
                "SET" => top_level_split_once(action, '=').map(|(path, _)| path.trim()),
                "REMOVE" => Some(action),
                "ADD" | "DELETE" => action.split_whitespace().next(),
                _ => None,
            };
            if let Some(path) = path
                && let Some(parts) = document_path_parts(path, names)
            {
                paths.push(UpdatePath { parts });
            }
        }
    }
    paths
}

fn top_level_split_once(input: &str, separator: char) -> Option<(&str, &str)> {
    let mut paren_depth = 0usize;
    let mut bracket_depth = 0usize;
    for (index, ch) in input.char_indices() {
        match ch {
            '(' => paren_depth += 1,
            ')' => paren_depth = paren_depth.saturating_sub(1),
            '[' => bracket_depth += 1,
            ']' => bracket_depth = bracket_depth.saturating_sub(1),
            _ => {}
        }
        if ch == separator && paren_depth == 0 && bracket_depth == 0 {
            return Some((input.get(..index)?, input.get(index + ch.len_utf8()..)?));
        }
    }
    None
}

fn document_path_parts(
    path: &str,
    names: Option<&crate::VecMap<String, String>>,
) -> Option<Vec<String>> {
    let mut parts = Vec::new();
    for part in path.split('.') {
        let part = part.trim();
        if part.is_empty() {
            return None;
        }
        let mut name_end = part.len();
        if let Some(index_start) = part.find('[') {
            name_end = index_start;
        }
        if name_end > 0 {
            let raw_name = part.get(..name_end)?;
            parts.push(resolve_expression_path_name(raw_name, names));
        }
        let mut rest = part.get(name_end..)?;
        while let Some(index_start) = rest.strip_prefix('[') {
            let end = index_start.find(']')?;
            parts.push(format!("[{}]", index_start.get(..end)?));
            rest = index_start.get(end + 1..)?;
        }
        if !rest.is_empty() {
            return None;
        }
    }
    Some(parts)
}

fn resolve_expression_path_name(
    name: &str,
    names: Option<&crate::VecMap<String, String>>,
) -> String {
    if name.starts_with('#')
        && let Some(resolved) = names.and_then(|names| names.get(name))
    {
        return resolved.clone();
    }
    name.to_string()
}

fn document_paths_overlap(left: &[String], right: &[String]) -> bool {
    let min_len = left.len().min(right.len());
    min_len > 0 && left[..min_len] == right[..min_len]
}
