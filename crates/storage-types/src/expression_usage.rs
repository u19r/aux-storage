use std::collections::{HashMap, HashSet};

use crate::{AttributeValue, StorageError, StorageResult};

#[must_use]
pub fn extract_expression_attribute_placeholders(
    expression: &str,
) -> (HashSet<String>, HashSet<String>) {
    let mut names = HashSet::new();
    let mut values = HashSet::new();
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = expression.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if in_single_quote {
            if ch == '\\' {
                let _ = chars.next();
                continue;
            }
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        if in_double_quote {
            if ch == '\\' {
                let _ = chars.next();
                continue;
            }
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }

        if ch == '\'' {
            in_single_quote = true;
            continue;
        }
        if ch == '"' {
            in_double_quote = true;
            continue;
        }

        if ch != '#' && ch != ':' {
            continue;
        }

        let mut end = idx + ch.len_utf8();
        let mut has_identifier = false;
        while let Some((next_idx, next_ch)) = chars.peek().copied() {
            if next_ch.is_ascii_alphanumeric() || next_ch == '_' {
                has_identifier = true;
                end = next_idx + next_ch.len_utf8();
                let _ = chars.next();
            } else {
                break;
            }
        }

        if !has_identifier {
            continue;
        }

        let token = expression.get(idx..end).unwrap_or_default().to_string();
        if ch == '#' {
            let _ = names.insert(token);
        } else {
            let _ = values.insert(token);
        }
    }

    (names, values)
}

#[expect(clippy::implicit_hasher)]
pub fn validate_expression_attribute_usage<'a, I>(
    expression_attribute_names: Option<&HashMap<String, String>>,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
    expressions: I,
) -> StorageResult<()>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut used_names = Vec::new();
    let mut used_values = Vec::new();

    for expression in expressions {
        collect_expression_attribute_placeholders(expression, &mut used_names, &mut used_values);
    }

    if let Some(values) = expression_attribute_values {
        let unused_values = collect_unused_expression_attribute_keys(values, &used_values);
        if !unused_values.is_empty() {
            return Err(StorageError::validation(format!(
                "Value provided in ExpressionAttributeValues unused in expressions: keys: {{{}}}",
                unused_values.join(", ")
            )));
        }
    }

    if let Some(names) = expression_attribute_names {
        let unused_names = collect_unused_expression_attribute_keys(names, &used_names);
        if !unused_names.is_empty() {
            return Err(StorageError::validation(format!(
                "Value provided in ExpressionAttributeNames unused in expressions: keys: {{{}}}",
                unused_names.join(", ")
            )));
        }
    }

    Ok(())
}

#[expect(clippy::implicit_hasher)]
#[must_use]
pub fn subset_expression_attribute_names_for_expression(
    expression: &str,
    expression_attribute_names: Option<&HashMap<String, String>>,
) -> Option<HashMap<String, String>> {
    let names = expression_attribute_names?;
    let (used_names, _) = extract_expression_attribute_placeholders(expression);
    if used_names.is_empty() {
        return None;
    }

    let filtered = names
        .iter()
        .filter(|(key, _)| used_names.contains(*key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered)
    }
}

#[expect(clippy::implicit_hasher)]
#[must_use]
pub fn subset_expression_attribute_values_for_expression(
    expression: &str,
    expression_attribute_values: Option<&HashMap<String, AttributeValue>>,
) -> Option<HashMap<String, AttributeValue>> {
    let values = expression_attribute_values?;
    let (_, used_values) = extract_expression_attribute_placeholders(expression);
    if used_values.is_empty() {
        return None;
    }

    let filtered = values
        .iter()
        .filter(|(key, _)| used_values.contains(*key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    if filtered.is_empty() {
        None
    } else {
        Some(filtered)
    }
}

fn collect_unused_expression_attribute_keys<T>(
    provided: &HashMap<String, T>,
    used: &[&str],
) -> Vec<String> {
    let mut unused = provided
        .keys()
        .filter(|key| !used.contains(&key.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    unused.sort();
    unused
}

fn collect_expression_attribute_placeholders<'a>(
    expression: &'a str,
    names: &mut Vec<&'a str>,
    values: &mut Vec<&'a str>,
) {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut chars = expression.char_indices().peekable();

    while let Some((idx, ch)) = chars.next() {
        if in_single_quote {
            if ch == '\\' {
                let _ = chars.next();
                continue;
            }
            if ch == '\'' {
                in_single_quote = false;
            }
            continue;
        }
        if in_double_quote {
            if ch == '\\' {
                let _ = chars.next();
                continue;
            }
            if ch == '"' {
                in_double_quote = false;
            }
            continue;
        }

        if ch == '\'' {
            in_single_quote = true;
            continue;
        }
        if ch == '"' {
            in_double_quote = true;
            continue;
        }

        if ch != '#' && ch != ':' {
            continue;
        }

        let mut end = idx + ch.len_utf8();
        let mut has_identifier = false;
        while let Some((next_idx, next_ch)) = chars.peek().copied() {
            if next_ch.is_ascii_alphanumeric() || next_ch == '_' {
                has_identifier = true;
                end = next_idx + next_ch.len_utf8();
                let _ = chars.next();
            } else {
                break;
            }
        }

        if !has_identifier {
            continue;
        }

        let token = expression.get(idx..end).unwrap_or_default();
        if ch == '#' {
            if !names.contains(&token) {
                names.push(token);
            }
        } else if !values.contains(&token) {
            values.push(token);
        }
    }
}
