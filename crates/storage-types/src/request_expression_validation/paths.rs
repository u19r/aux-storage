use super::ExpressionKind;

pub(super) fn document_path_index_syntax_error(
    expression: &str,
    kind: ExpressionKind,
) -> Option<String> {
    let bytes = expression.as_bytes();
    let mut offset = 0usize;
    while let Some(relative_start) = expression.get(offset..)?.find('[') {
        let start = offset + relative_start;
        let after_start = start + 1;
        let first = bytes.get(after_start).copied();
        match first {
            Some(b'0'..=b'9') => {
                let mut end = after_start + 1;
                while matches!(bytes.get(end), Some(b'0'..=b'9')) {
                    end += 1;
                }
                match bytes.get(end).copied() {
                    Some(b']') => {
                        offset = end + 1;
                    }
                    Some(byte) => {
                        let token = char::from(byte).to_string();
                        return Some(document_path_syntax_message(
                            kind,
                            &token,
                            near_until_whitespace(expression, after_start),
                        ));
                    }
                    None => {
                        return Some(document_path_syntax_message(
                            kind,
                            "<EOF>",
                            expression.get(start..).unwrap_or_default(),
                        ));
                    }
                }
            }
            Some(b']') => {
                return Some(document_path_syntax_message(
                    kind,
                    "]",
                    empty_list_index_near(expression, start),
                ));
            }
            Some(b'-') => {
                return Some(document_path_syntax_message(
                    kind,
                    "-",
                    signed_list_index_near(expression, start),
                ));
            }
            Some(byte) if byte.is_ascii_alphabetic() || byte == b'_' => {
                let mut end = after_start + 1;
                while matches!(bytes.get(end), Some(byte) if byte.is_ascii_alphanumeric() || *byte == b'_')
                {
                    end += 1;
                }
                return Some(document_path_syntax_message(
                    kind,
                    expression.get(after_start..end).unwrap_or_default(),
                    expression
                        .get(start..end + usize::from(bytes.get(end) == Some(&b']')))
                        .unwrap_or_default(),
                ));
            }
            Some(byte) => {
                let token = char::from(byte).to_string();
                return Some(document_path_syntax_message(
                    kind,
                    &token,
                    expression.get(start..).unwrap_or_default(),
                ));
            }
            None => {
                return Some(document_path_syntax_message(kind, "<EOF>", "["));
            }
        }
    }
    None
}

fn empty_list_index_near(expression: &str, start: usize) -> &str {
    expression
        .get(start..)
        .and_then(|near| near.starts_with("[].").then_some("[]."))
        .or_else(|| expression.get(start..start.saturating_add(2)))
        .unwrap_or_default()
}

fn signed_list_index_near(expression: &str, start: usize) -> &str {
    let bytes = expression.as_bytes();
    let mut end = start + 2;
    while matches!(bytes.get(end), Some(b'0'..=b'9')) {
        end += 1;
    }
    expression.get(start..end).unwrap_or_default()
}

fn near_until_whitespace(expression: &str, start: usize) -> &str {
    let end = expression
        .get(start..)
        .and_then(|tail| {
            tail.char_indices()
                .find(|(_, ch)| ch.is_ascii_whitespace())
                .map(|(index, _)| start + index)
        })
        .unwrap_or(expression.len());
    expression.get(start..end).unwrap_or_default()
}

fn document_path_syntax_message(kind: ExpressionKind, token: &str, near: &str) -> String {
    format!("Invalid {kind}: Syntax error; token: \"{token}\", near: \"{near}\"")
}
