use super::{
    DYNAMODB_RESERVED_WORDS, ExpressionKind, Hash, HashSet, Hasher, LazyLock,
    MAX_EXPRESSION_SUBSTITUTION_BYTES, validation_message,
};

#[derive(Clone, Copy, Eq)]
struct AsciiCaseInsensitive<'a>(&'a str);

impl PartialEq for AsciiCaseInsensitive<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq_ignore_ascii_case(other.0)
    }
}

impl Hash for AsciiCaseInsensitive<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for byte in self.0.bytes() {
            state.write_u8(byte.to_ascii_uppercase());
        }
    }
}

static DYNAMODB_RESERVED_WORD_SET: LazyLock<HashSet<AsciiCaseInsensitive<'static>>> =
    LazyLock::new(|| {
        DYNAMODB_RESERVED_WORDS
            .split_ascii_whitespace()
            .map(AsciiCaseInsensitive)
            .collect()
    });

pub(crate) fn validate_expression_attribute_value_keys(
    values: Option<&crate::AttributeMap>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(values) = values else {
        return Ok(());
    };
    if values.is_empty() {
        return Err(validation_message(
            "ExpressionAttributeValues must not be empty".to_string(),
            prefixed,
        ));
    }
    let value_size = values.iter().try_fold(0usize, |size, (key, value)| {
        serde_json::to_vec(value)
            .map(|encoded| size + key.len() + encoded.len())
            .map_err(|err| format!("Invalid ExpressionAttributeValues: {err}"))
    })?;
    if value_size > MAX_EXPRESSION_SUBSTITUTION_BYTES {
        return Err(validation_message(
            "ExpressionAttributeValues exceeds max size".to_string(),
            prefixed,
        ));
    }
    for key in values.keys() {
        if key.len() > 255 {
            let size_suffix = if prefixed {
                format!(" size of key: {}", key.len())
            } else {
                String::new()
            };
            return Err(validation_message(
                format!(
                    "ExpressionAttributeValues contains invalid key: The expression attribute map \
                     contains a key that is too long;{size_suffix}"
                ),
                prefixed,
            ));
        }
        let mut chars = key.chars();
        if chars.next() != Some(':')
            || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || key.len() == 1
        {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeValues contains invalid key: Syntax error; key: \"{key}\""
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_expression_attribute_name_keys(
    names: Option<&crate::VecMap<String, String>>,
    prefixed: bool,
) -> Result<(), String> {
    let Some(names) = names else {
        return Ok(());
    };
    if names.is_empty() {
        return Err(validation_message(
            "ExpressionAttributeNames must not be empty".to_string(),
            prefixed,
        ));
    }
    let name_size = names
        .iter()
        .map(|(key, value)| key.len() + value.len())
        .sum::<usize>();
    if name_size > MAX_EXPRESSION_SUBSTITUTION_BYTES {
        return Err(validation_message(
            "ExpressionAttributeNames exceeds max size".to_string(),
            prefixed,
        ));
    }
    for key in names.keys() {
        if key.len() > 255 {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeNames contains invalid key: The expression attribute map \
                     contains a key that is too long; size of key: {}",
                    key.len()
                ),
                prefixed,
            ));
        }
        let mut chars = key.chars();
        if chars.next() != Some('#')
            || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            || key.len() == 1
        {
            return Err(validation_message(
                format!(
                    "ExpressionAttributeNames contains invalid key: Syntax error; key: \"{key}\""
                ),
                prefixed,
            ));
        }
    }
    Ok(())
}

pub(super) fn reserved_word_in_expression(expression: &str, kind: ExpressionKind) -> Option<&str> {
    let bytes = expression.as_bytes();
    let mut start = None;

    for (index, ch) in expression.char_indices() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            if start.is_none() {
                start = Some(index);
            }
            continue;
        }

        if let Some(token_start) = start.take()
            && let Some(token) =
                reserved_expression_token(expression, bytes, token_start, index, kind)
        {
            return Some(token);
        }
    }

    start.and_then(|token_start| {
        reserved_expression_token(expression, bytes, token_start, expression.len(), kind)
    })
}

fn reserved_expression_token<'a>(
    expression: &'a str,
    bytes: &[u8],
    start: usize,
    end: usize,
    kind: ExpressionKind,
) -> Option<&'a str> {
    let previous = start
        .checked_sub(1)
        .and_then(|index| bytes.get(index).copied());
    if matches!(previous, Some(b'#' | b':')) {
        return None;
    }

    let token = expression.get(start..end)?;
    let next = expression
        .get(end..)?
        .chars()
        .find(|ch| !ch.is_ascii_whitespace());
    if matches!(next, Some('(')) || is_expression_grammar_keyword(token, kind) {
        return None;
    }

    is_dynamodb_reserved_word(token).then_some(token)
}

pub(crate) fn is_dynamodb_reserved_word(token: &str) -> bool {
    DYNAMODB_RESERVED_WORD_SET.contains(&AsciiCaseInsensitive(token))
}

fn is_expression_grammar_keyword(token: &str, kind: ExpressionKind) -> bool {
    let is_one_of = |words: &[&str]| words.iter().any(|word| word.eq_ignore_ascii_case(token));
    match kind {
        ExpressionKind::Projection => false,
        ExpressionKind::Update => is_one_of(&[
            "AND", "OR", "NOT", "BETWEEN", "IN", "SET", "ADD", "REMOVE", "DELETE",
        ]),
        _ => is_one_of(&["AND", "OR", "NOT", "BETWEEN", "IN"]),
    }
}
