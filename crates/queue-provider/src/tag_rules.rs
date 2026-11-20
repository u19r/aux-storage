//! The one tag rule of SQS queues and SNS topics: at most 50 tags after the
//! merge; a key has 1 to 128 characters and does not start with `aws:`; a
//! value has at most 256 characters. Each service keeps its own error text.

use std::collections::BTreeMap;

const MAX_TAGS: usize = 50;
const MAX_TAG_KEY_CHARACTERS: usize = 128;
const MAX_TAG_VALUE_CHARACTERS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TagRuleViolation {
    TooMany,
    InvalidKey,
    /// A key that starts with `aws:` (any case) is a system tag.
    ReservedPrefix,
    InvalidValue,
}

/// Add or replace `added`, then remove `removed` (an unknown key is not an
/// error). `current` changes only when the merged set obeys the rule.
pub fn apply_tag_change(
    current: &mut BTreeMap<String, String>,
    added: BTreeMap<String, String>,
    removed: &[String],
) -> Result<(), TagRuleViolation> {
    for (key, value) in &added {
        if key.is_empty() || key.chars().count() > MAX_TAG_KEY_CHARACTERS {
            return Err(TagRuleViolation::InvalidKey);
        }
        if key.to_ascii_lowercase().starts_with("aws:") {
            return Err(TagRuleViolation::ReservedPrefix);
        }
        if value.chars().count() > MAX_TAG_VALUE_CHARACTERS {
            return Err(TagRuleViolation::InvalidValue);
        }
    }
    let mut merged = current.clone();
    merged.extend(added);
    for key in removed {
        merged.remove(key);
    }
    if merged.len() > MAX_TAGS {
        return Err(TagRuleViolation::TooMany);
    }
    *current = merged;
    Ok(())
}
