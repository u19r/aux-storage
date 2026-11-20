//! Subscription filter policies (W3 design, section 3, with the AWS results
//! of section 14). Pure: no I/O. Materialization evaluates the policy of each
//! subscription and writes no delivery record for a message that does not
//! match.

use std::{collections::HashMap, fmt, net::IpAddr, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::{MessageAttribute, PubsubError, PubsubResult, PubsubValidationKind};

const MAX_POLICY_BYTES: usize = 256 * 1024;
const MAX_KEYS: usize = 5;
const MAX_COMBINATIONS: u64 = 150;
/// Numbers compare as fixed point with five decimal places.
const FIXED_SCALE: i64 = 100_000;
const FIXED_DECIMALS: i32 = 5;
const FIXED_LIMIT: i64 = 1_000_000_000 * FIXED_SCALE;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FilterPolicyScope {
    #[default]
    MessageAttributes,
    MessageBody,
}

impl FilterPolicyScope {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MessageAttributes => "MessageAttributes",
            Self::MessageBody => "MessageBody",
        }
    }
}

impl FromStr for FilterPolicyScope {
    type Err = PubsubError;

    fn from_str(value: &str) -> PubsubResult<Self> {
        match value {
            "MessageAttributes" => Ok(Self::MessageAttributes),
            "MessageBody" => Ok(Self::MessageBody),
            _ => Err(invalid(&format!(
                "FilterPolicyScope: Invalid value [{value}]. Please use either MessageBody or \
                 MessageAttributes"
            ))),
        }
    }
}

/// A parsed policy. Keys are AND, values of one key are OR, and each `$or`
/// group needs one matching branch.
#[derive(Debug, Clone, PartialEq)]
pub struct FilterPolicy {
    root: Policy,
}

#[derive(Debug, Clone, PartialEq, Default)]
struct Policy {
    leaves: Vec<(Vec<String>, Vec<Condition>)>,
    alternatives: Vec<Vec<Policy>>,
}

#[derive(Debug, Clone, PartialEq)]
enum Condition {
    Exact(String),
    /// `None`: outside the numeric range; it never matches.
    Number(Option<i64>),
    Null,
    Bool(bool),
    Prefix(String),
    /// Lower case.
    PrefixIgnoreCase(String),
    Suffix(String),
    EqualsIgnoreCase(String),
    Wildcard(String),
    Cidr(IpAddr, u8),
    AnythingBut(Vec<Literal>),
    AnythingButPrefix(String),
    AnythingButWildcard(String),
    AnythingButSuffix(String),
    AnythingButIgnoreCase(Vec<String>),
    Range(Vec<(Comparison, Option<i64>)>),
    Exists(bool),
}

#[derive(Debug, Clone, PartialEq)]
enum Literal {
    Text(String),
    Number(Option<i64>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Comparison {
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
    Equal,
}

/// One value that a key of a message has.
#[derive(Debug, Clone, PartialEq)]
enum Candidate {
    Text(String),
    Number(Option<i64>),
    Bool(bool),
    Null,
}

impl FilterPolicy {
    /// Validate a policy (design 3.4 as confirmed in section 14): a JSON
    /// object of at most 256 KiB, at most 5 keys, at most 150 value
    /// combinations. Depth and number precision are not limited (AWS accepts
    /// them).
    pub fn parse(raw: &str) -> PubsubResult<Self> {
        if raw.len() > MAX_POLICY_BYTES {
            return Err(invalid("FilterPolicy: Filter policy is too long"));
        }
        let Ok(Value::Object(object)) = serde_json::from_str::<Value>(raw) else {
            return Err(invalid("FilterPolicy: failed to parse JSON."));
        };
        let source = Source::new(raw);
        let root = Policy::parse(&source, &object, &[])?;
        if root.key_count() > MAX_KEYS {
            return Err(invalid(
                "FilterPolicy: Filter policy can not have more than 5 keys",
            ));
        }
        if root.combinations() > MAX_COMBINATIONS {
            return Err(invalid("FilterPolicy: Filter policy is too complex"));
        }
        Ok(Self { root })
    }

    /// Message attributes scope: a message with no attributes matches no
    /// policy, also not `exists: false`. A Binary attribute counts as absent.
    #[must_use]
    pub fn matches_attributes(&self, attributes: &HashMap<String, MessageAttribute>) -> bool {
        !attributes.is_empty()
            && self.root.matches(&|path: &[String]| match path {
                [name] => attributes
                    .get(name)
                    .map_or(Vec::new(), attribute_candidates),
                _ => Vec::new(),
            })
    }

    /// Message body scope: the body must be a JSON object. A JSON array at
    /// any level matches when one element matches.
    #[must_use]
    pub fn matches_body(&self, body: &str) -> bool {
        let Ok(value @ Value::Object(_)) = serde_json::from_str::<Value>(body) else {
            return false;
        };
        self.root
            .matches(&|path: &[String]| body_candidates(&value, path))
    }
}

impl Policy {
    fn parse(
        source: &Source,
        object: &Map<String, Value>,
        prefix: &[String],
    ) -> PubsubResult<Self> {
        let mut policy = Self::default();
        for (key, value) in object {
            if key == "$or" {
                policy.alternatives.push(parse_or(source, value, prefix)?);
                continue;
            }
            let path = [prefix, std::slice::from_ref(key)].concat();
            match value {
                Value::Array(values) if !values.is_empty() => {
                    let conditions = values
                        .iter()
                        .map(|value| Condition::parse(source, value))
                        .collect::<PubsubResult<_>>()?;
                    policy.leaves.push((path, conditions));
                }
                Value::Object(nested) => {
                    let nested = Self::parse(source, nested, &path)?;
                    policy.leaves.extend(nested.leaves);
                    policy.alternatives.extend(nested.alternatives);
                }
                Value::Array(_) => {
                    return Err(invalid(&format!(
                        "FilterPolicy: Match value must be String, number, true, false, or null \
                         at {key}"
                    )));
                }
                _ => {
                    return Err(invalid(&source.at_field(
                        &format!("FilterPolicy: \"{key}\" must be an object or an array"),
                        key,
                    )));
                }
            }
        }
        Ok(policy)
    }

    fn key_count(&self) -> usize {
        self.leaves.len()
            + self
                .alternatives
                .iter()
                .flatten()
                .map(Self::key_count)
                .sum::<usize>()
    }

    fn combinations(&self) -> u64 {
        let leaves = self.leaves.iter().fold(1_u64, |product, (_, values)| {
            product.saturating_mul(values.len() as u64)
        });
        self.alternatives.iter().fold(leaves, |product, group| {
            product.saturating_mul(group.iter().map(Self::combinations).sum())
        })
    }

    fn matches(&self, lookup: &dyn Fn(&[String]) -> Vec<Candidate>) -> bool {
        self.leaves.iter().all(|(path, conditions)| {
            let found = lookup(path);
            conditions.iter().any(|condition| condition.matches(&found))
        }) && self
            .alternatives
            .iter()
            .all(|group| group.iter().any(|branch| branch.matches(lookup)))
    }
}

fn parse_or(source: &Source, value: &Value, prefix: &[String]) -> PubsubResult<Vec<Policy>> {
    let Value::Array(branches) = value else {
        return Err(invalid("FilterPolicy: $or must be a list of objects"));
    };
    if branches.len() < 2 {
        return Err(invalid("FilterPolicy: $or must have at least 2 elements"));
    }
    branches
        .iter()
        .map(|branch| match branch {
            Value::Object(object) => Policy::parse(source, object, prefix),
            _ => Err(invalid("FilterPolicy: $or must be a list of objects")),
        })
        .collect()
}

impl Condition {
    fn parse(source: &Source, value: &Value) -> PubsubResult<Self> {
        match value {
            Value::String(text) => Ok(Self::Exact(text.clone())),
            Value::Number(number) => Ok(Self::Number(fixed(&number.to_string()))),
            Value::Null => Ok(Self::Null),
            Value::Bool(flag) => Ok(Self::Bool(*flag)),
            Value::Object(operator) if operator.len() == 1 => {
                let (name, argument) = operator
                    .iter()
                    .next()
                    .ok_or_else(|| unrecognized(source, ""))?;
                Self::parse_operator(source, name, argument)
            }
            _ => Err(invalid(
                "FilterPolicy: Match value must be String, number, true, false, or null",
            )),
        }
    }

    fn parse_operator(source: &Source, name: &str, argument: &Value) -> PubsubResult<Self> {
        match (name, argument) {
            ("prefix", Value::String(text)) => Ok(Self::Prefix(text.clone())),
            ("prefix", Value::Object(nested)) if nested.len() == 1 => match nested.iter().next() {
                Some((inner, Value::String(text))) if inner == "equals-ignore-case" => {
                    Ok(Self::PrefixIgnoreCase(text.to_lowercase()))
                }
                _ => Err(unrecognized(source, name)),
            },
            ("suffix", Value::String(text)) => Ok(Self::Suffix(text.clone())),
            ("equals-ignore-case", Value::String(text)) => {
                Ok(Self::EqualsIgnoreCase(text.to_lowercase()))
            }
            ("wildcard", Value::String(text)) => Ok(Self::Wildcard(text.clone())),
            ("exists", Value::Bool(flag)) => Ok(Self::Exists(*flag)),
            ("exists", _) => Err(invalid(&source.at_field(
                "FilterPolicy: exists match pattern must be either true or false.",
                name,
            ))),
            ("cidr", Value::String(text)) => parse_cidr(text),
            ("numeric", Value::Array(terms)) => parse_range(source, terms),
            ("anything-but", argument) => parse_anything_but(source, argument),
            _ => Err(unrecognized(source, name)),
        }
    }

    fn matches(&self, found: &[Candidate]) -> bool {
        match self {
            Self::Exists(expected) => found.is_empty() != *expected,
            _ => found.iter().any(|candidate| self.matches_value(candidate)),
        }
    }

    fn matches_value(&self, candidate: &Candidate) -> bool {
        let text = match candidate {
            Candidate::Text(text) => Some(text.as_str()),
            _ => None,
        };
        match self {
            Self::Exact(expected) => text == Some(expected.as_str()),
            Self::Number(expected) => {
                expected.is_some()
                    && matches!(candidate, Candidate::Number(value) if value == expected)
            }
            Self::Null => *candidate == Candidate::Null,
            Self::Bool(expected) => *candidate == Candidate::Bool(*expected),
            Self::Prefix(prefix) => text.is_some_and(|text| text.starts_with(prefix.as_str())),
            Self::PrefixIgnoreCase(prefix) => {
                text.is_some_and(|text| text.to_lowercase().starts_with(prefix.as_str()))
            }
            Self::Suffix(suffix) => text.is_some_and(|text| text.ends_with(suffix.as_str())),
            Self::EqualsIgnoreCase(expected) => {
                text.is_some_and(|text| text.to_lowercase() == *expected)
            }
            Self::Wildcard(pattern) => text.is_some_and(|text| wildcard_matches(pattern, text)),
            Self::Cidr(network, bits) => text
                .and_then(|text| text.parse::<IpAddr>().ok())
                .is_some_and(|address| in_network(address, *network, *bits)),
            Self::AnythingBut(excluded) => {
                !excluded.iter().any(|literal| literal.equals(candidate))
            }
            Self::AnythingButPrefix(prefix) => {
                text.is_some_and(|text| !text.starts_with(prefix.as_str()))
            }
            Self::AnythingButWildcard(pattern) => {
                text.is_some_and(|text| !wildcard_matches(pattern, text))
            }
            Self::AnythingButSuffix(suffix) => {
                text.is_some_and(|text| !text.ends_with(suffix.as_str()))
            }
            Self::AnythingButIgnoreCase(excluded) => {
                text.is_some_and(|text| !excluded.contains(&text.to_lowercase()))
            }
            Self::Range(bounds) => match candidate {
                Candidate::Number(Some(value)) => bounds.iter().all(|(comparison, bound)| {
                    bound.is_some_and(|bound| comparison.holds(*value, bound))
                }),
                _ => false,
            },
            Self::Exists(_) => false,
        }
    }
}

impl Literal {
    fn equals(&self, candidate: &Candidate) -> bool {
        match (self, candidate) {
            (Self::Text(expected), Candidate::Text(text)) => expected == text,
            (Self::Number(Some(expected)), Candidate::Number(Some(value))) => expected == value,
            _ => false,
        }
    }
}

impl Comparison {
    fn parse(source: &Source, value: &Value) -> PubsubResult<Self> {
        match value.as_str() {
            Some("<") => Ok(Self::Less),
            Some("<=") => Ok(Self::LessOrEqual),
            Some(">") => Ok(Self::Greater),
            Some(">=") => Ok(Self::GreaterOrEqual),
            Some("=") => Ok(Self::Equal),
            other => {
                let operator = other.map_or_else(|| value.to_string(), str::to_owned);
                Err(invalid(&source.after_element_following(
                    &format!("FilterPolicy: Unrecognized numeric range operator: {operator}"),
                    &value.to_string(),
                )))
            }
        }
    }

    const fn holds(self, value: i64, bound: i64) -> bool {
        match self {
            Self::Less => value < bound,
            Self::LessOrEqual => value <= bound,
            Self::Greater => value > bound,
            Self::GreaterOrEqual => value >= bound,
            Self::Equal => value == bound,
        }
    }
}

fn parse_range(source: &Source, terms: &[Value]) -> PubsubResult<Condition> {
    let pairs = terms.chunks(2).collect::<Vec<_>>();
    if !matches!(pairs.len(), 1 | 2) || !terms.len().is_multiple_of(2) {
        return Err(invalid(
            "FilterPolicy: Value of numeric must be an array of 2 or 4 elements",
        ));
    }
    let bounds = pairs
        .into_iter()
        .map(|pair| {
            let bound = pair[1].as_number().ok_or_else(|| {
                invalid("FilterPolicy: Value of numeric operator must be a number")
            })?;
            Ok((
                Comparison::parse(source, &pair[0])?,
                fixed(&bound.to_string()),
            ))
        })
        .collect::<PubsubResult<Vec<_>>>()?;
    if bounds.len() == 2
        && bounds
            .iter()
            .any(|(comparison, _)| *comparison == Comparison::Equal)
    {
        return Err(invalid("FilterPolicy: Bad numeric range operator"));
    }
    Ok(Condition::Range(bounds))
}

fn parse_anything_but(source: &Source, argument: &Value) -> PubsubResult<Condition> {
    let literal = |value: &Value| match value {
        Value::String(text) => Ok(Literal::Text(text.clone())),
        Value::Number(number) => Ok(Literal::Number(fixed(&number.to_string()))),
        _ => Err(invalid(
            "FilterPolicy: Inside anything but list, only strings and numbers are allowed",
        )),
    };
    match argument {
        Value::Array(values) => Ok(Condition::AnythingBut(
            values.iter().map(literal).collect::<PubsubResult<_>>()?,
        )),
        Value::Object(operator) if operator.len() == 1 => {
            match operator
                .iter()
                .next()
                .ok_or_else(|| unrecognized(source, ""))?
            {
                (name, Value::String(text)) if name == "prefix" => {
                    Ok(Condition::AnythingButPrefix(text.clone()))
                }
                (name, Value::String(text)) if name == "suffix" => {
                    Ok(Condition::AnythingButSuffix(text.clone()))
                }
                (name, Value::String(text)) if name == "equals-ignore-case" => {
                    Ok(Condition::AnythingButIgnoreCase(vec![text.to_lowercase()]))
                }
                (name, Value::String(text)) if name == "wildcard" => {
                    Ok(Condition::AnythingButWildcard(text.clone()))
                }
                (name, _) => Err(invalid(&source.at_field(
                    &format!("FilterPolicy: Unsupported anything-but pattern: {name}"),
                    name,
                ))),
            }
        }
        other => Ok(Condition::AnythingBut(vec![literal(other)?])),
    }
}

fn parse_cidr(text: &str) -> PubsubResult<Condition> {
    let malformed = || invalid("FilterPolicy: Malformed CIDR, one '/' required");
    let (address, bits) = text.split_once('/').ok_or_else(malformed)?;
    let address = address.parse::<IpAddr>().map_err(|_| malformed())?;
    let bits = bits.parse::<u8>().map_err(|_| malformed())?;
    let limit = if address.is_ipv4() { 32 } else { 128 };
    if bits > limit {
        return Err(malformed());
    }
    Ok(Condition::Cidr(address, bits))
}

fn in_network(address: IpAddr, network: IpAddr, bits: u8) -> bool {
    let (address, network, width) = match (address, network) {
        (IpAddr::V4(address), IpAddr::V4(network)) => (
            u128::from(u32::from(address)),
            u128::from(u32::from(network)),
            32,
        ),
        (IpAddr::V6(address), IpAddr::V6(network)) => {
            (u128::from(address), u128::from(network), 128)
        }
        _ => return false,
    };
    let shift = width - u32::from(bits);
    shift >= 128 || address >> shift == network >> shift
}

/// `*` matches any sequence of characters, including none. Each middle part
/// takes its first occurrence, which leaves the most text for later parts.
fn wildcard_matches(pattern: &str, text: &str) -> bool {
    let mut parts = pattern.split('*');
    let first = parts.next().unwrap_or_default();
    let Some(mut rest) = text.strip_prefix(first) else {
        return false;
    };
    let later = parts.collect::<Vec<_>>();
    let Some((last, middle)) = later.split_last() else {
        return rest.is_empty();
    };
    for part in middle {
        let Some((_, after)) = rest.split_once(part) else {
            return false;
        };
        rest = after;
    }
    rest.ends_with(last)
}

/// Fixed point with five decimal places, parsed from the decimal text with no
/// binary float: sign, integer digits, fraction digits and an optional
/// exponent. Digits after the fifth fraction digit are truncated. A value
/// outside ±1,000,000,000 is `None`.
fn fixed(text: &str) -> Option<i64> {
    let (negative, unsigned) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (unsigned, 0),
    };
    let (integer, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let digits = format!("{integer}{fraction}");
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    // Zero is zero with any exponent (review D11).
    if digits.bytes().all(|byte| byte == b'0') {
        return Some(0);
    }
    // The decimal point sits after `integer.len() + exponent` digits; keep
    // five more digits and truncate the rest.
    let point = i32::try_from(integer.len()).ok()?.checked_add(exponent)?;
    let kept = point.checked_add(FIXED_DECIMALS)?;
    let scaled = if kept <= 0 {
        0
    } else {
        let kept = usize::try_from(kept).ok()?;
        let significant = digits.trim_start_matches('0');
        let leading = digits.len() - significant.len();
        if kept.saturating_sub(leading) > 18 {
            return None;
        }
        let padded = format!("{digits:0<kept$}");
        padded.get(..kept)?.parse::<i64>().ok()?
    };
    (scaled <= FIXED_LIMIT).then_some(if negative { -scaled } else { scaled })
}

fn attribute_candidates(attribute: &MessageAttribute) -> Vec<Candidate> {
    match attribute {
        MessageAttribute::String(text) => vec![Candidate::Text(text.clone())],
        MessageAttribute::Number(text) => vec![Candidate::Number(fixed(text))],
        MessageAttribute::StringArray(json) => serde_json::from_str::<Vec<Value>>(json)
            .map(|values| values.iter().filter_map(json_candidate).collect())
            .unwrap_or_default(),
        MessageAttribute::Binary(_) => Vec::new(),
    }
}

fn body_candidates(value: &Value, path: &[String]) -> Vec<Candidate> {
    let Some((key, rest)) = path.split_first() else {
        return match value {
            Value::Array(values) => values.iter().filter_map(json_candidate).collect(),
            other => json_candidate(other).into_iter().collect(),
        };
    };
    match value {
        Value::Object(object) => object
            .get(key)
            .map_or(Vec::new(), |value| body_candidates(value, rest)),
        Value::Array(values) => values
            .iter()
            .flat_map(|value| body_candidates(value, path))
            .collect(),
        _ => Vec::new(),
    }
}

fn json_candidate(value: &Value) -> Option<Candidate> {
    match value {
        Value::String(text) => Some(Candidate::Text(text.clone())),
        Value::Number(number) => Some(Candidate::Number(fixed(&number.to_string()))),
        Value::Bool(flag) => Some(Candidate::Bool(*flag)),
        Value::Null => Some(Candidate::Null),
        Value::Array(_) | Value::Object(_) => None,
    }
}

fn unrecognized(source: &Source, name: &str) -> PubsubError {
    invalid(&source.at_field(
        &format!("FilterPolicy: Unrecognized match type {name}"),
        name,
    ))
}

/// The policy text that AWS parses: insignificant whitespace removed. Its
/// parse errors name the JSON location after the offending value began, as
/// `<message>\n at [Source: (String)"<policy>"; line: 1, column: <n>]` (suite
/// script `filter_policy_unknown_operator_rejected`).
struct Source {
    text: String,
}

impl Source {
    fn new(raw: &str) -> Self {
        let mut text = String::with_capacity(raw.len());
        let mut in_string = false;
        let mut escaped = false;
        for character in raw.chars() {
            if in_string {
                text.push(character);
                match (escaped, character) {
                    (true, _) => escaped = false,
                    (false, '\\') => escaped = true,
                    (false, '"') => in_string = false,
                    _ => {}
                }
            } else if !character.is_whitespace() {
                in_string = character == '"';
                text.push(character);
            }
        }
        Self { text }
    }

    /// The text from byte `start`, empty when `start` is not a character
    /// boundary.
    fn tail(&self, start: usize) -> &str {
        self.text.get(start..).unwrap_or_default()
    }

    /// The number of characters before byte `end`.
    fn chars_before(&self, end: usize) -> usize {
        self.text.get(..end).map_or(0, |head| head.chars().count())
    }

    /// The message located after the value of the first field named `key`.
    fn at_field(&self, message: &str, key: &str) -> String {
        let needle = format!("\"{key}\":");
        match self.text.find(&needle) {
            Some(start) => self.located(message, start + needle.len()),
            None => message.to_owned(),
        }
    }

    /// The message located after the array element that follows the first
    /// occurrence of `token`.
    fn after_element_following(&self, message: &str, token: &str) -> String {
        match self.text.find(token) {
            Some(start) if self.tail(start + token.len()).starts_with(',') => {
                let element = start + token.len() + 1;
                let end = self
                    .tail(element)
                    .find([',', ']', '}'])
                    .map_or(self.text.len(), |offset| element + offset);
                self.located_at(message, self.chars_before(end) + 1)
            }
            _ => message.to_owned(),
        }
    }

    /// `value_start` is the byte index of the first character of a value:
    /// Jackson has then consumed a bracket or an opening quote, or a whole
    /// scalar.
    fn located(&self, message: &str, value_start: usize) -> String {
        let consumed = match self.tail(value_start).chars().next() {
            Some('"' | '[' | '{') => self.chars_before(value_start) + 1,
            _ => {
                let end = self
                    .tail(value_start)
                    .find([',', ']', '}'])
                    .map_or(self.text.len(), |offset| value_start + offset);
                self.chars_before(end)
            }
        };
        self.located_at(message, consumed + 1)
    }

    fn located_at(&self, message: &str, column: usize) -> String {
        format!(
            "{message}\n at [Source: (String)\"{}\"; line: 1, column: {column}]",
            self.text
        )
    }
}

fn invalid(detail: &str) -> PubsubError {
    PubsubError::validation_with_detail(PubsubValidationKind::InvalidAttributeValue, detail)
}

impl fmt::Display for FilterPolicyScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
