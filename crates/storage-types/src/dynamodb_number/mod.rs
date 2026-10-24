//! Exact DynamoDB number parsing and key encoding shared by storage backends.

mod mantissa;
mod parse;

use smallvec::SmallVec;

use crate::constants::{
    DYNAMODB_NUMBER_INLINE_DIGITS, DYNAMODB_NUMBER_KEY_BYTES, DYNAMODB_NUMBER_KEY_DIGITS,
    DYNAMODB_NUMBER_KEY_EXPONENT_BIAS, DYNAMODB_NUMBER_KEY_MARKER, DYNAMODB_WRITE_NUMBER_BYTES,
};

#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DynamodbNumberError {
    #[error("empty number")]
    Empty,
    #[error("number must contain a digit")]
    MissingDigit,
    #[error("invalid exponent")]
    InvalidExponent,
    #[error("exponent is out of range")]
    ExponentOutOfRange,
    #[error("invalid number")]
    Invalid,
    #[error("number has more than 38 significant digits")]
    Precision,
    #[error("number exponent is out of range")]
    AdjustedExponentOutOfRange,
    #[error("number is smaller than DynamoDB's supported range")]
    Underflow,
    #[error("number is larger than DynamoDB's supported range")]
    Overflow,
}

impl DynamodbNumberError {
    pub(crate) const fn validation_kind(self) -> crate::StorageValidationKind {
        match self {
            Self::Precision => crate::StorageValidationKind::NumberPrecision,
            Self::Underflow => crate::StorageValidationKind::NumberUnderflow,
            Self::Overflow => crate::StorageValidationKind::NumberOverflow,
            _ => crate::StorageValidationKind::InvalidNumber,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedDynamodbNumber {
    negative: bool,
    adjusted_exponent: i32,
    digits: SmallVec<[u8; DYNAMODB_NUMBER_INLINE_DIGITS]>,
    is_zero: bool,
}

impl ParsedDynamodbNumber {
    fn significant_digits(&self) -> Result<&str, DynamodbNumberError> {
        std::str::from_utf8(&self.digits).map_err(|_| DynamodbNumberError::Invalid)
    }

    fn canonical_string(&self) -> Result<String, DynamodbNumberError> {
        if self.is_zero {
            return Ok("0".to_string());
        }

        let decimal_position = self.adjusted_exponent + 1;
        let mut value = if decimal_position <= 0 {
            format!(
                "0.{}{}",
                "0".repeat(decimal_position.unsigned_abs() as usize),
                self.significant_digits()?
            )
        } else if decimal_position as usize >= self.digits.len() {
            format!(
                "{}{}",
                self.significant_digits()?,
                "0".repeat(decimal_position as usize - self.digits.len())
            )
        } else {
            let split = decimal_position as usize;
            let (integer, fractional) = self.significant_digits()?.split_at(split);
            format!("{integer}.{fractional}")
        };
        if self.negative {
            value.insert(0, '-');
        }
        Ok(value)
    }
}

/// Returns the exact canonical decimal spelling for a valid DynamoDB number.
pub fn canonical_dynamodb_number(value: &str) -> Result<String, DynamodbNumberError> {
    ParsedDynamodbNumber::parse(value).and_then(|number| number.canonical_string())
}

/// Compares valid DynamoDB numbers without losing decimal precision.
pub fn compare_dynamodb_numbers(
    left: &str,
    right: &str,
) -> Result<std::cmp::Ordering, DynamodbNumberError> {
    let left = ParsedDynamodbNumber::parse(left)?;
    let right = ParsedDynamodbNumber::parse(right)?;
    if left.negative != right.negative {
        return Ok(right.negative.cmp(&left.negative));
    }
    if left.is_zero || right.is_zero {
        return Ok(right.is_zero.cmp(&left.is_zero));
    }
    // Canonical significant digits have no trailing zeroes, so lexical order
    // also orders equal-exponent mantissas with different retained lengths.
    let magnitude = left
        .adjusted_exponent
        .cmp(&right.adjusted_exponent)
        .then_with(|| left.digits.cmp(&right.digits));
    Ok(if left.negative {
        magnitude.reverse()
    } else {
        magnitude
    })
}

/// Preserve bounded retained spelling while streaming longer tokens through the
/// number grammar.
pub fn project_dynamodb_number_from_bytes(
    input: impl Iterator<Item = u8>,
) -> Result<String, DynamodbNumberError> {
    let mut spelling = [0; DYNAMODB_WRITE_NUMBER_BYTES];
    let mut length = 0;
    let number = ParsedDynamodbNumber::parse_bytes(input.inspect(|&byte| {
        if let Some(slot) = spelling.get_mut(length) {
            *slot = byte;
            length += 1;
        } else {
            length = spelling.len() + 1;
        }
    }))?;
    if length <= spelling.len() {
        return Ok(spelling[..length]
            .iter()
            .map(|&byte| char::from(byte))
            .collect());
    }
    let decimal = number.canonical_string()?;
    if number.is_zero {
        return Ok(decimal);
    }
    let exponent = number.adjusted_exponent - (number.digits.len() as i32 - 1);
    let sign = if number.negative { "-" } else { "" };
    let scientific = format!("{sign}{}e{exponent}", number.significant_digits()?);
    Ok(if scientific.len() < decimal.len() {
        scientific
    } else {
        decimal
    })
}

pub(crate) fn validate_dynamodb_number(value: &str) -> Result<(), DynamodbNumberError> {
    ParsedDynamodbNumber::parse(value).map(|_| ())
}

/// Encodes a DynamoDB number into a fixed-width, lexicographically sortable
/// text key.
///
/// The marker makes the result unambiguously text even when SQLite key columns
/// are declared with a numeric-looking input. The bytes after the marker
/// preserve numeric ordering, including the reverse ordering required for
/// negative values.
pub fn encode_dynamodb_number_key(value: &str) -> Result<String, String> {
    let number = ParsedDynamodbNumber::parse(value).map_err(|error| error.to_string())?;
    let mut encoded = [0u8; DYNAMODB_NUMBER_KEY_BYTES];
    encode_number_bytes_into(&number, &mut encoded)?;
    let mut text = String::with_capacity(1 + encoded.len() * 2);
    text.push(DYNAMODB_NUMBER_KEY_MARKER);
    for byte in encoded {
        text.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        text.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    Ok(text)
}

/// Appends the raw sortable number bytes to an existing key buffer.
///
/// This keeps the parser and byte ordering policy shared with the public
/// encoder while avoiding a temporary `Vec` for callers that already own the
/// destination buffer.
pub(crate) fn append_dynamodb_number_key_bytes(
    value: &str,
    output: &mut Vec<u8>,
) -> Result<(), String> {
    let number = ParsedDynamodbNumber::parse(value).map_err(|error| error.to_string())?;
    let mut encoded = [0u8; DYNAMODB_NUMBER_KEY_BYTES];
    encode_number_bytes_into(&number, &mut encoded)?;
    output.extend_from_slice(&encoded);
    Ok(())
}

/// Decodes a key produced by the number encoder into canonical decimal text.
pub fn decode_dynamodb_number_key(value: &str) -> Result<String, String> {
    let Some(encoded) = value.strip_prefix(DYNAMODB_NUMBER_KEY_MARKER) else {
        return Err("number key is missing its codec marker".to_string());
    };
    if encoded.len() != DYNAMODB_NUMBER_KEY_BYTES * 2 {
        return Err("number key has an invalid encoded length".to_string());
    }
    let mut bytes = Vec::with_capacity(DYNAMODB_NUMBER_KEY_BYTES);
    let encoded = encoded.as_bytes();
    for chunk in encoded.as_chunks::<2>().0 {
        let high = decode_hex_digit(chunk[0])?;
        let low = decode_hex_digit(chunk[1])?;
        bytes.push((high << 4) | low);
    }
    decode_dynamodb_number_key_bytes(&bytes)
}

pub(crate) fn decode_dynamodb_number_key_bytes(bytes: &[u8]) -> Result<String, String> {
    decode_number_bytes(bytes)?
        .canonical_string()
        .map_err(|error| error.to_string())
}

fn encode_number_bytes_into(
    number: &ParsedDynamodbNumber,
    encoded: &mut [u8; DYNAMODB_NUMBER_KEY_BYTES],
) -> Result<(), String> {
    if number.is_zero {
        encoded.fill(0);
        encoded[0] = 1;
        return Ok(());
    }

    let biased_exponent = number
        .adjusted_exponent
        .checked_add(DYNAMODB_NUMBER_KEY_EXPONENT_BIAS)
        .ok_or_else(|| "number exponent is out of range".to_string())?;
    let biased_exponent = u16::try_from(biased_exponent)
        .map_err(|_| "number exponent is out of range".to_string())?;
    let negative = number.negative;
    encoded[0] = if negative { 0 } else { 2 };
    let [exponent_high, exponent_low] = biased_exponent.to_be_bytes();
    encoded[1] = if negative {
        !exponent_high
    } else {
        exponent_high
    };
    encoded[2] = if negative {
        !exponent_low
    } else {
        exponent_low
    };
    for index in 0..DYNAMODB_NUMBER_KEY_DIGITS {
        let byte = number.digits.get(index).copied().unwrap_or(b'0');
        encoded[3 + index] = if negative { !byte } else { byte };
    }
    Ok(())
}

fn decode_number_bytes(bytes: &[u8]) -> Result<ParsedDynamodbNumber, String> {
    if bytes.len() != DYNAMODB_NUMBER_KEY_BYTES {
        return Err("number key has an invalid byte length".to_string());
    }
    let tag = bytes[0];
    if tag == 1 {
        if bytes[1..].iter().any(|byte| *byte != 0) {
            return Err("zero number key has a non-zero payload".to_string());
        }
        return Ok(ParsedDynamodbNumber {
            negative: false,
            adjusted_exponent: 0,
            digits: SmallVec::new(),
            is_zero: true,
        });
    }
    let negative = match tag {
        0 => true,
        2 => false,
        _ => return Err("number key has an invalid sign tag".to_string()),
    };
    let mut payload = bytes[1..].to_vec();
    if negative {
        for byte in &mut payload {
            *byte = !*byte;
        }
    }
    let adjusted_exponent =
        i32::from(u16::from_be_bytes([payload[0], payload[1]])) - DYNAMODB_NUMBER_KEY_EXPONENT_BIAS;
    let digits = &payload[2..];
    if digits.iter().any(|byte| !byte.is_ascii_digit()) {
        return Err("number key has invalid digits".to_string());
    }
    let significant_end = digits
        .iter()
        .rposition(|byte| *byte != b'0')
        .ok_or_else(|| "non-zero number key has no significant digits".to_string())?;
    let significant = &digits[..=significant_end];
    let digits = SmallVec::from_slice(significant);
    if digits.len() > DYNAMODB_NUMBER_KEY_DIGITS || !(-130..=125).contains(&adjusted_exponent) {
        return Err("number key exponent or precision is out of range".to_string());
    }
    Ok(ParsedDynamodbNumber {
        negative,
        adjusted_exponent,
        digits,
        is_zero: false,
    })
}

fn decode_hex_digit(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("number key contains invalid hex".to_string()),
    }
}

#[cfg(test)]
mod dynamodb_number_tests;
