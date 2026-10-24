//! Number grammar consumes padding without retaining it in a temporary string.
use std::iter::Peekable;

use super::{DynamodbNumberError, ParsedDynamodbNumber, mantissa::MantissaDigits};

impl ParsedDynamodbNumber {
    pub(super) fn parse(raw: &str) -> Result<Self, DynamodbNumberError> {
        Self::parse_bytes(raw.bytes())
    }

    pub(super) fn parse_bytes(
        input: impl Iterator<Item = u8>,
    ) -> Result<Self, DynamodbNumberError> {
        let mut input = input.peekable();
        if input.peek().is_none() {
            return Err(DynamodbNumberError::Empty);
        }
        let negative = input.peek() == Some(&b'-');
        if matches!(input.peek(), Some(b'+') | Some(b'-')) {
            input.next();
        }
        let mut digits = MantissaDigits::default();
        let integer = digits.consume(&mut input)?;
        let fractional = if input.peek() == Some(&b'.') {
            input.next();
            digits.consume(&mut input)?
        } else {
            0
        };
        if integer == 0 && fractional == 0 {
            return Err(DynamodbNumberError::MissingDigit);
        }
        let exponent = parse_exponent(&mut input)?;
        if input.next().is_some() {
            return Err(DynamodbNumberError::Invalid);
        }
        digits.finish(negative, fractional, exponent)
    }
}

fn parse_exponent<I: Iterator<Item = u8>>(
    input: &mut Peekable<I>,
) -> Result<i64, DynamodbNumberError> {
    if !matches!(input.peek(), Some(b'e') | Some(b'E')) {
        return Ok(0);
    }
    input.next();
    let negative = input.peek() == Some(&b'-');
    if matches!(input.peek(), Some(b'+') | Some(b'-')) {
        input.next();
    }
    let mut value = Some(0i64);
    let mut has_digits = false;
    while let Some(byte @ b'0'..=b'9') = input.peek().copied() {
        input.next();
        has_digits = true;
        value = value
            .and_then(|value| value.checked_mul(10))
            .and_then(|value| value.checked_add(i64::from(byte - b'0')));
    }
    if !has_digits {
        return Err(DynamodbNumberError::InvalidExponent);
    }
    let value = value.ok_or(DynamodbNumberError::ExponentOutOfRange)?;
    if negative {
        value
            .checked_neg()
            .ok_or(DynamodbNumberError::ExponentOutOfRange)
    } else {
        Ok(value)
    }
}
