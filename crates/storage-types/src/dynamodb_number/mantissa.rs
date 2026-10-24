//! Significant number digits have bounded storage even for long padding runs.
use std::iter::Peekable;

use smallvec::SmallVec;

use super::{DynamodbNumberError, ParsedDynamodbNumber};
use crate::constants::{DYNAMODB_NUMBER_INLINE_DIGITS, DYNAMODB_NUMBER_KEY_DIGITS};

#[derive(Default)]
pub(super) struct MantissaDigits {
    digits: SmallVec<[u8; DYNAMODB_NUMBER_INLINE_DIGITS]>,
    significant: usize,
    trailing_zeroes: i64,
}

impl MantissaDigits {
    pub(super) fn consume<I: Iterator<Item = u8>>(
        &mut self,
        input: &mut Peekable<I>,
    ) -> Result<i64, DynamodbNumberError> {
        let mut count = 0i64;
        while let Some(byte @ b'0'..=b'9') = input.peek().copied() {
            input.next();
            count = count
                .checked_add(1)
                .ok_or(DynamodbNumberError::AdjustedExponentOutOfRange)?;
            self.include_digit(byte)?;
        }
        Ok(count)
    }

    fn include_digit(&mut self, byte: u8) -> Result<(), DynamodbNumberError> {
        if byte == b'0' {
            if self.significant != 0 {
                self.trailing_zeroes = self
                    .trailing_zeroes
                    .checked_add(1)
                    .ok_or(DynamodbNumberError::AdjustedExponentOutOfRange)?;
            }
            return Ok(());
        }
        let zeroes = usize::try_from(self.trailing_zeroes)
            .map_err(|_| DynamodbNumberError::AdjustedExponentOutOfRange)?;
        self.significant = self
            .significant
            .checked_add(zeroes)
            .and_then(|count| count.checked_add(1))
            .ok_or(DynamodbNumberError::AdjustedExponentOutOfRange)?;
        if self.significant <= DYNAMODB_NUMBER_KEY_DIGITS {
            self.digits.extend(std::iter::repeat_n(b'0', zeroes));
            self.digits.push(byte);
        }
        self.trailing_zeroes = 0;
        Ok(())
    }

    pub(super) fn finish(
        self,
        negative: bool,
        fractional: i64,
        exponent: i64,
    ) -> Result<ParsedDynamodbNumber, DynamodbNumberError> {
        if self.significant == 0 {
            return Ok(ParsedDynamodbNumber {
                negative: false,
                adjusted_exponent: 0,
                digits: self.digits,
                is_zero: true,
            });
        }
        if self.significant > DYNAMODB_NUMBER_KEY_DIGITS {
            return Err(DynamodbNumberError::Precision);
        }
        let adjusted = i64::try_from(self.significant)
            .ok()
            .and_then(|count| count.checked_add(exponent))
            .and_then(|value| value.checked_sub(fractional))
            .and_then(|value| value.checked_add(self.trailing_zeroes))
            .and_then(|value| value.checked_sub(1))
            .and_then(|value| i32::try_from(value).ok())
            .ok_or(DynamodbNumberError::AdjustedExponentOutOfRange)?;
        if adjusted < -130 {
            return Err(DynamodbNumberError::Underflow);
        }
        if adjusted > 125 {
            return Err(DynamodbNumberError::Overflow);
        }
        Ok(ParsedDynamodbNumber {
            negative,
            adjusted_exponent: adjusted,
            digits: self.digits,
            is_zero: false,
        })
    }
}
