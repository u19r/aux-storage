use super::{DynamoDecimal, StorageResult};

pub(in crate::update_logic) fn add_with_aligned_scratch(
    lhs: &DynamoDecimal,
    rhs: &DynamoDecimal,
) -> StorageResult<DynamoDecimal> {
    let scale = lhs.scale.max(rhs.scale);
    let mut lhs_digits = lhs.digits.clone();
    lhs_digits.extend(std::iter::repeat_n(0, (scale - lhs.scale) as usize));
    let mut rhs_digits = rhs.digits.clone();
    rhs_digits.extend(std::iter::repeat_n(0, (scale - rhs.scale) as usize));
    let value = DynamoDecimal {
        negative: lhs.negative,
        digits: add_abs_digits(&lhs_digits, &rhs_digits),
        scale,
    }
    .normalized();
    value.validate_range()?;
    Ok(value)
}

fn add_abs_digits(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(lhs.len().max(rhs.len()) + 1);
    let mut carry = 0u8;
    let mut lhs_index = lhs.len();
    let mut rhs_index = rhs.len();
    while lhs_index > 0 || rhs_index > 0 || carry > 0 {
        let lhs_digit = if lhs_index > 0 {
            lhs_index -= 1;
            lhs[lhs_index]
        } else {
            0
        };
        let rhs_digit = if rhs_index > 0 {
            rhs_index -= 1;
            rhs[rhs_index]
        } else {
            0
        };
        let sum = lhs_digit + rhs_digit + carry;
        result.push(sum % 10);
        carry = sum / 10;
    }
    result.reverse();
    result
}
