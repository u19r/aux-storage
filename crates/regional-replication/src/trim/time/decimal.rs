//! Exact fixed-point report conversion. Bounds include the CSV rounding unit.
pub(super) fn parse_scaled(value: &str, digits: u32) -> Option<i128> {
    let (negative, magnitude) = match value.strip_prefix('-') {
        Some(value) => (true, value),
        None => (false, value.strip_prefix('+').unwrap_or(value)),
    };
    let (whole, fraction) = magnitude.split_once('.').unwrap_or((magnitude, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > usize::try_from(digits).ok()?
    {
        return None;
    }
    let scale = 10_i128.checked_pow(digits)?;
    let whole = whole.parse::<i128>().ok()?.checked_mul(scale)?;
    let fraction_value = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<i128>().ok()?
    };
    let fraction_scale =
        10_i128.checked_pow(digits.checked_sub(u32::try_from(fraction.len()).ok()?)?)?;
    let magnitude = whole.checked_add(fraction_value.checked_mul(fraction_scale)?)?;
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}

pub(super) fn parse_seconds(value: &str) -> Option<i128> {
    parse_scaled(value, 9)
}

pub(super) fn parse_error(value: &str) -> Option<i128> {
    bound_report_magnitude(parse_seconds(value)?.checked_abs()?)
}

pub(super) fn parse_nonnegative(value: &str) -> Option<i128> {
    let value = parse_seconds(value)?;
    (value >= 0).then_some(value)
}

pub(super) fn parse_nonnegative_error(value: &str) -> Option<i128> {
    bound_report_magnitude(parse_nonnegative(value)?)
}

pub(super) fn parse_frequency_error(value: &str) -> Option<i128> {
    bound_report_magnitude(parse_scaled(value, 3)?.checked_abs()?)
}

fn bound_report_magnitude(value: i128) -> Option<i128> {
    // Chrony 4.6 monitoring Float has a signed 25-bit coefficient. Include
    // coefficient rounding/shifts as well as one outward CSV rounding unit.
    let coefficient_error = value.checked_add(4_194_303)?.checked_div(4_194_304)?;
    value.checked_add(coefficient_error)?.checked_add(1)
}
