#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::string_slice)]
#![allow(clippy::cast_sign_loss)]
use core::str::FromStr;

use rust_decimal::prelude::*;

/// Binary encoding for Decimal values with variable length encoding that
/// preserves lexicographic sort order
pub trait SortableVec {
    /// Encode a numeric value (implemented for Decimal) into a byte vector that
    /// preserves ascending lexicographic order matching its numeric order.
    fn encode(&self) -> Vec<u8>;
    /// Decode from previously produced bytes.
    fn decode(bytes: &[u8]) -> Result<Self, DecimalEncodingError>
    where Self: Sized;

    /// NEW: Accept a numeric string (integer or decimal) and convert it into a
    /// `SortableNumeric` newtype (hex string of encoded bytes) that can be
    /// stored as part of keys (e.g. for descending/ascending ordering when
    /// combined with other segments). Implemented only for Decimal; other
    /// impls may return Unsupported errors if added later.
    fn from_numeric_str(num: &str) -> Result<SortableNumeric, DecimalEncodingError>
    where Self: Sized;

    /// Decode a `SortableNumeric` back into a Decimal (implemented for
    /// Decimal).
    fn decode_numeric(sortable: &SortableNumeric) -> Result<Decimal, DecimalEncodingError>;
}

/// A wrapper around a hex-encoded sortable numeric representation. Chosen as
/// String (not Vec<u8>) so it can easily concatenate into primary / sort keys
/// directly without additional base encoding.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SortableNumeric(pub String);

impl SortableNumeric {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn ascending(num: &str) -> Result<Self, DecimalEncodingError> {
        <Decimal as SortableVec>::from_numeric_str(num)
    }

    /// Produce a descending sortable variant by bitwise inverting the encoded
    /// bytes (so lexicographic ascending gives numeric descending). This keeps
    /// the same length and character set.
    pub fn descending(num: &str) -> Result<Self, DecimalEncodingError> {
        let base = <Decimal as SortableVec>::from_numeric_str(num)?;
        let bytes = hex_decode(base.as_str()).map_err(DecimalEncodingError::InvalidHex)?;
        let inverted: Vec<u8> = bytes.into_iter().map(|b| !b).collect();
        Ok(SortableNumeric(hex_encode(&inverted)))
    }
}

impl SortableVec for Decimal {
    fn encode(&self) -> Vec<u8> {
        let decimal = self;
        if decimal.is_zero() {
            // Special case for zero - use a value between negative and positive numbers
            return vec![0b0000_1111]; // 15 - sorts between negatives (0-14) and positives (16+)
        }

        let is_positive = decimal.is_sign_positive();
        let abs_decimal = decimal.abs();

        // Get the decimal parts: mantissa and scale for exact reconstruction
        let mantissa = abs_decimal.mantissa();
        let scale = abs_decimal.scale();
        let mantissa_u64 = if mantissa < 0 {
            (-mantissa) as u64
        } else {
            mantissa as u64
        };

        // Calculate order-of-magnitude for scale index selection
        let magnitude = calculate_magnitude(abs_decimal);
        let scale_index = magnitude_to_scale_index(magnitude);

        // Create a normalized value for sorting: multiply to get a large integer
        let normalized_value = create_normalized_value(abs_decimal, scale_index);
        let normalized_bytes = encode_normalized_value(normalized_value);

        // Encode original scale and mantissa for exact reconstruction
        let scale_bytes = encode_scale(scale);
        let mantissa_bytes = encode_significand(mantissa_u64);

        // Create the header byte: reserved(4) + scale_index(4) using least significant
        // bits This ensures all decimal numbers sort before UTF-8 characters
        // (which start at 65+)
        let mut result = Vec::with_capacity(
            1 + normalized_bytes.len() + scale_bytes.len() + mantissa_bytes.len(),
        );

        if is_positive {
            // For positive numbers: use scale_index directly in least significant bits

            let header = 0b0001_0000 | (scale_index as u8); // 0001SSSS format
            result.push(header);
            result.extend_from_slice(&normalized_bytes);
            result.extend_from_slice(&scale_bytes);
            result.extend_from_slice(&mantissa_bytes);
        } else {
            // For negative numbers: use inverted scale index to preserve sort order
            let inverted_scale = 15 - scale_index;

            let header = inverted_scale as u8; // 0000SSSS format (no high bits set)
            result.push(header);

            // Invert normalized, scale and mantissa bytes for negative numbers
            for byte in normalized_bytes {
                result.push(!byte);
            }
            for byte in scale_bytes {
                result.push(!byte);
            }
            for byte in mantissa_bytes {
                result.push(!byte);
            }
        }

        result
    }

    fn decode(bytes: &[u8]) -> Result<Self, DecimalEncodingError> {
        if bytes.is_empty() {
            return Err(DecimalEncodingError::EmptyInput);
        }

        let header = bytes[0];
        let is_positive = (header & 0b0001_0000) != 0; // Check positive marker bit

        if bytes.len() == 1 && header == 0b0000_1111 {
            return Ok(Decimal::ZERO);
        }

        let scale_index = if is_positive {
            (header & 0b0000_1111) as usize // Extract scale from least significant 4 bits
        } else {
            15 - (header & 0b0000_1111) as usize // Invert for negative numbers
        };

        if scale_index >= 16 {
            return Err(DecimalEncodingError::InvalidScaleIndex(scale_index));
        }

        let data_bytes = &bytes[1..];
        let (scale, mantissa) = if is_positive {
            decode_normalized_scale_and_mantissa(data_bytes)?
        } else {
            // For negative numbers, invert the bytes back
            let inverted: Vec<u8> = data_bytes.iter().map(|b| !b).collect();
            decode_normalized_scale_and_mantissa(&inverted)?
        };

        // Reconstruct the decimal using mantissa and scale directly
        let mut result = if scale == 0 {
            Decimal::from(mantissa)
        } else {
            Decimal::from_i128_with_scale(i128::from(mantissa), scale)
        };

        if !is_positive {
            result = -result;
        }

        Ok(result)
    }

    fn from_numeric_str(num: &str) -> Result<SortableNumeric, DecimalEncodingError> {
        let dec = Decimal::from_str(num)?;
        let bytes = dec.encode();
        Ok(SortableNumeric(hex_encode(&bytes)))
    }

    fn decode_numeric(sortable: &SortableNumeric) -> Result<Decimal, DecimalEncodingError> {
        let bytes = hex_decode(&sortable.0).map_err(DecimalEncodingError::InvalidHex)?;
        Decimal::decode(&bytes)
    }
}

static SCALING_FACTORS: [i32; 16] = [
    -18, -14, -10, -7, -4, -2, -1, 0, // 0-7: small/fractional numbers
    1, 2, 4, 7, 10, 14, 18, 22, // 8-15: larger numbers
];

/// Calculate the order of magnitude for scale index selection
fn calculate_magnitude(decimal: Decimal) -> i32 {
    if decimal.is_zero() {
        return 0;
    }

    // Use string representation to calculate magnitude
    let str_repr = decimal.abs().to_string();
    let decimal_pos = str_repr.find('.').unwrap_or(str_repr.len());

    // Count digits before decimal point

    let integer_part = &str_repr[..decimal_pos];
    let non_zero_digits = integer_part.trim_start_matches('0');

    if non_zero_digits.is_empty() {
        // Number is less than 1, need to count leading zeros after decimal
        if let Some(decimal_idx) = str_repr.find('.') {
            let fractional_part = &str_repr[decimal_idx + 1..];
            let leading_zeros =
                fractional_part.len() - fractional_part.trim_start_matches('0').len();

            -(leading_zeros as i32 + 1)
        } else {
            0
        }
    } else {
        (non_zero_digits.len() as i32) - 1
    }
}

/// Create a normalized integer value for sorting within the same magnitude
/// class
fn create_normalized_value(decimal: Decimal, _scale_index: usize) -> u64 {
    // Multiply by a large factor to create an integer representation that preserves
    // ordering

    // Use a smaller normalization factor to avoid overflow while still preserving
    // precision
    let normalization_factor = Decimal::new(1_000_000i64, 0); // 10^6

    // Handle multiplication overflow gracefully
    let normalized = decimal.checked_mul(normalization_factor).unwrap_or({
        // If multiplication would overflow, use the original value
        // This still preserves ordering for very large numbers
        decimal
    });

    normalized.abs().trunc().to_u64().unwrap_or(u64::MAX)
}

/// Encode normalized value using fixed-length encoding for proper negative
/// number sorting
fn encode_normalized_value(value: u64) -> Vec<u8> {
    // Use fixed 8-byte big-endian encoding to ensure proper lexicographic sorting
    // when bytes are inverted for negative numbers
    value.to_be_bytes().to_vec()
}

/// Map magnitude to a scale index (0-15)
fn magnitude_to_scale_index(magnitude: i32) -> usize {
    // Find the closest scale factor
    let mut best_index = 0;
    let mut best_diff = (magnitude - SCALING_FACTORS[0]).abs();

    for (i, &scale) in SCALING_FACTORS.iter().enumerate() {
        let diff = (magnitude - scale).abs();
        if diff < best_diff {
            best_diff = diff;
            best_index = i;
        }
    }

    best_index
}

/// Encode scale as variable-length bytes
fn encode_scale(scale: u32) -> Vec<u8> {
    // Always encode at least one byte, even for scale 0
    if scale == 0 {
        vec![0]
    } else {
        encode_significand(u64::from(scale))
    }
}

/// Decode scale and mantissa from byte array (skipping normalized bytes)
fn decode_normalized_scale_and_mantissa(bytes: &[u8]) -> Result<(u32, u64), DecimalEncodingError> {
    if bytes.is_empty() {
        return Ok((0, 0));
    }

    // Skip the fixed 8-byte normalized value
    if bytes.len() < 8 {
        return Err(DecimalEncodingError::InvalidDecimal(
            rust_decimal::Error::ErrorString("Insufficient bytes for normalized value".to_string()),
        ));
    }

    // Now decode scale and mantissa from remaining bytes after the 8-byte
    // normalized value
    decode_scale_and_mantissa(&bytes[8..])
}

/// Decode scale and mantissa from byte array
fn decode_scale_and_mantissa(bytes: &[u8]) -> Result<(u32, u64), DecimalEncodingError> {
    if bytes.is_empty() {
        return Ok((0, 0));
    }

    // First decode the scale
    let mut pos = 0;
    let mut scale_value = 0u64;
    let mut shift = 0;

    while pos < bytes.len() {
        let byte = bytes[pos];
        let value = u64::from(byte & 0x7F);
        scale_value |= value << shift;
        shift += 7;
        pos += 1;

        if (byte & 0x80) == 0 {
            break;
        }
    }

    let scale = scale_value as u32;

    // Then decode the mantissa from remaining bytes
    let mantissa = decode_significand(&bytes[pos..])?;

    Ok((scale, mantissa))
}

fn encode_significand(value: u64) -> Vec<u8> {
    if value == 0 {
        return vec![];
    }

    let mut result = Vec::new();
    let mut remaining = value;

    while remaining > 0 {
        let mut byte = (remaining & 0x7F) as u8;
        remaining >>= 7;

        if remaining > 0 {
            byte |= 0x80; // Set continuation bit
        }

        result.push(byte);
    }

    result
}

fn decode_significand(bytes: &[u8]) -> Result<u64, DecimalEncodingError> {
    if bytes.is_empty() {
        return Ok(0);
    }

    let mut result = 0u64;
    let mut shift = 0;

    for &byte in bytes {
        let value = u64::from(byte & 0x7F);
        result |= value << shift;
        shift += 7;

        if shift > 56 {
            return Err(DecimalEncodingError::Overflow);
        }

        if (byte & 0x80) == 0 {
            break;
        }
    }

    Ok(result)
}

#[derive(Debug, Clone, PartialEq)]
pub enum DecimalEncodingError {
    EmptyInput,
    InvalidScaleIndex(usize),
    Overflow,
    InvalidDecimal(rust_decimal::Error),
    InvalidHex(String),
}

impl From<rust_decimal::Error> for DecimalEncodingError {
    fn from(err: rust_decimal::Error) -> Self {
        DecimalEncodingError::InvalidDecimal(err)
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if !s.len().is_multiple_of(2) {
        return Err("length must be even".to_string());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..s.len()).step_by(2) {
        let hi = decode_hex_nibble(bytes[i])?;
        let lo = decode_hex_nibble(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn decode_hex_nibble(b: u8) -> Result<u8, String> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(10 + b - b'a'),
        b'A'..=b'F' => Ok(10 + b - b'A'),
        _ => Err(format!("invalid hex nibble: {}", b as char)),
    }
}

/// Deserialize a `u128` represented as a decimal string.
pub fn from_string_u128<'de, D>(deserializer: D) -> Result<u128, D::Error>
where D: serde::Deserializer<'de> {
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    s.parse::<u128>().map_err(serde::de::Error::custom)
}

/// Serialize a `u128` as a decimal string.
pub fn to_string_u128<S>(x: &u128, serializer: S) -> Result<S::Ok, S::Error>
where S: serde::Serializer {
    serializer.serialize_str(&x.to_string())
}
