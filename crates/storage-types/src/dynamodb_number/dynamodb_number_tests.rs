use super::{DYNAMODB_NUMBER_KEY_BYTES, DYNAMODB_NUMBER_KEY_EXPONENT_BIAS, ParsedDynamodbNumber};

fn legacy_encode_dynamodb_number_key(value: &str) -> Result<String, String> {
    let number = ParsedDynamodbNumber::parse(value).map_err(|error| error.to_string())?;
    let mut encoded = Vec::with_capacity(DYNAMODB_NUMBER_KEY_BYTES);
    if number.is_zero {
        encoded.push(1);
        encoded.resize(DYNAMODB_NUMBER_KEY_BYTES, 0);
    } else {
        let biased_exponent = number
            .adjusted_exponent
            .checked_add(DYNAMODB_NUMBER_KEY_EXPONENT_BIAS)
            .ok_or_else(|| "number exponent is out of range".to_string())?;
        let biased_exponent = u16::try_from(biased_exponent)
            .map_err(|_| "number exponent is out of range".to_string())?;
        let mut payload = Vec::with_capacity(DYNAMODB_NUMBER_KEY_BYTES - 1);
        payload.extend_from_slice(&biased_exponent.to_be_bytes());
        payload.extend_from_slice(number.digits.as_slice());
        payload.resize(DYNAMODB_NUMBER_KEY_BYTES - 1, b'0');
        if number.negative {
            encoded.push(0);
            encoded.extend(payload.into_iter().map(|byte| !byte));
        } else {
            encoded.push(2);
            encoded.extend(payload);
        }
    }
    let mut text = String::with_capacity(1 + encoded.len() * 2);
    text.push('~');
    for byte in encoded {
        text.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
        text.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
    }
    Ok(text)
}

use alloc_counter::AllocationGuard;

use super::{canonical_dynamodb_number, decode_dynamodb_number_key, encode_dynamodb_number_key};

fn measure_text_encoder(
    label: &'static str,
    encode: fn(&str) -> Result<String, String>,
) -> alloc_counter::AllocationReport<'static> {
    let guard = AllocationGuard::start(
        module_path!(),
        "dynamodb_number_text_encoder_allocation_profile",
        file!(),
        line!(),
        Some(label),
    );
    for _ in 0..256 {
        let encoded = encode("-12345.6789").expect("valid number");
        std::hint::black_box(encoded);
    }
    guard.finish()
}

#[test]
fn canonicalizes_equal_forms_and_signed_zero() {
    assert_eq!(canonical_dynamodb_number("1"), Ok("1".to_string()));
    assert_eq!(canonical_dynamodb_number("1.0"), Ok("1".to_string()));
    assert_eq!(canonical_dynamodb_number("1E0"), Ok("1".to_string()));
    assert_eq!(canonical_dynamodb_number("-0"), Ok("0".to_string()));
}

#[test]
fn encodes_full_domain_without_loss_and_round_trips() {
    for value in [
        "99999999999999999999999999999999999999",
        "1E-130",
        "-1E-130",
        "9.9999999999999999999999999999999999999E125",
    ] {
        let encoded = encode_dynamodb_number_key(value).expect("valid number");
        assert_eq!(
            decode_dynamodb_number_key(&encoded).unwrap(),
            canonical_dynamodb_number(value).unwrap()
        );
    }
}

#[test]
fn encoded_order_is_numeric_and_equal_forms_share_bytes() {
    let numbers = [
        "-99999999999999999999999999999999999999",
        "-1E-130",
        "-0",
        "0",
        "1E-130",
        "2",
        "99999999999999999999999999999999999999",
    ];
    let mut encoded = numbers
        .iter()
        .map(|value| (encode_dynamodb_number_key(value).unwrap(), *value))
        .collect::<Vec<_>>();
    encoded.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        encoded
            .into_iter()
            .map(|(_, value)| value)
            .collect::<Vec<_>>(),
        numbers
    );
    assert_eq!(
        encode_dynamodb_number_key("1").unwrap(),
        encode_dynamodb_number_key("1.0").unwrap()
    );
}

#[test]
fn encoded_order_preserves_adjacent_max_precision_values() {
    let positive_lower = "99999999999999999999999999999999999998";
    let positive_upper = "99999999999999999999999999999999999999";
    let negative_lower = "-99999999999999999999999999999999999999";
    let negative_upper = "-99999999999999999999999999999999999998";

    assert!(
        encode_dynamodb_number_key(positive_lower).unwrap()
            < encode_dynamodb_number_key(positive_upper).unwrap()
    );
    assert!(
        encode_dynamodb_number_key(negative_lower).unwrap()
            < encode_dynamodb_number_key(negative_upper).unwrap()
    );
    assert_eq!(
        decode_dynamodb_number_key(&encode_dynamodb_number_key(positive_upper).unwrap()).unwrap(),
        positive_upper
    );
    assert_eq!(
        decode_dynamodb_number_key(&encode_dynamodb_number_key(negative_lower).unwrap()).unwrap(),
        negative_lower
    );
}

#[test]
fn hostile_exponents_are_rejected_without_integer_overflow() {
    for value in [
        "1e9223372036854775807",
        "1e-9223372036854775808",
        "1e+9223372036854775808",
    ] {
        assert!(canonical_dynamodb_number(value).is_err(), "{value}");
    }
}

#[test]
fn text_encoder_preserves_legacy_bytes_and_avoids_payload_allocation() {
    for value in [
        "-99999999999999999999999999999999999999",
        "-1E-130",
        "-0",
        "0",
        "1E-130",
        "2",
        "99999999999999999999999999999999999999",
    ] {
        assert_eq!(
            encode_dynamodb_number_key(value),
            legacy_encode_dynamodb_number_key(value),
            "encoded bytes changed for {value}"
        );
    }

    let legacy = measure_text_encoder("legacy_vec_payload", legacy_encode_dynamodb_number_key);
    let candidate = measure_text_encoder("stack_payload", encode_dynamodb_number_key);
    let _ = alloc_counter::emit_report(&legacy);
    let _ = alloc_counter::emit_report(&candidate);
    assert!(candidate.allocation_count < legacy.allocation_count);
    assert!(candidate.allocated_bytes < legacy.allocated_bytes);
}
