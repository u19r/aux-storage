use crate::AttributeValue;

#[test]
fn given_equivalent_or_invalid_number_members_when_sized_then_preserve_validation_errors() {
    for members in [
        vec!["1", "1.0"],
        vec!["0", "-0e125"],
        vec!["100", "1e2"],
        vec!["0.001", "1e-3"],
    ] {
        let value = AttributeValue::NS(members.into_iter().map(str::to_owned).collect());
        let error = crate::validate_attribute_value_for_write(&value, "Item").unwrap_err();
        assert_eq!(
            error,
            "One or more parameter values were invalid: Input collection contains duplicates."
        );
    }
    for invalid in [
        "NaN",
        "1e126",
        "1e-131",
        "123456789012345678901234567890123456789",
    ] {
        let scalar = AttributeValue::N(invalid.to_owned());
        let set = AttributeValue::NS(vec!["1".to_owned(), invalid.to_owned()]);
        assert_eq!(
            crate::validate_attribute_value_for_write(&set, "Item").unwrap_err(),
            crate::validate_attribute_value_for_write(&scalar, "Item").unwrap_err()
        );
    }
    let value = AttributeValue::NS(vec!["1".to_owned(), "1.0".to_owned(), "NaN".to_owned()]);
    assert_eq!(
        crate::validate_attribute_value_for_write(&value, "Item").unwrap_err(),
        "One or more parameter values were invalid: Input collection contains duplicates."
    );
}

#[test]
fn given_duplicate_set_members_when_validated_then_preserve_errors_and_boundary_detection() {
    let strings = AttributeValue::SS(vec!["a\"b".to_owned(), "c".to_owned(), "a\"b".to_owned()]);
    assert_eq!(
        crate::validate_attribute_value_for_write(&strings, "Item").unwrap_err(),
        r#"One or more parameter values were invalid: Input collection ["a"b", "c", "a"b"] contains duplicates."#
    );
    let binary = AttributeValue::BS(vec!["not-base64".to_owned(), "not-base64".to_owned()]);
    assert_eq!(
        crate::validate_attribute_value_for_write(&binary, "Item").unwrap_err(),
        r#"One or more parameter values were invalid: Input collection ["not-base64","not-base64"]of type BS contains duplicates."#
    );
    for size in [2, 16, 17, 32, 128] {
        let unique: Vec<_> = (0..size).map(|n| format!("member_{n}")).collect();
        crate::validate_attribute_value_for_write(&AttributeValue::SS(unique.clone()), "Item")
            .unwrap();
        for position in 1..size {
            let mut repeated = unique.clone();
            repeated[position] = unique[0].clone();
            let error =
                crate::validate_attribute_value_for_write(&AttributeValue::SS(repeated), "Item")
                    .unwrap_err();
            assert!(error.ends_with("contains duplicates."), "{error}");
        }
    }
}

#[test]
fn given_scientific_lexical_forms_when_normalized_then_preserve_decimal_spelling_except_zero() {
    for mantissa in [
        "0",
        "-0",
        "+0",
        "0.0",
        "-0.0",
        "000.00100",
        ".5",
        "1.",
        "-001.2300",
        "1234567890.0000000001",
        "--1",
        "+-1",
        "1.2.3",
        "abc",
        "",
        ".",
        "1 0",
    ] {
        for exponent in ["-130", "-5", "-1", "0", "1", "2", "125", "oops", ""] {
            let raw = format!("{mantissa}e{exponent}");
            let expected = legacy_expand_scientific_number(&raw).unwrap_or_else(|| raw.clone());
            assert_eq!(
                crate::normalize_dynamodb_number_for_write(&raw),
                expected,
                "{raw}"
            );
        }
    }
}

fn legacy_expand_scientific_number(value: &str) -> Option<String> {
    let (mantissa, exponent) = value.split_once(['e', 'E'])?;
    let exponent = exponent.parse::<i32>().ok()?;
    let negative = mantissa.starts_with('-');
    let mantissa = mantissa.trim_start_matches(['+', '-']);
    let mut digits = String::new();
    let mut fractional_digits = 0i32;
    let mut after_decimal = false;
    for character in mantissa.chars() {
        match character {
            '0'..='9' => {
                digits.push(character);
                if after_decimal {
                    fractional_digits += 1;
                }
            }
            '.' if !after_decimal => after_decimal = true,
            _ => return None,
        }
    }
    if digits.is_empty() {
        return None;
    }
    // Zero is stored as `0` whatever its spelling or exponent.
    if digits.bytes().all(|byte| byte == b'0') {
        return Some("0".to_owned());
    }

    let decimal_position = digits.len() as i32 - fractional_digits + exponent;
    let expanded = if decimal_position <= 0 {
        format!(
            "0.{}{}",
            "0".repeat(decimal_position.unsigned_abs() as usize),
            digits
        )
    } else if decimal_position as usize >= digits.len() {
        format!(
            "{}{}",
            digits,
            "0".repeat(decimal_position as usize - digits.len())
        )
    } else {
        let split = decimal_position as usize;
        let (integer, fractional) = digits.split_at(split);
        format!("{integer}.{fractional}")
    };

    if negative && expanded != "0" {
        Some(format!("-{expanded}"))
    } else {
        Some(expanded)
    }
}
