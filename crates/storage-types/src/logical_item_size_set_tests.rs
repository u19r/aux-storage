use std::hint::black_box;

use alloc_counter::AllocationGuard;

use crate::{AttributeValue, LogicalItemSize};

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_set_item_shapes_when_sized_then_report_cpu_and_allocations() {
    let mut fixtures = vec![
        (
            "scalar_number".to_owned(),
            AttributeValue::N("12345.67".to_owned()),
        ),
        (
            "number_zero_forms".to_owned(),
            AttributeValue::NS(vec!["-0e125".to_owned(), "1".to_owned()]),
        ),
        (
            "number_bounds".to_owned(),
            AttributeValue::NS(vec!["1e-130".to_owned(), "9.99e125".to_owned()]),
        ),
    ];
    for size in [1, 4, 8, 16, 32, 128] {
        fixtures.push((
            format!("number_{size}"),
            AttributeValue::NS((0..size).map(|n| n.to_string()).collect()),
        ));
        fixtures.push((
            format!("scientific_{size}"),
            AttributeValue::NS((0..size).map(|n| format!("{n}e2")).collect()),
        ));
        fixtures.push((
            format!("string_{size}"),
            AttributeValue::SS((0..size).map(|n| format!("member_{n}")).collect()),
        ));
        fixtures.push((
            format!("long_string_{size}"),
            AttributeValue::SS(
                (0..size)
                    .map(|n| format!("{}_{n}", "prefix".repeat(170)))
                    .collect(),
            ),
        ));
    }
    for size in [1, 4, 32] {
        use base64::Engine as _;
        fixtures.push((
            format!("binary_{size}"),
            AttributeValue::BS(
                (0..size)
                    .map(|n| base64::engine::general_purpose::STANDARD.encode([n as u8; 64]))
                    .collect(),
            ),
        ));
    }
    for (label, position, width) in [
        ("duplicate_first_32", 1, 8),
        ("duplicate_last_32", 31, 8),
        ("duplicate_long_32", 1, 1024),
    ] {
        let mut values: Vec<_> = (0..32)
            .map(|n| format!("{}_{n}", "x".repeat(width)))
            .collect();
        values[position] = values[0].clone();
        fixtures.push((label.to_owned(), AttributeValue::SS(values)));
    }
    for (label, value) in fixtures {
        let is_valid = LogicalItemSize::try_from_entries([("values", &value)]).is_ok();
        for _ in 0..100 {
            drop(black_box(LogicalItemSize::try_from_entries([(
                "values", &value,
            )])));
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "logical_sets",
            file!(),
            line!(),
            Some(&label),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..10_000 {
            let size = LogicalItemSize::try_from_entries([("values", black_box(&value))]);
            assert_eq!(size.is_ok(), is_valid);
            drop(black_box(size));
        }
        let cpu = started.elapsed();
        alloc_counter::emit_report(&guard.finish()).unwrap();
        eprintln!(
            "logical_sets label={label} iterations=10000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}

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
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_set_sizes_when_duplicates_scanned_then_report_algorithm_crossover() {
    for size in [1, 4, 8, 16, 32, 128] {
        for width in [8, 64, 1024] {
            let values: Vec<_> = (0..size)
                .map(|n| format!("{}_{n}", "x".repeat(width)))
                .collect();
            for (algorithm, operation) in [
                ("growing", growing_hash_duplicate as fn(&[String]) -> bool),
                ("reserved", reserved_hash_duplicate),
                ("linear", linear_duplicate),
            ] {
                assert!(!operation(&values));
                let label = format!("{algorithm}_{size}_{width}");
                let guard = AllocationGuard::start(
                    module_path!(),
                    "duplicate_scan",
                    file!(),
                    line!(),
                    Some(&label),
                );
                let started = cpu_time::ProcessTime::now();
                for _ in 0..10_000 {
                    assert!(!black_box(operation)(black_box(&values)));
                }
                let cpu = started.elapsed();
                alloc_counter::emit_report(&guard.finish()).unwrap();
                eprintln!(
                    "duplicate_scan label={label} iterations=10000 cpu_ns={}",
                    cpu.as_nanos()
                );
            }
        }
    }
}

fn growing_hash_duplicate(values: &[String]) -> bool {
    let mut seen = std::collections::HashSet::new();
    values.iter().any(|value| !seen.insert(value.as_str()))
}

fn reserved_hash_duplicate(values: &[String]) -> bool {
    let mut seen = std::collections::HashSet::with_capacity(values.len());
    values.iter().any(|value| !seen.insert(value.as_str()))
}

fn linear_duplicate(values: &[String]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
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
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_scientific_number_shapes_when_normalized_then_report_cpu_and_allocations() {
    for (label, value) in [
        ("plain", "12345.67"),
        ("positive", "12.3e2"),
        ("negative", "-12.3e2"),
        ("fractional", "-12.3e-2"),
        ("small_bound", "1e-130"),
        ("large_bound", "9.99e125"),
        ("precision", "12345678901234567890123456789012345678e-5"),
        ("zero", "-0e0"),
    ] {
        let expected = crate::normalize_dynamodb_number_for_write(value);
        for _ in 0..100 {
            black_box(crate::normalize_dynamodb_number_for_write(value));
        }
        let guard = AllocationGuard::start(
            module_path!(),
            "normalization",
            file!(),
            line!(),
            Some(label),
        );
        let started = cpu_time::ProcessTime::now();
        for _ in 0..100_000 {
            let normalized = crate::normalize_dynamodb_number_for_write(black_box(value));
            assert_eq!(normalized, expected);
            black_box(normalized);
        }
        let cpu = started.elapsed();
        alloc_counter::emit_report(&guard.finish()).unwrap();
        eprintln!(
            "normalization label={label} iterations=100000 cpu_ns={}",
            cpu.as_nanos()
        );
    }
}

#[test]
fn given_scientific_lexical_forms_when_normalized_then_preserve_decimal_spelling() {
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
