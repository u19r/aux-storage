use alloc_counter::AllocationGuard;

use super::{ExpressionKind, validate_expression_set};
use crate::AttributeValue;

fn legacy_in_operand_lists(expression: &str) -> Vec<&str> {
    let mut lists = Vec::new();
    let mut offset = 0usize;
    let upper = expression.to_ascii_uppercase();
    while let Some(relative_index) = upper.get(offset..).and_then(|tail| tail.find(" IN ")) {
        let after_in = offset + relative_index + 4;
        let Some(open_relative) = expression.get(after_in..).and_then(|tail| tail.find('(')) else {
            break;
        };
        let open = after_in + open_relative;
        let mut depth = 1usize;
        let args_start = open + 1;
        let Some(tail) = expression.get(args_start..) else {
            break;
        };
        let mut found_close = false;
        for (relative_arg_index, ch) in tail.char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        let close = args_start + relative_arg_index;
                        if let Some(args) = expression.get(args_start..close) {
                            lists.push(args);
                        }
                        offset = close + 1;
                        found_close = true;
                        break;
                    }
                }
                _ => {}
            }
        }
        if !found_close {
            break;
        }
    }
    lists
}

#[test]
fn given_in_operand_shapes_when_scanned_then_preserve_nested_unicode_and_malformed_results() {
    for expression in [
        "tenant_key = :v0",
        "tenant_key IN (:v0, :v1)",
        "étiquette in (:v0, function(:v1)) AND x IN (:v2)",
        "x IN (",
        "x IN",
        "x IN ((:v0)",
        "x IN (:v0))",
        "x IN (:v0) AND y in (:v1)",
        "x IN (:v0) AND y IN (",
    ] {
        let actual: Vec<_> = super::shape::in_operand_lists(expression).collect();
        assert_eq!(actual, legacy_in_operand_lists(expression), "{expression}");
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_in_scanner_cases_when_repeated_then_report_cpu_and_allocations() {
    let long = format!("{} in (:v0, :v1)", "étiquette".repeat(100));
    for (label, expression) in [
        ("simple", "tenant_key = :v0"),
        ("in", "tenant_key IN (:v0, :v1)"),
        ("unicode", long.as_str()),
        ("nested", "x IN (:v0, function(:v1)) AND y in (:v2)"),
        ("malformed", "x IN ((:v0)"),
    ] {
        for optimized in [false, true] {
            let label = format!(
                "{}_{}",
                if optimized { "optimized" } else { "baseline" },
                label
            );
            let operation = || {
                if optimized {
                    super::shape::in_operand_lists(std::hint::black_box(expression))
                        .map(str::len)
                        .sum::<usize>()
                } else {
                    legacy_in_operand_lists(std::hint::black_box(expression))
                        .into_iter()
                        .map(str::len)
                        .sum::<usize>()
                }
            };
            for _ in 0..100 {
                std::hint::black_box(operation());
            }
            let guard = AllocationGuard::start(
                module_path!(),
                "in_scanner",
                file!(),
                line!(),
                Some(label.as_str()),
            );
            let started = cpu_time::ProcessTime::now();
            for _ in 0..100_000 {
                std::hint::black_box(operation());
            }
            let cpu = started.elapsed();
            let report = guard.finish();
            alloc_counter::emit_report(&report).unwrap();
            eprintln!(
                "in_scanner label={label} iterations=100000 cpu_ns={}",
                cpu.as_nanos()
            );
        }
    }
}

#[test]
fn given_in_operand_limits_when_validated_then_allow_one_hundred_and_reject_one_hundred_one() {
    for count in [1, 100, 101] {
        let values: crate::AttributeMap = (0..count)
            .map(|index| (format!(":v{index}"), AttributeValue::N(index.to_string())))
            .collect();
        let operands = values.keys().collect::<Vec<_>>().join(", ");
        let expression = format!("tenant_key iN ({operands})");
        let result = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Condition)],
            None,
            Some(&values),
            false,
        );
        if count <= 100 {
            result.unwrap();
        } else {
            assert_eq!(
                result.unwrap_err(),
                "Invalid ConditionExpression: The IN operator is provided with too many operands; \
                 number of operands: 101"
            );
        }
    }
}

#[test]
fn given_invalid_function_case_when_validated_then_preserve_function_name_error() {
    for name in ["ATTRIBUTE_EXISTS", "Attribute_exists", "aTtRiBuTe_ExIsTs"] {
        let expression = format!("{name} (tenant_key)");
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Condition)],
            None,
            None,
            false,
        )
        .unwrap_err();
        assert_eq!(
            error,
            format!("Invalid ConditionExpression: Invalid function name; function: {name}")
        );
    }
}

fn legacy_expression_function_names(expression: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut index = 0usize;
    while index < expression.len() {
        let Some((relative_start, _)) = expression.get(index..).and_then(|tail| {
            tail.char_indices()
                .find(|(_, ch)| ch.is_ascii_alphabetic() || *ch == '_')
        }) else {
            break;
        };
        let start = index + relative_start;
        let mut end = start;
        for (relative_index, ch) in expression.get(start..).unwrap_or_default().char_indices() {
            if relative_index == 0 {
                end = start + ch.len_utf8();
                continue;
            }
            if ch.is_ascii_alphanumeric() || ch == '_' {
                end = start + relative_index + ch.len_utf8();
            } else {
                break;
            }
        }
        let tail = expression.get(end..).unwrap_or_default().trim_start();
        if tail.starts_with('(')
            && let Some(name) = expression.get(start..end)
        {
            names.push(name);
        }
        index = end;
    }
    names
}

#[test]
fn given_function_name_shapes_when_scanned_then_preserve_borrowed_names() {
    for expression in [
        "field",
        "function (nested(value))",
        "étiquette.attr(x)",
        "ATTRIBUTE_EXISTS (tenant_key)",
        "_func\u{2003}(x)",
        "12call(x)",
    ] {
        assert_eq!(
            super::shape::expression_function_names(expression).collect::<Vec<_>>(),
            legacy_expression_function_names(expression)
        );
    }
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_function_name_cases_when_repeated_then_report_cpu_and_allocations() {
    let long = format!("{}(tenant_key)", "étiquette".repeat(100));
    for (name, expression) in [
        ("simple", "tenant_key = :v0"),
        ("in", "tenant_key IN (:v0, :v1)"),
        (
            "functions",
            "attribute_exists(tenant_key) AND attribute_not_exists(payload_key)",
        ),
        ("unicode", long.as_str()),
        (
            "nested",
            "list_append(if_not_exists(notes_key, :empty), :value)",
        ),
    ] {
        for optimized in [false, true] {
            let label = format!(
                "{}_{}",
                if optimized { "optimized" } else { "baseline" },
                name
            );
            let operation = || {
                if optimized {
                    super::shape::expression_function_names(std::hint::black_box(expression))
                        .map(str::len)
                        .sum::<usize>()
                } else {
                    legacy_expression_function_names(std::hint::black_box(expression))
                        .into_iter()
                        .map(str::len)
                        .sum::<usize>()
                }
            };
            for _ in 0..100 {
                std::hint::black_box(operation());
            }
            let guard = AllocationGuard::start(
                module_path!(),
                "function_names",
                file!(),
                line!(),
                Some(label.as_str()),
            );
            let started = cpu_time::ProcessTime::now();
            for _ in 0..100_000 {
                std::hint::black_box(operation());
            }
            let cpu = started.elapsed();
            alloc_counter::emit_report(&guard.finish()).unwrap();
            eprintln!(
                "function_names label={label} iterations=100000 cpu_ns={}",
                cpu.as_nanos()
            );
        }
    }
}

#[test]
fn given_invalid_update_values_when_validated_then_preserve_payload_and_operand_errors() {
    for keyword in ["ADD", "DELETE"] {
        let values =
            crate::AttributeMap::from_iter([(":delta".to_owned(), AttributeValue::NS(Vec::new()))]);
        let expression = format!("{keyword} tally_key :delta");
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Update)],
            None,
            Some(&values),
            false,
        )
        .unwrap_err();
        assert_eq!(
            error,
            "One or more parameter values were invalid: An number set  may not be empty"
        );
        let values = crate::AttributeMap::from_iter([(
            ":delta".to_owned(),
            AttributeValue::S("text".to_owned()),
        )]);
        let error = validate_expression_set(
            [(Some(expression.as_str()), ExpressionKind::Update)],
            None,
            Some(&values),
            false,
        )
        .unwrap_err();
        assert!(
            error.contains(&format!("operator: {keyword}, operand type: S")),
            "{error}"
        );
    }
}
