use std::{hint::black_box, time::Instant};

use alloc_counter::AllocationGuard;

use super::{
    ExpressionKind,
    functions::{
        attribute_type_literal_operand_error, contains_same_operand_error, function_argument_lists,
        validate_attribute_type_value_operands, validate_begins_with_value_operands,
    },
};
use crate::{AttributeMap, AttributeValue};

#[test]
fn given_function_calls_when_scanned_then_preserves_case_nesting_and_unicode() {
    for (expression, expected) in [
        ("tenant = :tenant", vec![]),
        ("ConTaInS(tags, :tag)", vec!["tags, :tag"]),
        ("contains(étiquette, :tag)", vec!["étiquette, :tag"]),
        (
            "contains(tags, nested(a, b)) AND contains(other, :v)",
            vec!["tags, nested(a, b)", "other, :v"],
        ),
        (
            "contains(tags, contains(other, :v))",
            vec!["tags, contains(other, :v)"],
        ),
        (
            "contains(tags, :v) AND contains(unfinished",
            vec!["tags, :v"],
        ),
        ("contains (tags, :v)", vec![]),
        ("mycontains(tags, :v)", vec!["tags, :v"]),
    ] {
        assert_eq!(
            function_argument_lists(expression, "contains").collect::<Vec<_>>(),
            expected
        );
    }
}

#[test]
fn given_function_operands_when_validated_then_preserves_errors() {
    let values = operand_values();
    assert!(
        contains_same_operand_error("contains(tags, tags)", ExpressionKind::Filter, None)
            .unwrap()
            .contains("first operand: [tags]")
    );
    assert!(
        attribute_type_literal_operand_error("attribute_type(payload, S)", ExpressionKind::Filter)
            .unwrap()
            .contains("attribute_type")
    );
    assert!(
        validate_attribute_type_value_operands(
            "attribute_type(payload, :kind)",
            ExpressionKind::Filter,
            Some(&values),
            false
        )
        .is_ok()
    );
    assert!(
        validate_begins_with_value_operands(
            "begins_with(tenant, :revision)",
            ExpressionKind::Filter,
            Some(&values),
            false
        )
        .unwrap_err()
        .contains("operand type: N")
    );
}

#[test]
#[ignore = "performance report; run with --ignored --nocapture --test-threads=1"]
fn given_expression_function_scenarios_when_validated_repeatedly_then_report_cost() {
    const ITERATIONS: usize = 20_000;
    let values = operand_values();
    let cases = [
        (
            "absent",
            "tenant = :prefix AND revision >= :revision".to_owned(),
        ),
        ("single", "contains(tags, :prefix)".to_owned()),
        (
            "multiple",
            std::iter::repeat_n(
                "contains(tags, :prefix) AND attribute_type(payload, :kind) AND \
                 begins_with(tenant, :prefix)",
                8,
            )
            .collect::<Vec<_>>()
            .join(" AND "),
        ),
        (
            "unicode",
            "contains(étiquette, :prefix) AND begins_with(tenant, :prefix)".to_owned(),
        ),
        ("malformed", "contains(tags, contains(unfinished".to_owned()),
        ("invalid", "contains(tags, tags)".to_owned()),
        (
            "long_absent",
            format!(
                "{}revision >= :revision",
                "tenant = :prefix AND ".repeat(128)
            ),
        ),
    ];
    for (label, expression) in cases {
        let guard = AllocationGuard::start(
            module_path!(),
            "expression_functions",
            file!(),
            line!(),
            Some(label),
        );
        let started = Instant::now();
        let cpu_started = cpu_time::ProcessTime::try_now().unwrap();
        for _ in 0..ITERATIONS {
            let expression = black_box(expression.as_str());
            let _ = black_box(contains_same_operand_error(
                expression,
                ExpressionKind::Filter,
                None,
            ));
            let _ = black_box(attribute_type_literal_operand_error(
                expression,
                ExpressionKind::Filter,
            ));
            let _ = black_box(validate_attribute_type_value_operands(
                expression,
                ExpressionKind::Filter,
                Some(&values),
                false,
            ));
            let _ = black_box(validate_begins_with_value_operands(
                expression,
                ExpressionKind::Filter,
                Some(&values),
                false,
            ));
        }
        eprintln!(
            "expression_functions label={label} iterations={ITERATIONS} elapsed_ns={} cpu_ns={}",
            started.elapsed().as_nanos(),
            cpu_started.try_elapsed().unwrap().as_nanos()
        );
        alloc_counter::emit_report(&guard.finish()).unwrap();
    }
}

fn operand_values() -> AttributeMap {
    AttributeMap::from_iter([
        (":prefix".to_owned(), AttributeValue::S("tenant".to_owned())),
        (":kind".to_owned(), AttributeValue::S("M".to_owned())),
        (":revision".to_owned(), AttributeValue::N("1".to_owned())),
    ])
}
