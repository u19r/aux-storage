//! Expected texts are AWS answers: dynamo-conformance cases and
//! `docs/plans/incomplete/handover/ddb-parity-aws-probe-2026-11-13.log` P1–P8.

use super::{ExpressionKind, syntax::validate_expression_syntax};

fn syntax_error(kind: ExpressionKind, expression: &str) -> String {
    validate_expression_syntax(expression, kind, false).unwrap_err()
}

fn token_near(kind: ExpressionKind, token: &str, near: &str) -> String {
    format!("Invalid {kind}: Syntax error; token: \"{token}\", near: \"{near}\"")
}

#[test]
fn given_aws_recorded_syntax_errors_when_validated_then_token_and_near_match() {
    use ExpressionKind::{Condition, Filter, KeyCondition, Projection, Update};
    for (kind, expression, token, near) in [
        // dynamo-conformance cases.
        (Projection, "#list[-1].#comment", "-", "[-1"),
        (Projection, "#list[].#comment", "]", "[]."),
        (Condition, "#list[abc].#comment = :value", "abc", "[abc]"),
        (Filter, "contains()", ")", "()"),
        (Update, "SET #list[0.#comment = :value", ".", "0.#comment"),
        (Update, "ADD numx", "<EOF>", "numx"),
        (Update, "ADD numx 1", "1", "numx 1"),
        (Update, "SET #a = :v +", "<EOF>", "+"),
        (Update, "SET", "<EOF>", "SET"),
        (KeyCondition, "pk =", "<EOF>", "="),
        (Condition, "attribute_exists(", "<EOF>", "("),
        // P1
        (Condition, "x = = :v", "=", "= = :v"),
        (Condition, "x = :v AND", "<EOF>", "AND"),
        (Condition, "(x = :v", "<EOF>", ":v"),
        (Condition, "x = :v)", ")", ":v)"),
        (Condition, "x BETWEEN :v", "<EOF>", ":v"),
        (Condition, "x IN ()", ")", "()"),
        (Filter, "invalid filter", "filter", "invalid filter"),
        (KeyCondition, "pk = :pk AND", "<EOF>", "AND"),
        (KeyCondition, "pk = :pk sk", "sk", ":pk sk"),
        (Update, "SET x = :v REMOVE", "<EOF>", "REMOVE"),
        (Update, "REMOVE", "<EOF>", "REMOVE"),
        (Update, "FOO x = :v", "FOO", "FOO x"),
        (Update, "SET x = :v,", "<EOF>", ","),
        (Update, "REMOVE x,", "<EOF>", ","),
        (Projection, "x,", "<EOF>", ","),
        (Projection, "x,,s", ",", ",,s"),
        (Projection, ",x", ",", ",x"),
        (Projection, "x s", "s", "x s"),
        (Projection, "x.", "<EOF>", "."),
        (Projection, "#", "#", "#"),
        (Projection, "x[1]]", "]", "]]"),
        (Projection, "x.[0]", "[", ".[0"),
        // P2 literal operands
        (Condition, "x = 'lit'", "'", "= 'lit"),
        (Condition, "x = \"lit\"", "\"", "= \"lit"),
        (Condition, "x = 5", "5", "= 5"),
        (KeyCondition, "pk = 5", "5", "= 5"),
        (Update, "SET x = 5", "5", "= 5"),
        (Update, "SET x = \"s\"", "\"", "= \"s"),
        // P3 characters the lexer does not know
        (Filter, "x = :v $", "$", ":v $"),
        (Filter, "attribute_exists(é)", "é", "(é)"),
        (Filter, "attribute_exists(a-b)", "-", "a-b"),
        (Condition, "x = :v #", "#", ":v #"),
        (Projection, "x $", "$", "x $"),
        // P4 blank expressions are syntax errors at <EOF>
        (Condition, "   ", "<EOF>", "   "),
        (Filter, "   ", "<EOF>", "   "),
        (KeyCondition, "   ", "<EOF>", "   "),
        (Update, "   ", "<EOF>", "   "),
        (Projection, "   ", "<EOF>", "   "),
        // P5 trailing whitespace belongs to <EOF>
        (KeyCondition, "pk = ", "<EOF>", "= "),
        (KeyCondition, "pk =   ", "<EOF>", "=   "),
        // P6 syntax error before the reserved-word rule
        (Condition, "COMMENT = = :v", "=", "= = :v"),
        // P7, P8
        (Update, "SET x :v", ":v", "x :v"),
        (Condition, "attribute_exists(l[1.5])", ".", "1.5"),
        // P10: an empty argument list is a syntax error.
        (Condition, "attribute_not_exists()", ")", "()"),
    ] {
        assert_eq!(
            syntax_error(kind, expression),
            token_near(kind, token, near),
            "{kind} {expression:?}"
        );
    }
}

#[test]
fn given_empty_expression_when_validated_then_empty_text_per_kind() {
    for kind in [
        ExpressionKind::Condition,
        ExpressionKind::Filter,
        ExpressionKind::KeyCondition,
        ExpressionKind::Projection,
        ExpressionKind::Update,
    ] {
        assert_eq!(
            syntax_error(kind, ""),
            format!("Invalid {kind}: The expression can not be empty;")
        );
    }
}

#[test]
fn given_prefixed_caller_when_syntax_fails_then_validation_prefix_is_added() {
    assert_eq!(
        validate_expression_syntax("x = = :v", ExpressionKind::Condition, true).unwrap_err(),
        "1 validation error detected: Invalid ConditionExpression: Syntax error; token: \"=\", \
         near: \"= = :v\""
    );
}

#[test]
fn given_valid_expressions_when_validated_then_syntax_accepts_them() {
    use ExpressionKind::{Condition, Filter, KeyCondition, Projection, Update};
    for (kind, expression) in [
        (
            Condition,
            "attribute_not_exists(x) and attribute_not_exists(y)",
        ),
        (Condition, "((#a)) = :v"),
        (Condition, "NOT (a = :v OR b <> :w) AND c BETWEEN :x AND :y"),
        (Condition, "a IN (:a, :b, :c)"),
        (Condition, "size(#a.b[2]) >= :n"),
        (Condition, ":v < size(x)"),
        (Condition, "contains(tags, :tag)"),
        // Nested calls parse; the function rules report them.
        (Condition, "size(size(a)) > :v"),
        (Filter, "(#a) = :v"),
        (KeyCondition, "#pk = :pk AND begins_with(#sk, :prefix)"),
        (KeyCondition, "pk = :pk AND sk BETWEEN :lo AND :hi"),
        (Update, "set a = :v add n :one"),
        (
            Update,
            "SET a = if_not_exists(a, :zero) + :one, b = list_append(b, :l) REMOVE c[0], d.e \
             DELETE s :s",
        ),
        (Update, "SET a = (b)"),
        (Update, "SET a = b - :v"),
        (Projection, "a, b.c, #d[0].e"),
        (KeyCondition, "pk = :pk   "),
    ] {
        assert_eq!(
            validate_expression_syntax(expression, kind, false),
            Ok(()),
            "{kind} {expression:?}"
        );
    }
}
