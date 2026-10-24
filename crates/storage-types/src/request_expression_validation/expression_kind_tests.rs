use super::{ExpressionKind, validate_expression_set};
use crate::{AttributeMap, AttributeValue};

#[test]
fn given_each_expression_kind_when_reserved_word_is_used_then_preserve_public_error_name() {
    let values = AttributeMap::from_iter([(":v".to_owned(), AttributeValue::N("1".to_owned()))]);
    let cases = [
        (
            ExpressionKind::Condition,
            "comment = :v",
            "Invalid ConditionExpression: Attribute name is a reserved keyword; reserved keyword: \
             comment",
        ),
        (
            ExpressionKind::Filter,
            "comment = :v",
            "Invalid FilterExpression: Attribute name is a reserved keyword; reserved keyword: \
             comment",
        ),
        (
            ExpressionKind::KeyCondition,
            "comment = :v",
            "Invalid KeyConditionExpression: Attribute name is a reserved keyword; reserved \
             keyword: comment",
        ),
        (
            ExpressionKind::Projection,
            "comment",
            "Invalid ProjectionExpression: Attribute name is a reserved keyword; reserved \
             keyword: comment",
        ),
        (
            ExpressionKind::Update,
            "SET comment = :v",
            "Invalid UpdateExpression: Attribute name is a reserved keyword; reserved keyword: \
             comment",
        ),
    ];
    for (kind, expression, expected) in cases {
        let error = validate_expression_set([(Some(expression), kind)], None, Some(&values), false)
            .unwrap_err();
        assert_eq!(error, expected);
    }
}
