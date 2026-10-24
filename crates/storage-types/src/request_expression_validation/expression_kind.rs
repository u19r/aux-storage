use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExpressionKind {
    Condition,
    Filter,
    KeyCondition,
    Projection,
    Update,
}

impl fmt::Display for ExpressionKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Condition => "ConditionExpression",
            Self::Filter => "FilterExpression",
            Self::KeyCondition => "KeyConditionExpression",
            Self::Projection => "ProjectionExpression",
            Self::Update => "UpdateExpression",
        })
    }
}
