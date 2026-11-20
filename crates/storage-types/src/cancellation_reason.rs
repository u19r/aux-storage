use std::fmt;

use crate::{AttributeMap, DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE};

/// `CancellationReasons[].Code` of a DynamoDB `TransactionCanceledException`:
/// the seven codes AWS documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationCode {
    None,
    ConditionalCheckFailed,
    ItemCollectionSizeLimitExceeded,
    TransactionConflict,
    ProvisionedThroughputExceeded,
    ThrottlingError,
    ValidationError,
}

impl CancellationCode {
    /// The wire text of the code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "None",
            Self::ConditionalCheckFailed => "ConditionalCheckFailed",
            Self::ItemCollectionSizeLimitExceeded => "ItemCollectionSizeLimitExceeded",
            Self::TransactionConflict => "TransactionConflict",
            Self::ProvisionedThroughputExceeded => "ProvisionedThroughputExceeded",
            Self::ThrottlingError => "ThrottlingError",
            Self::ValidationError => "ValidationError",
        }
    }

    /// The code for an exact wire text; `None` for any other text.
    #[must_use]
    pub fn parse(code: &str) -> Option<Self> {
        Some(match code {
            "None" => Self::None,
            "ConditionalCheckFailed" => Self::ConditionalCheckFailed,
            "ItemCollectionSizeLimitExceeded" => Self::ItemCollectionSizeLimitExceeded,
            "TransactionConflict" => Self::TransactionConflict,
            "ProvisionedThroughputExceeded" => Self::ProvisionedThroughputExceeded,
            "ThrottlingError" => Self::ThrottlingError,
            "ValidationError" => Self::ValidationError,
            _ => return None,
        })
    }

    /// The message a reason of this code carries when its producer gives none.
    const fn default_message(self) -> Option<&'static str> {
        match self {
            Self::ConditionalCheckFailed => Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE),
            Self::ValidationError => Some(""),
            Self::None
            | Self::ItemCollectionSizeLimitExceeded
            | Self::TransactionConflict
            | Self::ProvisionedThroughputExceeded
            | Self::ThrottlingError => None,
        }
    }
}

/// One member's entry in `TransactionCanceledException.CancellationReasons`.
/// `message` and `item` are exactly what the wire carries.
#[derive(Clone, PartialEq)]
pub struct CancellationReason {
    pub code: CancellationCode,
    pub message: Option<String>,
    pub item: Option<AttributeMap>,
}

/// Never prints the item: error values reach log fields.
impl fmt::Debug for CancellationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let item = self
            .item
            .as_ref()
            .map(|item| format!("<{} attributes>", item.len()));
        f.debug_struct("CancellationReason")
            .field("code", &self.code)
            .field("message", &self.message)
            .field("item", &item.as_deref().map(RedactedItem))
            .finish()
    }
}

struct RedactedItem<'a>(&'a str);

impl fmt::Debug for RedactedItem<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

impl CancellationReason {
    /// A member that did not fail.
    #[must_use]
    pub const fn none() -> Self {
        Self {
            code: CancellationCode::None,
            message: None,
            item: None,
        }
    }

    /// A failed condition; `item` is the old item when `ALL_OLD` was requested
    /// and the item existed.
    #[must_use]
    pub fn conditional_check_failed(item: Option<AttributeMap>) -> Self {
        Self {
            code: CancellationCode::ConditionalCheckFailed,
            message: Some(DYNAMODB_CONDITIONAL_CHECK_FAILED_MESSAGE.to_string()),
            item,
        }
    }

    #[must_use]
    pub fn validation_error(message: impl Into<String>) -> Self {
        Self {
            code: CancellationCode::ValidationError,
            message: Some(message.into()),
            item: None,
        }
    }

    /// A reason decoded from a wire body; an absent message takes the code's
    /// default.
    #[must_use]
    pub fn from_parts(
        code: CancellationCode,
        message: Option<String>,
        item: Option<AttributeMap>,
    ) -> Self {
        let message = message.or_else(|| code.default_message().map(str::to_string));
        Self {
            code,
            message,
            item,
        }
    }

    #[must_use]
    pub fn is_conditional_check_failed(&self) -> bool {
        self.code == CancellationCode::ConditionalCheckFailed
    }
}
