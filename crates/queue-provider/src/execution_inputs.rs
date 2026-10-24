use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use storage_types::{DurationSeconds, TimestampMillis};
use uuid::Uuid;

use crate::{
    ChangeMessageVisibilityRequest, QueueError, QueueResult, QueueValidationKind,
    ReceiveMessageRequest, request_validation::QueueRequestValidation,
};

/// Captured inputs for one deterministic ReceiveMessage application.
///
/// The request supplies queue semantics. The timestamp and UUIDs are the
/// only nondeterministic values captured before a command is applied; message
/// selection and leasing remain state-dependent database work.
#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReceiveMessagesInput {
    request: ReceiveMessageRequest,
    captured_at: TimestampMillis,
    receipt_handle_entropy: Vec<Uuid>,
}

impl ReceiveMessagesInput {
    /// Construct deterministic receive inputs from already captured values.
    pub fn try_new(
        request: ReceiveMessageRequest,
        captured_at: TimestampMillis,
        receipt_handle_entropy: Vec<Uuid>,
    ) -> QueueResult<Self> {
        let input = Self {
            request,
            captured_at,
            receipt_handle_entropy,
        };
        input.validate()?;
        Ok(input)
    }

    pub fn validate(&self) -> QueueResult<()> {
        validate_receive_request(&self.request)?;
        self.queue_name()?;
        let max_messages = self.max_messages()?;
        let _ = self.visibility_timeout()?;
        self.validate_receipt_handle_entropy(max_messages)
    }

    #[must_use]
    pub const fn request(&self) -> &ReceiveMessageRequest {
        &self.request
    }

    #[must_use]
    pub const fn captured_at(&self) -> TimestampMillis {
        self.captured_at
    }

    #[must_use]
    pub fn receipt_handle_entropy(&self) -> &[Uuid] {
        &self.receipt_handle_entropy
    }

    pub fn queue_name(&self) -> QueueResult<&str> {
        self.request
            .queue_url
            .split('/')
            .next_back()
            .ok_or_else(|| QueueError::validation(QueueValidationKind::InvalidQueueUrlFormat))
    }

    pub fn max_messages(&self) -> QueueResult<u32> {
        self.request.max_number_of_messages.ok_or_else(|| {
            QueueError::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                "receive message input is missing max messages",
            )
        })
    }

    pub fn visibility_timeout(&self) -> QueueResult<DurationSeconds> {
        self.request
            .visibility_timeout
            .map(Into::into)
            .ok_or_else(|| {
                QueueError::validation_with_detail(
                    QueueValidationKind::InvalidParameterValue,
                    "receive message input is missing visibility timeout",
                )
            })
    }

    fn validate_receipt_handle_entropy(&self, max_messages: u32) -> QueueResult<()> {
        if self.receipt_handle_entropy.len() != max_messages as usize {
            return Err(QueueError::validation_with_detail(
                QueueValidationKind::InvalidParameterValue,
                "receive message receipt-handle entropy supply must equal max messages",
            ));
        }
        for (index, entropy) in self.receipt_handle_entropy.iter().enumerate() {
            if self.receipt_handle_entropy[..index]
                .iter()
                .any(|previous| previous == entropy)
            {
                return Err(QueueError::validation_with_detail(
                    QueueValidationKind::InvalidParameterValue,
                    "receive message receipt-handle entropy supply contains duplicates",
                ));
            }
        }
        Ok(())
    }
}

impl<'de> Deserialize<'de> for ReceiveMessagesInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireInput {
            request: ReceiveMessageRequest,
            captured_at: TimestampMillis,
            receipt_handle_entropy: Vec<Uuid>,
        }

        let wire = WireInput::deserialize(deserializer)?;
        Self::try_new(wire.request, wire.captured_at, wire.receipt_handle_entropy)
            .map_err(D::Error::custom)
    }
}

/// Captured inputs for one deterministic ChangeMessageVisibility application.
#[derive(Debug, Clone, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeMessageVisibilityInput {
    request: ChangeMessageVisibilityRequest,
    captured_at: TimestampMillis,
}

impl ChangeMessageVisibilityInput {
    pub fn try_new(
        request: ChangeMessageVisibilityRequest,
        captured_at: TimestampMillis,
    ) -> QueueResult<Self> {
        request.validate_request()?;
        Ok(Self {
            request,
            captured_at,
        })
    }

    #[must_use]
    pub const fn request(&self) -> &ChangeMessageVisibilityRequest {
        &self.request
    }

    #[must_use]
    pub const fn captured_at(&self) -> TimestampMillis {
        self.captured_at
    }

    #[must_use]
    pub fn new_visibility_timestamp(&self) -> TimestampMillis {
        self.captured_at
            .add_duration_seconds(&self.request.visibility_timeout.into())
    }
}

impl<'de> Deserialize<'de> for ChangeMessageVisibilityInput {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireInput {
            request: ChangeMessageVisibilityRequest,
            captured_at: TimestampMillis,
        }

        let wire = WireInput::deserialize(deserializer)?;
        Self::try_new(wire.request, wire.captured_at).map_err(D::Error::custom)
    }
}

fn validate_receive_request(request: &ReceiveMessageRequest) -> QueueResult<()> {
    request.validate_request().map_err(|error| {
        let kind = if request.queue_url.is_empty() {
            QueueValidationKind::InvalidQueueUrlFormat
        } else {
            QueueValidationKind::InvalidParameterValue
        };
        QueueError::validation_with_detail(kind, error)
    })
}
