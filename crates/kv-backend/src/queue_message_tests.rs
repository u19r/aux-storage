use queue_provider::{MESSAGE_ID_LEN, MessageId, QueueIncarnation};
use storage_types::ErrorClass;

use super::queue_message::*;

#[test]
fn given_repeated_message_key_when_rejected_then_internal_not_conflict() {
    let incarnation = QueueIncarnation::new();
    let id = MessageId::from_bytes([7; MESSAGE_ID_LEN]);
    let error = reject_repeated(vec![(incarnation, id), (incarnation, id)]).unwrap_err();
    assert_eq!(error.class(), ErrorClass::Internal);
    assert_eq!(error.attempt_kind(), None);
}
