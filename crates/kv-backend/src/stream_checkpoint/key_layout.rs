//! Typed addresses for logical checkpoint state.

use storage_types::SystemStreamCheckpointToken;

use crate::constants::{
    SYSTEM_STREAM_CHECKPOINT_OPERATION_PREFIX, SYSTEM_STREAM_CHECKPOINT_TOKEN_PREFIX,
};

pub(crate) fn token(token: &SystemStreamCheckpointToken) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_CHECKPOINT_TOKEN_PREFIX.to_vec();
    key.extend_from_slice(token.as_str().as_bytes());
    key
}

pub(super) fn operation(operation_key: &str) -> Vec<u8> {
    let mut key = SYSTEM_STREAM_CHECKPOINT_OPERATION_PREFIX.to_vec();
    key.extend_from_slice(
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, operation_key.as_bytes()).as_bytes(),
    );
    key
}
