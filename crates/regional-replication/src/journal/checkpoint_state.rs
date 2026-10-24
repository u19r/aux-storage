//! Fixed-width native state tags keep terminal hold settlement storage-neutral.
use serde::{Deserialize, Deserializer, Serializer};

use crate::history::checkpoint::CanonicalCheckpointState;

pub(super) fn serialize<S: Serializer>(
    state: &CanonicalCheckpointState,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    let tag = match state {
        CanonicalCheckpointState::Held => 0,
        CanonicalCheckpointState::Released => 1,
        CanonicalCheckpointState::Expired => 2,
    };
    serializer.serialize_u8(tag)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<CanonicalCheckpointState, D::Error> {
    match u8::deserialize(deserializer)? {
        0 => Ok(CanonicalCheckpointState::Held),
        1 => Ok(CanonicalCheckpointState::Released),
        2 => Ok(CanonicalCheckpointState::Expired),
        _ => Err(serde::de::Error::custom("Unknown native checkpoint state")),
    }
}
