//! Physical revision metadata. Replication policy belongs above this value.

use std::{fmt, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use crate::{StorageError, StorageResult};

/// Keeps existing stored metadata intact without interpreting a legacy winner.
/// Ordinary writes use the allocation-free local representation.
#[derive(Clone, Default)]
pub struct ItemProvenance(Option<Arc<RawValue>>);

impl ItemProvenance {
    pub const LOCAL_BYTES: &'static [u8] = br#"{"kind":"unreplicated"}"#;

    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        if bytes == Self::LOCAL_BYTES {
            return Ok(Self::default());
        }
        let raw: Box<RawValue> = serde_json::from_slice(bytes)
            .map_err(|_| StorageError::internal("invalid stored item provenance"))?;
        if raw.get().as_bytes() != bytes {
            return Err(StorageError::internal(
                "invalid stored item provenance envelope",
            ));
        }
        Self::parse_envelope(raw)
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        self.0
            .as_ref()
            .map_or(Self::LOCAL_BYTES, |value| value.get().as_bytes())
    }

    fn parse_envelope(raw: Box<RawValue>) -> StorageResult<Self> {
        if !raw.get().starts_with('{') || u32::try_from(raw.get().len()).is_err() {
            return Err(StorageError::internal(
                "invalid stored item provenance envelope",
            ));
        }
        if raw.get().as_bytes() == Self::LOCAL_BYTES {
            return Ok(Self::default());
        }
        Ok(Self(Some(Arc::from(raw))))
    }
}

impl fmt::Debug for ItemProvenance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ItemProvenance(<redacted>)")
    }
}

impl PartialEq for ItemProvenance {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for ItemProvenance {}

impl Serialize for ItemProvenance {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if let Some(raw) = &self.0 {
            raw.serialize(serializer)
        } else {
            #[derive(Serialize)]
            struct Local {
                kind: &'static str,
            }
            Local {
                kind: "unreplicated",
            }
            .serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for ItemProvenance {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Self::parse_envelope(raw).map_err(serde::de::Error::custom)
    }
}
