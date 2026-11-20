//! Validated initial provider-scope SystemStream topology size.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES, StorageError, StorageResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "usize", into = "usize")]
pub struct InitialStreamLineages(usize);

impl JsonSchema for InitialStreamLineages {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "InitialStreamLineages".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "integer",
            "minimum": 1,
            "maximum": SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES,
        })
    }
}

impl InitialStreamLineages {
    pub const ONE: Self = Self(1);
    pub const FOUR: Self = Self(4);

    #[must_use]
    pub const fn get(self) -> usize {
        self.0
    }
}

impl TryFrom<usize> for InitialStreamLineages {
    type Error = StorageError;

    fn try_from(count: usize) -> StorageResult<Self> {
        if count == 0 || count > SYSTEM_STREAM_CHECKPOINT_MAX_ENTRIES {
            return Err(StorageError::validation(
                "initial_stream_lineages must be between 1 and 128",
            ));
        }
        Ok(Self(count))
    }
}

impl From<InitialStreamLineages> for usize {
    fn from(count: InitialStreamLineages) -> Self {
        count.0
    }
}
