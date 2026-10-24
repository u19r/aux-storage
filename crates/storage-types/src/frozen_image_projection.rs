use serde::{Deserialize, Serialize};

use crate::{AttributeValue, StorageError, StorageResult, WireItem};

/// A projection bound to one source version, independent of mutable schema.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FrozenImageProjection {
    All,
    Fields(FrozenProjectionFields),
}

/// Ordered unique fields, including an empty selection. Table and index key
/// inclusion belongs to the GSI owner.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>", into = "Vec<String>")]
pub struct FrozenProjectionFields(Vec<String>);

impl TryFrom<Vec<String>> for FrozenProjectionFields {
    type Error = StorageError;

    fn try_from(fields: Vec<String>) -> StorageResult<Self> {
        if fields.len() > crate::constants::MAX_FROZEN_PROJECTION_FIELDS
            || fields.iter().enumerate().any(|(ordinal, field)| {
                fields[..ordinal].contains(field)
                    || crate::dynamodb_limits::validate_attribute_name(field, || {
                        "frozen projection field".to_owned()
                    })
                    .is_err()
            })
        {
            return Err(StorageError::internal(
                "invalid frozen image projection fields",
            ));
        }
        Ok(Self(fields))
    }
}

impl From<FrozenProjectionFields> for Vec<String> {
    fn from(fields: FrozenProjectionFields) -> Self {
        fields.0
    }
}

impl FrozenImageProjection {
    /// ALL has no finite field observation; its writes remain unconditional.
    pub fn select_values(
        &self,
        source: &WireItem,
    ) -> StorageResult<Option<Vec<Option<AttributeValue>>>> {
        let Self::Fields(fields) = self else {
            return Ok(None);
        };
        let fields = fields.0.iter().map(String::as_str).collect::<Vec<_>>();
        source.select_attribute_values(&fields).map(Some)
    }

    pub fn project(&self, source: &WireItem) -> StorageResult<WireItem> {
        match self {
            Self::All => Ok(source.clone()),
            Self::Fields(fields) => {
                let fields = fields.0.iter().map(String::as_str).collect::<Vec<_>>();
                source.project_wire_attributes(&fields)
            }
        }
    }
}

impl AsRef<[String]> for FrozenProjectionFields {
    fn as_ref(&self) -> &[String] {
        &self.0
    }
}
