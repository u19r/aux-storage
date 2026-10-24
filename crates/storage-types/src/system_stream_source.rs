use std::fmt;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    SystemStreamContractError, TableName, constants::SYSTEM_STREAM_SOURCE_TABLE_NAME_MAX_BYTES,
};

/// A database-owned logical table identity carried by a SystemStream record.
///
/// This is the source table's stable logical name, not a physical table,
/// partition, pointer, or provider key.  Its restricted grammar matches the
/// DynamoDB table-name character set so an ARN or provider path cannot cross
/// the public SystemStream boundary by accident.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, ToSchema)]
#[schema(value_type = String)]
#[serde(try_from = "String", into = "String")]
pub struct SystemStreamSourceTableName(String);

impl SystemStreamSourceTableName {
    /// Validate and create a stable logical source table identity.
    pub fn try_new(value: impl Into<String>) -> Result<Self, SystemStreamContractError> {
        let value = value.into();
        if value.is_empty()
            || value.trim() != value
            || value.len() > SYSTEM_STREAM_SOURCE_TABLE_NAME_MAX_BYTES
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
        {
            return Err(SystemStreamContractError::InvalidSourceTableName);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for SystemStreamSourceTableName {
    type Error = SystemStreamContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<&str> for SystemStreamSourceTableName {
    type Error = SystemStreamContractError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl TryFrom<&TableName> for SystemStreamSourceTableName {
    type Error = SystemStreamContractError;

    fn try_from(value: &TableName) -> Result<Self, Self::Error> {
        Self::try_new(value.as_ref())
    }
}

impl From<SystemStreamSourceTableName> for String {
    fn from(value: SystemStreamSourceTableName) -> Self {
        value.0
    }
}

impl fmt::Display for SystemStreamSourceTableName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for SystemStreamSourceTableName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}
