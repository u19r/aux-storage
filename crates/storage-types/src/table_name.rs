use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{errors::TableNameParseError, index_name::IndexName};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct TableName(String);

impl TableName {
    pub fn new(name: &(impl ToString + ?Sized)) -> Self {
        TableName(name.to_string())
    }

    #[must_use]
    pub fn dynamodb_resource_name(&self) -> &str {
        self.0
            .strip_prefix("arn:")
            .and_then(|_| self.0.split_once(":table/").map(|(_, name)| name))
            .and_then(|name| name.split('/').next())
            .filter(|name| !name.is_empty())
            .unwrap_or(&self.0)
    }

    /// Returns a sanitized version of the table name, keeping only characters
    /// that match the pattern [a-zA-Z0-9_.-]+
    #[must_use]
    pub fn sanitized_name(&self) -> String {
        if self
            .0
            .as_bytes()
            .iter()
            .all(|byte| is_sanitized_table_name_byte(*byte))
        {
            return self.0.clone();
        }

        let mut sanitized = String::with_capacity(self.0.len());
        for byte in self.0.as_bytes() {
            if is_sanitized_table_name_byte(*byte) {
                sanitized.push(char::from(*byte));
            }
        }
        sanitized
    }

    /// Physical table name shared by the SQL backends (`table_<sanitized>`).
    /// Postgres applies its 63-byte truncation hook on top of this.
    #[must_use]
    pub fn physical_table_name(&self) -> String {
        format!("table_{}", self.sanitized_name())
    }

    /// Physical GSI table name shared by the SQL backends
    /// (`gsi_<table>_<index>`). Postgres applies its truncation hook on top.
    #[must_use]
    pub fn physical_gsi_table_name(&self, index: &IndexName) -> String {
        format!("gsi_{}_{}", self.sanitized_name(), index.sanitized_name())
    }

    /// Physical TTL index table name shared by the SQL backends.
    #[must_use]
    pub fn physical_ttl_index_table_name(&self) -> String {
        format!("ttl_index_{}", self.sanitized_name())
    }
}

impl TryFrom<String> for TableName {
    type Error = TableNameParseError;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        if name.trim().is_empty() {
            return Err(TableNameParseError::Empty);
        }
        Ok(Self(name))
    }
}

impl TryFrom<&str> for TableName {
    type Error = TableNameParseError;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Self::try_from(name.to_owned())
    }
}

fn is_sanitized_table_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-')
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
impl AsRef<str> for TableName {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
