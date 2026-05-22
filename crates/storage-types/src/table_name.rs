use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

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
