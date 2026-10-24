use crate::{StorageError, StorageResult};

/// String with its non-emptiness proven at construction.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NonEmptyString(String);

impl NonEmptyString {
    pub fn try_new(value: String) -> StorageResult<Self> {
        if value.is_empty() {
            return Err(StorageError::validation("Value cannot be empty"));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl AsRef<str> for NonEmptyString {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NonEmptyString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&NonEmptyString> for String {
    fn from(value: &NonEmptyString) -> Self {
        value.0.clone()
    }
}
