use std::ops::Deref;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserStreamName(String);

impl UserStreamName {
    #[must_use]
    pub fn new(name: &str) -> Self {
        Self(name.to_string())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for UserStreamName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<String> for UserStreamName {
    fn from(value: String) -> Self {
        UserStreamName(value)
    }
}

impl From<&str> for UserStreamName {
    fn from(value: &str) -> Self {
        UserStreamName(value.to_string())
    }
}
