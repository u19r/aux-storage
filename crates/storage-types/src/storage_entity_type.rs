use std::{fmt, str::FromStr};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, schemars::JsonSchema, utoipa::ToSchema,
)]
#[serde(transparent)]
pub struct StorageEntityType(&'static str);

impl StorageEntityType {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Option<Self> {
        let value = value.into();
        if is_valid_entity_type(value.as_str()) {
            Some(Self(Box::leak(value.into_boxed_str())))
        } else {
            None
        }
    }

    #[must_use]
    pub const fn from_static(value: &'static str) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_str(&self) -> &'static str {
        self.0
    }

    #[must_use]
    pub fn as_static_str(&self) -> &'static str {
        self.as_str()
    }

    #[must_use]
    pub fn as_db_code(&self) -> &'static str {
        self.as_str()
    }

    #[must_use]
    pub fn parse_db(value: &str) -> Option<Self> {
        Self::new(value)
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::parse_db(value)
    }
}

fn is_valid_entity_type(value: &str) -> bool {
    let Some(first) = value.as_bytes().first() else {
        return false;
    };
    if !first.is_ascii_uppercase() {
        return false;
    }
    value
        .bytes()
        .all(|value| value.is_ascii_uppercase() || value.is_ascii_digit() || value == b'_')
}

impl fmt::Display for StorageEntityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl serde::Serialize for StorageEntityType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> serde::Deserialize<'de> for StorageEntityType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let value = String::deserialize(deserializer)?;
        Self::new(value).ok_or_else(|| serde::de::Error::custom("invalid storage entity type"))
    }
}

impl From<StorageEntityType> for String {
    fn from(value: StorageEntityType) -> Self {
        value.0.to_string()
    }
}

impl FromStr for StorageEntityType {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value).ok_or(())
    }
}
