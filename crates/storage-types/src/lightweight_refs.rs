use std::collections::HashMap;

use crate::{AttributeValue, KeyAttributes};

pub const DEFAULT_PK_NAME: &str = "pk";
pub const DEFAULT_SK_NAME: &str = "sk";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarValueRef<'a> {
    S(&'a str),
    N(&'a str),
    B(&'a str),
}

impl<'a> ScalarValueRef<'a> {
    #[must_use]
    pub fn as_str(self) -> &'a str {
        match self {
            Self::S(value) | Self::N(value) | Self::B(value) => value,
        }
    }

    #[must_use]
    pub fn to_owned_attribute_value(self) -> AttributeValue {
        match self {
            Self::S(value) => AttributeValue::S(value.to_string()),
            Self::N(value) => AttributeValue::N(value.to_string()),
            Self::B(value) => AttributeValue::B(value.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScalarValueOwned {
    S(String),
    N(String),
    B(String),
}

impl ScalarValueOwned {
    #[must_use]
    pub fn as_ref(&self) -> ScalarValueRef<'_> {
        match self {
            Self::S(value) => ScalarValueRef::S(value.as_str()),
            Self::N(value) => ScalarValueRef::N(value.as_str()),
            Self::B(value) => ScalarValueRef::B(value.as_str()),
        }
    }

    #[must_use]
    pub fn to_owned_attribute_value(self) -> AttributeValue {
        match self {
            Self::S(value) => AttributeValue::S(value),
            Self::N(value) => AttributeValue::N(value),
            Self::B(value) => AttributeValue::B(value),
        }
    }

    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self::S(value.into())
    }

    #[must_use]
    pub fn number(value: impl Into<String>) -> Self {
        Self::N(value.into())
    }

    #[must_use]
    pub fn binary(value: impl Into<String>) -> Self {
        Self::B(value.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::S(value) | Self::N(value) | Self::B(value) => value.as_str(),
        }
    }

    #[must_use]
    pub fn into_string(self) -> String {
        match self {
            Self::S(value) | Self::N(value) | Self::B(value) => value,
        }
    }
}

impl From<&str> for ScalarValueOwned {
    fn from(value: &str) -> Self {
        Self::S(value.to_string())
    }
}

impl From<String> for ScalarValueOwned {
    fn from(value: String) -> Self {
        Self::S(value)
    }
}

impl<'a> From<ScalarValueRef<'a>> for ScalarValueOwned {
    fn from(value: ScalarValueRef<'a>) -> Self {
        match value {
            ScalarValueRef::S(raw) => Self::S(raw.to_string()),
            ScalarValueRef::N(raw) => Self::N(raw.to_string()),
            ScalarValueRef::B(raw) => Self::B(raw.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionKey(pub ScalarValueOwned);

impl PartitionKey {
    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::string(value))
    }

    #[must_use]
    pub fn number(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::number(value))
    }

    #[must_use]
    pub fn binary(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::binary(value))
    }

    #[must_use]
    pub fn as_ref(&self) -> ScalarValueRef<'_> {
        self.0.as_ref()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn into_scalar(self) -> ScalarValueOwned {
        self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_string()
    }
}

impl From<String> for PartitionKey {
    fn from(value: String) -> Self {
        Self::string(value)
    }
}

impl From<&str> for PartitionKey {
    fn from(value: &str) -> Self {
        Self::string(value)
    }
}

impl From<ScalarValueOwned> for PartitionKey {
    fn from(value: ScalarValueOwned) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortKey(pub ScalarValueOwned);

impl SortKey {
    #[must_use]
    pub fn string(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::string(value))
    }

    #[must_use]
    pub fn number(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::number(value))
    }

    #[must_use]
    pub fn binary(value: impl Into<String>) -> Self {
        Self(ScalarValueOwned::binary(value))
    }

    #[must_use]
    pub fn as_ref(&self) -> ScalarValueRef<'_> {
        self.0.as_ref()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn into_scalar(self) -> ScalarValueOwned {
        self.0
    }

    #[must_use]
    pub fn into_string(self) -> String {
        self.0.into_string()
    }
}

impl From<String> for SortKey {
    fn from(value: String) -> Self {
        Self::string(value)
    }
}

impl From<&str> for SortKey {
    fn from(value: &str) -> Self {
        Self::string(value)
    }
}

impl From<ScalarValueOwned> for SortKey {
    fn from(value: ScalarValueOwned) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntityKey {
    pub pk_name: String,
    pub pk: PartitionKey,
    pub sk_name: Option<String>,
    pub sk: Option<SortKey>,
}

impl EntityKey {
    #[must_use]
    pub fn new(
        pk_name: impl Into<String>,
        pk: PartitionKey,
        sk_name: Option<String>,
        sk: Option<SortKey>,
    ) -> Self {
        Self {
            pk_name: pk_name.into(),
            pk,
            sk_name,
            sk,
        }
    }

    #[must_use]
    pub fn pk_sk(pk: impl Into<PartitionKey>, sk: impl Into<SortKey>) -> Self {
        Self {
            pk_name: DEFAULT_PK_NAME.to_string(),
            pk: pk.into(),
            sk_name: Some(DEFAULT_SK_NAME.to_string()),
            sk: Some(sk.into()),
        }
    }

    #[must_use]
    pub fn as_ref(&self) -> KeyRef<'_> {
        KeyRef {
            pk_name: self.pk_name.as_str(),
            pk: self.pk.as_ref(),
            sk_name: self.sk_name.as_deref(),
            sk: self.sk.as_ref().map(SortKey::as_ref),
        }
    }

    #[must_use]
    pub fn to_map(&self) -> HashMap<String, AttributeValue> {
        self.as_ref().to_map()
    }

    #[must_use]
    pub fn into_map(self) -> HashMap<String, AttributeValue> {
        KeyOwned::from(self).into_map()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyRef<'a> {
    pub pk_name: &'a str,
    pub pk: ScalarValueRef<'a>,
    pub sk_name: Option<&'a str>,
    pub sk: Option<ScalarValueRef<'a>>,
}

impl<'a> KeyRef<'a> {
    #[must_use]
    pub fn new(
        pk_name: &'a str,
        pk: ScalarValueRef<'a>,
        sk_name: Option<&'a str>,
        sk: Option<ScalarValueRef<'a>>,
    ) -> Self {
        Self {
            pk_name,
            pk,
            sk_name,
            sk,
        }
    }

    #[must_use]
    pub fn pk_sk(pk: ScalarValueRef<'a>, sk: ScalarValueRef<'a>) -> Self {
        Self {
            pk_name: DEFAULT_PK_NAME,
            pk,
            sk_name: Some(DEFAULT_SK_NAME),
            sk: Some(sk),
        }
    }

    #[must_use]
    pub fn to_map(self) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::with_capacity(if self.sk.is_some() { 2 } else { 1 });
        key.insert(self.pk_name.to_string(), self.pk.to_owned_attribute_value());

        if let (Some(sk_name), Some(sk)) = (self.sk_name, self.sk) {
            key.insert(sk_name.to_string(), sk.to_owned_attribute_value());
        }

        key
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOwned {
    pub pk_name: String,
    pub pk: ScalarValueOwned,
    pub sk_name: Option<String>,
    pub sk: Option<ScalarValueOwned>,
}

impl KeyOwned {
    #[must_use]
    pub fn new(
        pk_name: impl Into<String>,
        pk: ScalarValueOwned,
        sk_name: Option<String>,
        sk: Option<ScalarValueOwned>,
    ) -> Self {
        Self {
            pk_name: pk_name.into(),
            pk,
            sk_name,
            sk,
        }
    }

    #[must_use]
    pub fn pk_sk(pk: impl Into<String>, sk: impl Into<String>) -> Self {
        Self {
            pk_name: DEFAULT_PK_NAME.to_string(),
            pk: ScalarValueOwned::string(pk),
            sk_name: Some(DEFAULT_SK_NAME.to_string()),
            sk: Some(ScalarValueOwned::string(sk)),
        }
    }

    #[must_use]
    pub fn pk_sk_scalar(pk: ScalarValueOwned, sk: ScalarValueOwned) -> Self {
        Self {
            pk_name: DEFAULT_PK_NAME.to_string(),
            pk,
            sk_name: Some(DEFAULT_SK_NAME.to_string()),
            sk: Some(sk),
        }
    }

    #[must_use]
    pub fn as_ref(&self) -> KeyRef<'_> {
        KeyRef {
            pk_name: self.pk_name.as_str(),
            pk: self.pk.as_ref(),
            sk_name: self.sk_name.as_deref(),
            sk: self.sk.as_ref().map(ScalarValueOwned::as_ref),
        }
    }

    #[must_use]
    pub fn to_map(&self) -> HashMap<String, AttributeValue> {
        self.as_ref().to_map()
    }

    #[must_use]
    pub fn into_map(self) -> HashMap<String, AttributeValue> {
        let mut key = HashMap::with_capacity(if self.sk.is_some() { 2 } else { 1 });
        key.insert(self.pk_name, self.pk.to_owned_attribute_value());

        if let (Some(sk_name), Some(sk)) = (self.sk_name, self.sk) {
            key.insert(sk_name, sk.to_owned_attribute_value());
        }

        key
    }
}

impl From<KeyOwned> for HashMap<String, AttributeValue> {
    fn from(value: KeyOwned) -> Self {
        value.into_map()
    }
}

impl From<&KeyOwned> for HashMap<String, AttributeValue> {
    fn from(value: &KeyOwned) -> Self {
        value.to_map()
    }
}

impl From<KeyOwned> for KeyAttributes {
    fn from(value: KeyOwned) -> Self {
        let mut key = KeyAttributes::with_capacity(if value.sk.is_some() { 2 } else { 1 });
        key.insert(value.pk_name, value.pk.to_owned_attribute_value());

        if let (Some(sk_name), Some(sk)) = (value.sk_name, value.sk) {
            key.insert(sk_name, sk.to_owned_attribute_value());
        }

        key
    }
}

impl From<&KeyOwned> for KeyAttributes {
    fn from(value: &KeyOwned) -> Self {
        let mut key = KeyAttributes::with_capacity(if value.sk.is_some() { 2 } else { 1 });
        key.insert(
            value.pk_name.clone(),
            value.pk.clone().to_owned_attribute_value(),
        );

        if let (Some(sk_name), Some(sk)) = (&value.sk_name, &value.sk) {
            key.insert(sk_name.clone(), sk.clone().to_owned_attribute_value());
        }

        key
    }
}

impl From<EntityKey> for HashMap<String, AttributeValue> {
    fn from(value: EntityKey) -> Self {
        value.into_map()
    }
}

impl From<&EntityKey> for HashMap<String, AttributeValue> {
    fn from(value: &EntityKey) -> Self {
        value.to_map()
    }
}

impl<'a> From<KeyRef<'a>> for KeyOwned {
    fn from(value: KeyRef<'a>) -> Self {
        Self {
            pk_name: value.pk_name.to_string(),
            pk: value.pk.into(),
            sk_name: value.sk_name.map(str::to_string),
            sk: value.sk.map(ScalarValueOwned::from),
        }
    }
}

impl From<EntityKey> for KeyOwned {
    fn from(value: EntityKey) -> Self {
        Self {
            pk_name: value.pk_name,
            pk: value.pk.into_scalar(),
            sk_name: value.sk_name,
            sk: value.sk.map(SortKey::into_scalar),
        }
    }
}

impl From<KeyOwned> for EntityKey {
    fn from(value: KeyOwned) -> Self {
        Self {
            pk_name: value.pk_name,
            pk: PartitionKey(value.pk),
            sk_name: value.sk_name,
            sk: value.sk.map(SortKey),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AttributeValueRef<'a> {
    S(&'a str),
    N(&'a str),
    B(&'a str),
    Bool(bool),
    Null(bool),
    Borrowed(&'a AttributeValue),
}

impl AttributeValueRef<'_> {
    #[must_use]
    pub fn to_owned_attribute_value(self) -> AttributeValue {
        match self {
            Self::S(value) => AttributeValue::S(value.to_string()),
            Self::N(value) => AttributeValue::N(value.to_string()),
            Self::B(value) => AttributeValue::B(value.to_string()),
            Self::Bool(value) => AttributeValue::BOOL(value),
            Self::Null(value) => AttributeValue::NULL(value),
            Self::Borrowed(value) => value.clone(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExprNameRef<'a> {
    pub placeholder: &'a str,
    pub name: &'a str,
}

impl<'a> ExprNameRef<'a> {
    #[must_use]
    pub fn new(placeholder: &'a str, name: &'a str) -> Self {
        Self { placeholder, name }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExprValueRef<'a> {
    pub placeholder: &'a str,
    pub value: AttributeValueRef<'a>,
}

impl<'a> ExprValueRef<'a> {
    #[must_use]
    pub fn new(placeholder: &'a str, value: AttributeValueRef<'a>) -> Self {
        Self { placeholder, value }
    }
}

#[must_use]
pub fn expr_names_to_map(values: &[ExprNameRef<'_>]) -> HashMap<String, String> {
    let mut out = HashMap::with_capacity(values.len());
    for pair in values {
        out.insert(pair.placeholder.to_string(), pair.name.to_string());
    }
    out
}

#[must_use]
pub fn expr_values_to_map(values: &[ExprValueRef<'_>]) -> HashMap<String, AttributeValue> {
    let mut out = HashMap::with_capacity(values.len());
    for pair in values {
        out.insert(
            pair.placeholder.to_string(),
            pair.value.to_owned_attribute_value(),
        );
    }
    out
}
