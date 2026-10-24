//! Byte-key templates shared by every backend.
//!
//! A template is either a literal key or a prefix/suffix pair around a
//! placeholder binding. Placeholders materialize to deterministic fallback
//! bytes outside their owning transaction; the owner replaces them with
//! coordinated values at commit time. Lifted from the KV provider with the
//! backend-specific `rocks_key` name generalized.
use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum KeyTemplate {
    Literal(Vec<u8>),
    Placeholder(PlaceholderTemplate),
    UniquePlaceholder(PlaceholderTemplate),
}

impl KeyTemplate {
    #[must_use]
    pub fn literal(key: Vec<u8>) -> Self {
        Self::Literal(key)
    }

    #[must_use]
    pub fn placeholder(prefix: Vec<u8>, suffix: Vec<u8>, binding: PlaceholderBinding) -> Self {
        Self::Placeholder(PlaceholderTemplate {
            prefix,
            suffix,
            binding,
        })
    }

    /// Builds a placeholder whose generated key is unique to its transaction.
    #[must_use]
    pub fn unique_placeholder(
        prefix: Vec<u8>,
        suffix: Vec<u8>,
        binding: UniquePlaceholderBinding,
    ) -> Self {
        Self::UniquePlaceholder(PlaceholderTemplate {
            prefix,
            suffix,
            binding: binding.into_binding(),
        })
    }

    #[must_use]
    pub fn materialized_key(&self) -> Vec<u8> {
        match self {
            Self::Literal(key) => key.clone(),
            Self::Placeholder(template) | Self::UniquePlaceholder(template) => {
                template.materialize_fallback()
            }
        }
    }

    #[must_use]
    pub fn materialized_key_with_fallback(&self, fallback_value: &[u8]) -> Vec<u8> {
        match self {
            Self::Literal(key) => key.clone(),
            Self::Placeholder(template) | Self::UniquePlaceholder(template) => {
                template.materialize_with_fallback(fallback_value)
            }
        }
    }

    #[must_use]
    pub fn with_replaced_prefix(&self, prefix: Vec<u8>) -> Self {
        match self {
            Self::Literal(key) => Self::Literal(key.clone()),
            Self::Placeholder(template) => Self::Placeholder(template.with_replaced_prefix(prefix)),
            Self::UniquePlaceholder(template) => {
                Self::UniquePlaceholder(template.with_replaced_prefix(prefix))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlaceholderTemplate {
    pub prefix: Vec<u8>,
    pub suffix: Vec<u8>,
    pub binding: PlaceholderBinding,
}

impl PlaceholderTemplate {
    #[must_use]
    fn with_replaced_prefix(&self, prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            suffix: self.suffix.clone(),
            binding: self.binding.clone(),
        }
    }

    fn materialize_fallback(&self) -> Vec<u8> {
        self.materialize_with_fallback(&self.binding.fallback_value)
    }

    fn materialize_with_fallback(&self, fallback_value: &[u8]) -> Vec<u8> {
        let mut bytes = self.prefix.clone();
        bytes.extend_from_slice(fallback_value);
        bytes.extend_from_slice(&self.suffix);
        bytes
    }
}

#[derive(Clone, Debug)]
pub struct PlaceholderBinding {
    pub id: PlaceholderId,
    pub fallback_value: Vec<u8>,
    pub user_bytes: [u8; 2],
}

impl PlaceholderBinding {
    #[must_use]
    pub fn new(id: PlaceholderId, fallback_value: Vec<u8>, user_bytes: [u8; 2]) -> Self {
        Self {
            id,
            fallback_value,
            user_bytes,
        }
    }

    #[must_use]
    pub fn shared(id: u16, fallback_value: Vec<u8>) -> Self {
        Self::new(
            PlaceholderId::Shared(id),
            fallback_value,
            random_user_bytes(),
        )
    }
}

/// Binding for a generated key that does not coordinate updates to an existing
/// key.
#[derive(Clone, Debug)]
pub struct UniquePlaceholderBinding {
    pub binding: PlaceholderBinding,
}

impl UniquePlaceholderBinding {
    #[must_use]
    pub fn new(fallback_value: Vec<u8>) -> Self {
        Self {
            binding: PlaceholderBinding::new(
                PlaceholderId::Unique(rand_u64()),
                fallback_value,
                random_user_bytes(),
            ),
        }
    }

    fn into_binding(self) -> PlaceholderBinding {
        self.binding
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum PlaceholderId {
    Unique(u64),
    Shared(u16),
}

fn rand_u64() -> u64 {
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(array)
}

#[must_use]
pub fn random_user_bytes() -> [u8; 2] {
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    [bytes[0], bytes[1]]
}
