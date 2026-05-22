use uuid::Uuid;

#[derive(Clone, Debug)]
pub enum KeyTemplate {
    Literal(Vec<u8>),
    Placeholder(PlaceholderTemplate),
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

    #[must_use]
    pub fn rocks_key(&self) -> Vec<u8> {
        match self {
            Self::Literal(key) => key.clone(),
            Self::Placeholder(template) => template.materialize_fallback(),
        }
    }

    #[must_use]
    pub fn rocks_key_with_fallback(&self, fallback_value: &[u8]) -> Vec<u8> {
        match self {
            Self::Literal(key) => key.clone(),
            Self::Placeholder(template) => template.materialize_with_fallback(fallback_value),
        }
    }

    #[must_use]
    pub fn foundationdb_key(&self) -> Option<Vec<u8>> {
        match self {
            Self::Literal(_) => None,
            Self::Placeholder(template) => Some(template.encode_versionstamped()),
        }
    }

    #[must_use]
    pub fn placeholder_binding(&self) -> Option<&PlaceholderBinding> {
        match self {
            Self::Literal(_) => None,
            Self::Placeholder(template) => Some(&template.binding),
        }
    }

    #[must_use]
    pub fn prefix(&self) -> Option<&[u8]> {
        match self {
            Self::Literal(_) => None,
            Self::Placeholder(template) => Some(template.prefix()),
        }
    }

    #[must_use]
    pub fn with_replaced_prefix(&self, prefix: Vec<u8>) -> Self {
        match self {
            Self::Literal(key) => Self::Literal(key.clone()),
            Self::Placeholder(template) => Self::Placeholder(template.with_replaced_prefix(prefix)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PlaceholderTemplate {
    prefix: Vec<u8>,
    suffix: Vec<u8>,
    binding: PlaceholderBinding,
}

impl PlaceholderTemplate {
    #[must_use]
    fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    #[must_use]
    fn with_replaced_prefix(&self, prefix: Vec<u8>) -> Self {
        Self {
            prefix,
            suffix: self.suffix.clone(),
            binding: self.binding.clone(),
        }
    }

    fn materialize_fallback(&self) -> Vec<u8> {
        self.materialize_with_fallback(self.binding.fallback_value())
    }

    fn materialize_with_fallback(&self, fallback_value: &[u8]) -> Vec<u8> {
        let mut bytes = self.prefix.clone();
        bytes.extend_from_slice(fallback_value);
        bytes.extend_from_slice(&self.suffix);
        bytes
    }

    fn encode_versionstamped(&self) -> Vec<u8> {
        encode_versionstamped_key(&self.prefix, &self.suffix, self.binding.user_bytes)
    }
}

#[derive(Clone, Debug)]
pub struct PlaceholderBinding {
    pub id: PlaceholderId,
    fallback_value: Vec<u8>,
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
    pub fn unique(fallback_value: Vec<u8>) -> Self {
        Self::new(
            PlaceholderId::Unique(rand_u64()),
            fallback_value,
            random_user_bytes(),
        )
    }

    #[must_use]
    pub fn shared(id: u16, fallback_value: Vec<u8>) -> Self {
        Self::new(
            PlaceholderId::Shared(id),
            fallback_value,
            random_user_bytes(),
        )
    }

    #[must_use]
    pub fn fallback_value(&self) -> &[u8] {
        &self.fallback_value
    }

    #[must_use]
    pub fn id(&self) -> PlaceholderId {
        self.id
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

const VERSIONSTAMP_PLACEHOLDER: [u8; 10] = [0xFF; 10];

fn encode_versionstamped_key(prefix: &[u8], suffix: &[u8], user_bytes: [u8; 2]) -> Vec<u8> {
    let placeholder_index = prefix.len();
    let mut bytes = Vec::with_capacity(
        prefix.len() + VERSIONSTAMP_PLACEHOLDER.len() + user_bytes.len() + suffix.len() + 4,
    );

    bytes.extend_from_slice(prefix);
    bytes.extend_from_slice(&VERSIONSTAMP_PLACEHOLDER);
    bytes.extend_from_slice(&user_bytes);
    bytes.extend_from_slice(suffix);

    let offset = u32::try_from(placeholder_index)
        .unwrap_or(u32::MAX)
        .to_le_bytes();
    bytes.extend_from_slice(&offset);
    bytes
}
