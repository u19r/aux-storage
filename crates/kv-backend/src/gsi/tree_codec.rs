//! Compact ordered projection nodes retain full logical order in verified leaf
//! content.
use storage_types::{StorageError, StorageResult};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeId {
    Leaf([u8; 32]),
    Branch([u8; 16]),
}
#[derive(Clone)]
pub(super) struct Branch {
    pub bit: usize,
    pub children: [NodeId; 2],
}
#[derive(Clone)]
pub(super) struct Root {
    pub revision: u64,
    pub child: NodeId,
}

pub(super) fn corrupt() -> StorageError {
    StorageError::internal("Invalid compact GSI tree")
}
pub(super) fn bit(word: &[u8], position: usize) -> usize {
    let byte = position / 9;
    if byte >= word.len() {
        return 0;
    }
    if position.is_multiple_of(9) {
        return 1;
    }
    usize::from((word[byte] >> (8 - position % 9)) & 1)
}
pub(super) fn divergence(left: &[u8], right: &[u8]) -> StorageResult<usize> {
    (0..left.len().max(right.len()) * 9 + 1)
        .find(|position| bit(left, *position) != bit(right, *position))
        .ok_or_else(corrupt)
}
impl NodeId {
    fn encode(self, bytes: &mut Vec<u8>) {
        match self {
            Self::Leaf(id) => {
                bytes.push(0);
                bytes.extend_from_slice(&id);
            }
            Self::Branch(id) => {
                bytes.push(1);
                bytes.extend_from_slice(&id);
                bytes.extend_from_slice(&[0; 16]);
            }
        }
    }
    fn decode(bytes: &[u8]) -> StorageResult<Self> {
        match bytes {
            [0, rest @ ..] if rest.len() == 32 => {
                Ok(Self::Leaf(rest.try_into().map_err(|_| corrupt())?))
            }
            [1, rest @ ..] if rest.len() == 32 && rest[16..] == [0; 16] => {
                Ok(Self::Branch(rest[..16].try_into().map_err(|_| corrupt())?))
            }
            _ => Err(corrupt()),
        }
    }
    pub fn key(self, scope: &[u8; 6]) -> Vec<u8> {
        let (family, id): (u8, &[u8]) = match &self {
            Self::Leaf(id) => (crate::constants::GSI_AUTHORITY_FAMILY, id),
            Self::Branch(id) => (crate::constants::GSI_BRANCH_FAMILY, id),
        };
        [vec![family], scope.to_vec(), id.to_vec()].concat()
    }
}
impl Branch {
    pub fn decode(bytes: &[u8], previous: Option<usize>) -> StorageResult<Self> {
        if bytes.len() != 71 || bytes[0] != 1 {
            return Err(corrupt());
        }
        let bit = usize::try_from(u32::from_be_bytes(
            bytes[1..5].try_into().map_err(|_| corrupt())?,
        ))
        .map_err(|_| corrupt())?;
        if !(32 * 9..=crate::constants::GSI_TREE_MAX_BIT).contains(&bit)
            || previous.is_some_and(|last| bit <= last)
        {
            return Err(corrupt());
        }
        let children = [
            NodeId::decode(&bytes[5..38])?,
            NodeId::decode(&bytes[38..71])?,
        ];
        if children[0] == children[1] {
            return Err(corrupt());
        }
        Ok(Self { bit, children })
    }
    pub fn encode(&self) -> StorageResult<Vec<u8>> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(
            &u32::try_from(self.bit)
                .map_err(|_| corrupt())?
                .to_be_bytes(),
        );
        for child in self.children {
            child.encode(&mut bytes);
        }
        Self::decode(&bytes, None)?;
        Ok(bytes)
    }
}
impl Root {
    pub fn decode(bytes: &[u8]) -> StorageResult<Self> {
        if bytes.len() != 42 || bytes[0] != 1 {
            return Err(corrupt());
        }
        let revision = u64::from_be_bytes(bytes[1..9].try_into().map_err(|_| corrupt())?);
        if revision == 0 {
            return Err(corrupt());
        }
        Ok(Self {
            revision,
            child: NodeId::decode(&bytes[9..])?,
        })
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend_from_slice(&self.revision.to_be_bytes());
        self.child.encode(&mut bytes);
        bytes
    }
}
pub(super) fn root_key(scope: &[u8; 6], word: &[u8]) -> StorageResult<Vec<u8>> {
    if word.len() > crate::constants::GSI_TREE_MAX_WORD_BYTES {
        return Err(corrupt());
    }
    Ok([
        vec![crate::constants::GSI_PROJECTION_FAMILY],
        scope.to_vec(),
        word.get(..32).ok_or_else(corrupt)?.to_vec(),
    ]
    .concat())
}
