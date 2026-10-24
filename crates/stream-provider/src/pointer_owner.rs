//! Independent lifetime authority for table and system stream pointers.

/// A pointer releases only its own image reference. The other pointer emitted
/// by the same item mutation has an independent retention policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum PointerOwner {
    Table = 0,
    System = 1,
}

impl TryFrom<u8> for PointerOwner {
    type Error = storage_types::StorageError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Table),
            1 => Ok(Self::System),
            _ => Err(storage_types::StorageError::internal(
                "invalid stream pointer owner",
            )),
        }
    }
}
