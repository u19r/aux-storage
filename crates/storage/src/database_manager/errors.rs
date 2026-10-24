use storage_types::StorageError;

#[derive(Debug)]
pub enum CappedStorageError {
    CapacityExceededError,
    ItemExistError,
    ItemNotExistsError,
    StorageError(StorageError),
}

impl std::fmt::Display for CappedStorageError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityExceededError => formatter.write_str("entity capacity exceeded"),
            Self::ItemExistError => formatter.write_str("item already exists"),
            Self::ItemNotExistsError => formatter.write_str("item does not exist"),
            Self::StorageError(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for CappedStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StorageError(error) => Some(error),
            _ => None,
        }
    }
}

impl From<StorageError> for CappedStorageError {
    fn from(error: StorageError) -> Self {
        Self::StorageError(error)
    }
}
