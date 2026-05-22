use std::fmt::Display;

use crate::{StorageError, StorageResult, TryFromWireItem, WireItem, context::ErrorContext};

pub trait ValidatedEntity: Sized {
    type ValidationError: Display + Send + Sync + 'static;

    fn validate(&self) -> Result<(), Self::ValidationError>;

    fn into_validated(self) -> Result<Self, Self::ValidationError> {
        self.validate()?;
        Ok(self)
    }
}

pub trait NoopValidatedEntity: Sized {}

impl<T> ValidatedEntity for T
where T: NoopValidatedEntity
{
    type ValidationError = std::convert::Infallible;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        Ok(())
    }
}

pub trait StoredEntity: ValidatedEntity + Sized {
    fn try_from_stored_item(item: &WireItem) -> StorageResult<Self>;

    fn storage_type_name() -> &'static str {
        std::any::type_name::<Self>()
    }

    fn decode_projection<P, F>(item: &WireItem, build: F) -> StorageResult<Self>
    where
        P: TryFromWireItem,
        F: FnOnce(P) -> Result<Self, Self::ValidationError>,
    {
        let projection = P::try_from_wire_item(item)?;
        match build(projection) {
            Ok(entity) => Ok(entity),
            Err(err) => Err(StorageError::internal(&err.to_string())).context(format!(
                "invalid persisted {}: {err}",
                Self::storage_type_name()
            )),
        }
    }

    fn validate_loaded(entity: Self) -> StorageResult<Self> {
        match entity.into_validated() {
            Ok(entity) => Ok(entity),
            Err(err) => Err(StorageError::internal(&err.to_string())).context(format!(
                "invalid persisted {}: {err}",
                Self::storage_type_name()
            )),
        }
    }
}

impl<T> TryFromWireItem for T
where T: StoredEntity
{
    fn try_from_wire_item(item: &WireItem) -> StorageResult<Self> {
        T::try_from_stored_item(item)
    }
}
