//! The regional capability the storage crate holds as one trait object per
//! connection.
pub trait RegionalOperations:
    crate::RegionalBindingOperations
    + crate::RegionalReceiveOperations
    + crate::RegionalSendOperations
    + crate::RegionalPairingOperations
    + crate::RegionalUnpairOperations
{
}
impl<T> RegionalOperations for T where T: crate::RegionalBindingOperations
        + crate::RegionalReceiveOperations
        + crate::RegionalSendOperations
        + crate::RegionalPairingOperations
        + crate::RegionalUnpairOperations
        + crate::RegionalUnpairOperations
{
}
