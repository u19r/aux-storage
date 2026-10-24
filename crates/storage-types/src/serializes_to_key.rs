use crate::ItemKeyError;

pub trait SerializesToKey {
    /// Serialize the key to a byte vector
    fn increment_bytes_and_serialize(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut bytes = self.serialize_to_bytes()?;
        // The smallest key strictly greater than this one is its zero-byte
        // extension. Carrying into the final byte skips binary prefix keys.
        bytes.push(0x00);
        Ok(bytes)
    }
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError>;
}
