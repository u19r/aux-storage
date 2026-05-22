use crate::ItemKeyError;

pub trait SerializesToKey {
    /// Serialize the key to a byte vector
    fn increment_bytes_and_serialize(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut bytes = self.serialize_to_bytes()?;
        for i in (0..bytes.len()).rev() {
            if bytes[i] < 0xFF {
                bytes[i] += 1;
                return Ok(bytes);
            }
            bytes[i] = 0x00;
        }
        // If all bytes were 0xFF, we need to add an extra byte
        bytes.push(0x00);
        Ok(bytes)
    }
    fn decrement_bytes_and_serialize(&self) -> Result<Vec<u8>, ItemKeyError> {
        let mut bytes = self.serialize_to_bytes()?;
        for i in (0..bytes.len()).rev() {
            if bytes[i] > 0x00 {
                bytes[i] -= 1;
                return Ok(bytes);
            }
            bytes[i] = 0xFF;
        }
        // If all bytes were 0x00, we need to remove the last byte
        if !bytes.is_empty() {
            bytes.pop();
        }
        Ok(bytes)
    }
    fn serialize_to_bytes(&self) -> Result<Vec<u8>, ItemKeyError>;
}
