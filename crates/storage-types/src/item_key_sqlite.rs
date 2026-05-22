use crate::{
    ItemKey,
    item_key::{ItemKeyEnum, ItemKeyError},
};

impl ItemKey {
    pub fn to_stream_name_suffix(&self) -> Result<String, ItemKeyError> {
        let hash_key = self.hash_key().inner_string().map_err(|err| {
            ItemKeyEnum::Validation(format!("Key attribute must be scalar: {err}"))
        })?;
        let range_key = match self.range_key() {
            Some(value) => value.inner_string().map_err(|err| {
                ItemKeyEnum::Validation(format!("Key attribute must be scalar: {err}"))
            })?,
            None => String::new(),
        };
        Ok(format!("H={hash_key}/R={range_key}"))
    }
}
