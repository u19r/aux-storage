pub(crate) mod attributes;
pub mod dynamo_json;
mod item;
mod json_field;
mod key_attributes;
pub(crate) mod ordinal_projection;
pub(crate) mod projection;
pub(crate) mod residual;
mod scalar;
pub(crate) mod text;
mod verified;
pub(crate) mod write;

pub use item::{
    BatchGetWireItemResponse, TryFromWireItem, TryIntoWireItem, WireAttributeDecode, WireItem,
    decode_wire_bool, encode_wire_attribute,
};
pub use json_field::{decode_wire_field, decode_wire_field_json, decode_wire_serde_string};
pub use key_attributes::WireItemKeyAttributes;
pub use scalar::WireScalar;
pub use verified::VerifiedWireItem;

#[cfg(test)]
mod write_tests;

#[cfg(test)]
mod verified_tests;

#[cfg(test)]
mod item_tests;
