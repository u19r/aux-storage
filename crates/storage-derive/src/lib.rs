//! Internal derive macros for aux-storage entity helpers.
//!
//! This crate is not a supported downstream API.
#![doc(hidden)]

use proc_macro::TokenStream;

mod derive_helpers;
mod single_table_keys;
mod wire_attribute_derives;
mod wire_attribute_helpers;
mod wire_item_derives;
mod wire_item_helpers;

#[cfg(test)]
mod single_table_keys_tests;
#[cfg(test)]
mod wire_attribute_helpers_tests;

#[proc_macro_derive(SingleTableKeys, attributes(single_table, wire_item))]
pub fn derive_single_table_keys(input: TokenStream) -> TokenStream {
    single_table_keys::derive_single_table_keys_impl(input)
}

#[proc_macro_derive(WireItemDecode, attributes(wire_item))]
pub fn derive_wire_item_decode(input: TokenStream) -> TokenStream {
    wire_item_derives::derive_wire_item_decode_impl(input)
}

#[proc_macro_derive(WireProjectionDecode, attributes(wire_item))]
pub fn derive_wire_projection_decode(input: TokenStream) -> TokenStream {
    wire_item_derives::derive_wire_projection_decode_impl(input)
}

#[proc_macro_derive(WireEntityDecode, attributes(wire_item))]
pub fn derive_wire_entity_decode(input: TokenStream) -> TokenStream {
    wire_item_derives::derive_wire_entity_decode_impl(input)
}

#[proc_macro_derive(WireItemEncode, attributes(wire_item))]
pub fn derive_wire_item_encode(input: TokenStream) -> TokenStream {
    wire_item_derives::derive_wire_item_encode_impl(input)
}

#[proc_macro_derive(WireAttributeDecode, attributes(wire_attribute))]
pub fn derive_wire_attribute_decode(input: TokenStream) -> TokenStream {
    wire_attribute_derives::derive_wire_attribute_decode_impl(input)
}
