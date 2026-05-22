//! Stable storage provider traits, configuration, and shared provider types.
//!
//! Downstream libraries should depend on this crate for embedded storage
//! abstractions instead of backend implementation crates.

mod config;
mod provider;
mod update_logic;

pub use storage_types::AttributeValue;

pub use crate::{
    config::*,
    provider::*,
    update_logic::{
        BoundUpdateOperation, SetFunction, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, parse_update_expression,
        resolve_attribute_value, return_values_need_old_item, return_values_need_updated_fields,
        split_operations_preserving_functions, update_item_response,
    },
};
