//! Stable storage provider traits, configuration, and shared provider types.
//!
//! Downstream libraries should depend on this crate for embedded storage
//! abstractions instead of backend implementation crates.

mod change_index;
mod config;
mod provider;
#[cfg(test)]
mod quint_custom_stream_duration_api_tests;
#[cfg(test)]
mod quint_custom_stream_duration_tests;
mod stream_duration_planner;
#[cfg(test)]
mod stream_duration_planner_tests;
mod stream_duration_trim;
#[cfg(test)]
mod stream_duration_trim_tests;
mod stream_duration_worker;
#[cfg(test)]
mod stream_duration_worker_tests;
mod update_logic;

pub use storage_types::AttributeValue;

pub use crate::{
    change_index::*,
    config::*,
    provider::*,
    stream_duration_planner::*,
    stream_duration_trim::*,
    stream_duration_worker::*,
    update_logic::{
        BoundUpdateOperation, SetFunction, UpdateOperation, apply_bound_update_operations,
        apply_update_operations, before_update_item, before_update_item_optional,
        parse_update_expression, resolve_attribute_value, return_values_need_old_item,
        return_values_need_updated_fields, split_operations_preserving_functions,
        update_item_response, updated_attributes_for_response,
    },
};
