mod app;
mod entrypoint;

#[cfg(test)]
pub(crate) use app::{
    collect_property_refs, collect_visible_features, render_compile_time, schema_definitions,
    type_info,
};
pub use entrypoint::main;

#[cfg(test)]
mod configuration_doc_tests;

#[cfg(test)]
mod compile_time_render_tests;
