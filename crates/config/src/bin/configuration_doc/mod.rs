mod app;

#[cfg(test)]
pub(crate) use app::{
    collect_property_refs, collect_visible_features, render_compile_time, schema_definitions,
    type_info,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    app::run()
}

#[cfg(test)]
mod configuration_doc_tests;

#[cfg(test)]
mod compile_time_render_tests;
