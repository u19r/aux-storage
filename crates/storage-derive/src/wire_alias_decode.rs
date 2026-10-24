use proc_macro2::TokenStream;
use quote::quote;
use syn::{LitStr, Type};

use super::wire_item_helpers::{
    WireItemFieldAttributes, WireItemFieldDefault, is_bool_or_option_bool, is_option_bool,
};

/// Resolve every accepted persisted name before applying a missing-field
/// default. Two representations are corrupt even when their decoded values
/// agree.
pub(crate) fn generate_alias_field_decode(
    field_type: &Type,
    attributes: &WireItemFieldAttributes,
    decode_json: bool,
    scalar_fields: &mut Vec<LitStr>,
) -> TokenStream {
    let canonical = LitStr::new(&attributes.wire_name, proc_macro2::Span::call_site());
    let aliases = &attributes.aliases;
    let decode = generate_alias_value_decode(field_type, attributes, decode_json, scalar_fields);
    let default = attributes.default.as_ref().map(|default| match default {
        WireItemFieldDefault::Trait => quote! { ::core::default::Default::default() },
        WireItemFieldDefault::Path(path) => quote! { #path() },
    });
    let missing = default.unwrap_or_else(|| decode.clone());
    quote! {{
        let names = [#canonical, #(#aliases),*];
        let representations = item.select_attribute_values(&names)?;
        let mut selected = None;
        for (offset, (name, value)) in names.into_iter().zip(representations).enumerate() {
            if value.is_some() && selected.replace((offset, name)).is_some() {
                return Err(storage::types::StorageError::internal("duplicate persisted field representation"));
            }
        }
        let field_name = selected.map(|(_, name)| name).unwrap_or(#canonical);
        if selected.is_none() { #missing } else { #decode }
    }}
}

fn generate_alias_value_decode(
    field_type: &Type,
    attributes: &WireItemFieldAttributes,
    decode_json: bool,
    scalar_fields: &mut Vec<LitStr>,
) -> TokenStream {
    if attributes.parse_with.is_none() && is_bool_or_option_bool(field_type) {
        return if is_option_bool(field_type) {
            quote! { storage::types::decode_wire_bool(item, field_name)? }
        } else {
            quote! { storage::types::decode_wire_bool(item, field_name)?
            .ok_or_else(|| storage::types::StorageError::internal("missing required boolean field"))? }
        };
    }
    let start = scalar_fields.len();
    scalar_fields.push(LitStr::new(
        &attributes.wire_name,
        proc_macro2::Span::call_site(),
    ));
    scalar_fields.extend(attributes.aliases.iter().cloned());
    let value = if let Some(parser) = &attributes.parse_with {
        quote! { #parser(raw, field_name)? }
    } else if decode_json {
        quote! { storage::types::decode_wire_field_json::<#field_type>(item, raw, field_name)? }
    } else {
        quote! { storage::types::decode_wire_field::<#field_type>(item, raw, field_name)? }
    };
    quote! {{
        let index = #start + selected.map(|(offset, _)| offset).unwrap_or_default();
        let raw = values.get(index).and_then(|value| value.as_deref());
        #value
    }}
}
