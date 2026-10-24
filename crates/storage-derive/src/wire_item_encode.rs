use std::collections::HashSet;

use proc_macro::TokenStream;
use quote::{ToTokens, quote};
use syn::{DeriveInput, Field, LitStr, parse_macro_input};

use crate::{
    single_table_keys::parse_entity_indexers,
    wire_item_helpers::{
        WireItemFieldAttributes, option_inner_type, parse_named_fields,
        parse_wire_item_field_attributes, parse_wire_item_struct_serde_rename_all,
        validate_wire_item_struct_attributes,
    },
};

pub(crate) fn derive_wire_item_encode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let entries = match parse_item_entries(&input_ast) {
        Ok(entries) => entries,
        Err(error) => return error.to_compile_error().into(),
    };
    let struct_name = &input_ast.ident;
    quote! {
        impl storage::types::TryIntoWireItem for #struct_name {
            fn try_into_wire_item(&self) -> storage::types::StorageResult<storage::types::WireItem> {
                let mut data = Vec::new();
                {
                    use serde::ser::SerializeMap as _;
                    let mut serializer = serde_json::Serializer::new(&mut data);
                    let mut map = serde::Serializer::serialize_map(&mut serializer, None).map_err(
                        |err| storage::types::StorageError::internal(
                            &format!("encode wire item map start failed: {err}"),
                        ),
                    )?;
                    #(#entries)*
                    map.end().map_err(|err| storage::types::StorageError::internal(
                        &format!("encode wire item map finish failed: {err}"),
                    ))?;
                }
                Ok(storage::types::WireItem::dynamo_json(data))
            }
        }
    }.into()
}

fn parse_item_entries(input_ast: &DeriveInput) -> syn::Result<Vec<impl ToTokens>> {
    validate_wire_item_struct_attributes(input_ast)?;
    let rename_all = parse_wire_item_struct_serde_rename_all(input_ast);
    order_wire_fields(
        input_ast,
        parse_named_fields(input_ast, "WireItemEncode")?,
        rename_all.as_deref(),
    )?
    .into_iter()
    .map(|(field, attrs)| encode_field_entry(field, attrs))
    .collect()
}

fn encode_field_entry(
    field: &syn::Field,
    attrs: WireItemFieldAttributes,
) -> syn::Result<impl ToTokens + use<>> {
    let ident = field
        .ident
        .as_ref()
        .ok_or_else(|| syn::Error::new_spanned(field, "WireItemEncode field identifier missing"))?;
    let name = LitStr::new(&attrs.wire_name, ident.span());
    if is_absent_option_predicate(attrs.skip_serializing_if.as_ref())
        && let Some(inner) = option_inner_type(&field.ty)
    {
        let encoded = encode_attribute(
            inner,
            quote! { value },
            attrs.serialize_with.as_ref(),
            &name,
        );
        let entry = serialize_field_entry(&name, encoded);
        return Ok(quote! { if let Some(value) = self.#ident.as_ref() { #entry } });
    }
    let encoded = encode_attribute(
        &field.ty,
        quote! { &self.#ident },
        attrs.serialize_with.as_ref(),
        &name,
    );
    let entry = serialize_field_entry(&name, encoded);
    Ok(match attrs.skip_serializing_if {
        Some(predicate) => quote! { if !#predicate(&self.#ident) { #entry } },
        None => quote! { #entry },
    })
}

fn encode_attribute(
    ty: &syn::Type,
    value: impl ToTokens,
    custom: Option<&syn::ExprPath>,
    name: &LitStr,
) -> impl ToTokens {
    if let Some(custom) = custom {
        quote! { #custom(#value, #name)? }
    } else if let Some(scalar) = encode_scalar(ty, &value) {
        quote! { #scalar }
    } else {
        quote! { storage::types::encode_wire_attribute(#value, #name)? }
    }
}

fn serialize_field_entry(name: &LitStr, encoded: impl ToTokens) -> impl ToTokens {
    quote! {
        let attr = #encoded;
        map.serialize_entry(#name, &attr).map_err(|err| storage::types::StorageError::internal(
            &format!("encode wire item field {} failed: {err}", #name),
        ))?;
    }
}

fn is_absent_option_predicate(predicate: Option<&syn::ExprPath>) -> bool {
    predicate.is_some_and(|predicate| {
        predicate
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "is_none")
            && predicate
                .path
                .segments
                .iter()
                .any(|segment| segment.ident == "Option")
    })
}

fn encode_scalar(ty: &syn::Type, value: impl ToTokens) -> Option<impl ToTokens> {
    let syn::Type::Path(path) = ty else {
        return None;
    };
    let name = path.path.segments.last()?.ident.to_string();
    match name.as_str() {
        "String" => Some(quote! { storage::types::WireScalar::String(#value) }),
        "bool" => Some(quote! { storage::types::WireScalar::Bool(*(#value)) }),
        "u8" | "u16" | "u32" | "u64" | "usize" | "i8" | "i16" | "i32" | "i64" | "isize" => {
            Some(quote! { storage::types::WireScalar::Number(#value) })
        }
        _ => None,
    }
}

pub(crate) fn order_wire_fields<'a>(
    input: &DeriveInput,
    fields: impl IntoIterator<Item = &'a Field>,
    rename_all: Option<&str>,
) -> Result<Vec<(&'a Field, WireItemFieldAttributes)>, syn::Error> {
    let indexers = parse_entity_indexers(input)?;
    let mut names = HashSet::new();
    let mut ordered = Vec::new();
    for field in fields {
        let attributes = parse_wire_item_field_attributes(field, rename_all)?;
        let name = attributes.wire_name.as_str();
        if !names.insert(attributes.wire_name.clone()) {
            return Err(syn::Error::new_spanned(
                field,
                format!("duplicate wire attribute `{name}`"),
            ));
        }
        ordered.push((field, attributes));
    }
    ordered.sort_unstable_by_key(|(_, attributes)| {
        let ordinal = indexers
            .iter()
            .find(|indexer| indexer.attribute_name == attributes.wire_name)
            .map(|indexer| usize::from(indexer.ordinal));
        (
            storage_types::wire_attribute_priority(&attributes.wire_name, ordinal),
            attributes.wire_name.clone(),
        )
    });
    Ok(ordered)
}
