use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input, punctuated::Punctuated};

use crate::wire_item_helpers::{
    is_bool_or_option_bool, is_option_bool, is_vec_or_option_vec_non_u8,
    is_vec_u8_or_option_vec_u8, option_inner_type, parse_wire_item_field_attributes,
    parse_wire_item_struct_serde_rename_all, validate_wire_item_struct_attributes,
};

pub(crate) fn derive_wire_item_decode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_ast.ident;
    if let Err(error) = validate_wire_item_struct_attributes(&input_ast) {
        return error.to_compile_error().into();
    }
    let serde_rename_all = parse_wire_item_struct_serde_rename_all(&input_ast);

    let fields = match named_fields(&input_ast, "WireItemDecode") {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };

    let mut scalar_field_names: Vec<LitStr> = Vec::new();
    let mut field_initializers = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.as_ref() else {
            return syn::Error::new_spanned(field, "WireItemDecode field identifier missing")
                .to_compile_error()
                .into();
        };
        let field_ty = &field.ty;
        let attrs = match parse_wire_item_field_attributes(field, serde_rename_all.as_deref()) {
            Ok(attrs) => attrs,
            Err(error) => return error.to_compile_error().into(),
        };
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                }
            } else {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                        .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            if let Some(parse_with) = parse_with {
                quote! {
                    #parse_with(#raw_value_expr, #wire_name_lit)?
                }
            } else if is_vec_or_option_vec_non_u8(field_ty) {
                quote! {
                    storage::types::decode_wire_field_json::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::decode_wire_field::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            }
        };

        field_initializers.push(quote! {
            #field_ident: #decode_expr
        });
    }

    let values_stmt = if scalar_field_names.is_empty() {
        quote! {
            let values: Vec<Option<std::borrow::Cow<'_, str>>> = Vec::new();
        }
    } else {
        quote! {
            let values = item.scalar_attributes(&[#(#scalar_field_names),*])?;
        }
    };

    let expanded = quote! {
        impl storage::types::TryFromWireItem for #struct_name {
            fn try_from_wire_item(item: &storage::types::WireItem) -> storage::types::StorageResult<Self> {
                #values_stmt
                Ok(Self {
                    #(#field_initializers),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

pub(crate) fn derive_wire_projection_decode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_ast.ident;
    if let Err(error) = validate_wire_item_struct_attributes(&input_ast) {
        return error.to_compile_error().into();
    }
    let serde_rename_all = parse_wire_item_struct_serde_rename_all(&input_ast);

    let fields = match named_fields(&input_ast, "WireProjectionDecode") {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };

    let mut scalar_field_names: Vec<LitStr> = Vec::new();
    let mut field_initializers = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.as_ref() else {
            return syn::Error::new_spanned(field, "WireProjectionDecode field identifier missing")
                .to_compile_error()
                .into();
        };
        let field_ty = &field.ty;
        let attrs = match parse_wire_item_field_attributes(field, serde_rename_all.as_deref()) {
            Ok(attrs) => attrs,
            Err(error) => return error.to_compile_error().into(),
        };
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                }
            } else {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                        .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            if let Some(parse_with) = parse_with {
                quote! {
                    #parse_with(#raw_value_expr, #wire_name_lit)?
                }
            } else if is_vec_u8_or_option_vec_u8(field_ty) {
                quote! {
                    storage::types::decode_wire_field::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::decode_wire_field_json::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            }
        };

        field_initializers.push(quote! {
            #field_ident: #decode_expr
        });
    }

    let values_stmt = if scalar_field_names.is_empty() {
        quote! {
            let values: Vec<Option<std::borrow::Cow<'_, str>>> = Vec::new();
        }
    } else {
        quote! {
            let values = item.scalar_attributes(&[#(#scalar_field_names),*])?;
        }
    };

    let expanded = quote! {
        impl storage::types::TryFromWireItem for #struct_name {
            fn try_from_wire_item(item: &storage::types::WireItem) -> storage::types::StorageResult<Self> {
                #values_stmt
                Ok(Self {
                    #(#field_initializers),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

pub(crate) fn derive_wire_entity_decode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_ast.ident;
    if let Err(error) = validate_wire_item_struct_attributes(&input_ast) {
        return error.to_compile_error().into();
    }
    let serde_rename_all = parse_wire_item_struct_serde_rename_all(&input_ast);

    let fields = match named_fields(&input_ast, "WireEntityDecode") {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };

    let mut scalar_field_names: Vec<LitStr> = Vec::new();
    let mut field_initializers = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.as_ref() else {
            return syn::Error::new_spanned(field, "WireEntityDecode field identifier missing")
                .to_compile_error()
                .into();
        };
        let field_ty = &field.ty;
        let attrs = match parse_wire_item_field_attributes(field, serde_rename_all.as_deref()) {
            Ok(attrs) => attrs,
            Err(error) => return error.to_compile_error().into(),
        };
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                }
            } else {
                quote! {
                    item.bool_attribute(#wire_name_lit)?
                        .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            if let Some(parse_with) = parse_with {
                quote! {
                    #parse_with(#raw_value_expr, #wire_name_lit)?
                }
            } else if is_vec_u8_or_option_vec_u8(field_ty) {
                quote! {
                    storage::types::decode_wire_field::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::decode_wire_field_json::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            }
        };

        field_initializers.push(quote! {
            #field_ident: #decode_expr
        });
    }

    let values_stmt = if scalar_field_names.is_empty() {
        quote! {
            let values: Vec<Option<std::borrow::Cow<'_, str>>> = Vec::new();
        }
    } else {
        quote! {
            let values = item.scalar_attributes(&[#(#scalar_field_names),*])?;
        }
    };

    let expanded = quote! {
        impl #struct_name {
            fn try_from_wire_unvalidated(
                item: &storage::types::WireItem,
            ) -> storage::types::StorageResult<Self> {
                #values_stmt
                Ok(Self {
                    #(#field_initializers),*
                })
            }
        }
    };

    TokenStream::from(expanded)
}

pub(crate) fn derive_wire_item_encode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_ast.ident;
    if let Err(error) = validate_wire_item_struct_attributes(&input_ast) {
        return error.to_compile_error().into();
    }
    let serde_rename_all = parse_wire_item_struct_serde_rename_all(&input_ast);

    let fields = match named_fields(&input_ast, "WireItemEncode") {
        Ok(fields) => fields,
        Err(error) => return error.to_compile_error().into(),
    };

    let mut entry_writes = Vec::new();

    for field in fields {
        let Some(field_ident) = field.ident.as_ref() else {
            return syn::Error::new_spanned(field, "WireItemEncode field identifier missing")
                .to_compile_error()
                .into();
        };
        let field_ty = &field.ty;
        let attrs = match parse_wire_item_field_attributes(field, serde_rename_all.as_deref()) {
            Ok(attrs) => attrs,
            Err(error) => return error.to_compile_error().into(),
        };
        let wire_name_lit = LitStr::new(&attrs.wire_name, field_ident.span());
        let serialize_with = attrs.serialize_with.clone();

        let encode_expr = if let Some(serialize_with) = serialize_with.clone() {
            quote! {
                #serialize_with(&self.#field_ident, #wire_name_lit)?
            }
        } else {
            quote! {
                storage::types::encode_wire_attribute(&self.#field_ident, #wire_name_lit)?
            }
        };

        if attrs.skip_serializing_if_option_is_none && option_inner_type(field_ty).is_some() {
            let option_encode_expr = if let Some(serialize_with) = serialize_with {
                quote! {
                    #serialize_with(value, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::encode_wire_attribute(value, #wire_name_lit)?
                }
            };

            entry_writes.push(quote! {
                if let Some(value) = self.#field_ident.as_ref() {
                    let attr = #option_encode_expr;
                    map.serialize_entry(#wire_name_lit, &attr).map_err(|err| {
                        storage::types::StorageError::internal(
                            &format!("encode wire item field {} failed: {err}", #wire_name_lit),
                        )
                    })?;
                }
            });
        } else {
            entry_writes.push(quote! {
                {
                    let attr = #encode_expr;
                    map.serialize_entry(#wire_name_lit, &attr).map_err(|err| {
                        storage::types::StorageError::internal(
                            &format!("encode wire item field {} failed: {err}", #wire_name_lit),
                        )
                    })?;
                }
            });
        }
    }

    let expanded = quote! {
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
                    #(#entry_writes)*
                    map.end().map_err(|err| {
                        storage::types::StorageError::internal(
                            &format!("encode wire item map finish failed: {err}"),
                        )
                    })?;
                }
                Ok(storage::types::WireItem::dynamo_json(data))
            }
        }
    };

    TokenStream::from(expanded)
}

fn named_fields<'a>(
    input_ast: &'a DeriveInput,
    derive_name: &str,
) -> Result<&'a Punctuated<syn::Field, syn::Token![,]>, syn::Error> {
    match &input_ast.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => Ok(&named.named),
            _ => Err(syn::Error::new_spanned(
                &input_ast.ident,
                format!("{derive_name} only supports structs with named fields"),
            )),
        },
        _ => Err(syn::Error::new_spanned(
            &input_ast.ident,
            format!("{derive_name} only supports structs"),
        )),
    }
}
