use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, LitStr, parse_macro_input};

use crate::wire_item_helpers::{
    WireItemFieldDefault, is_bool_or_option_bool, is_option_bool, is_string_or_option_string,
    is_vec_or_option_vec_non_u8, is_vec_u8_or_option_vec_u8, parse_named_fields,
    parse_wire_item_field_attributes, parse_wire_item_struct_serde_rename_all,
    validate_wire_item_struct_attributes,
};

pub(crate) fn derive_wire_item_decode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let struct_name = &input_ast.ident;
    if let Err(error) = validate_wire_item_struct_attributes(&input_ast) {
        return error.to_compile_error().into();
    }
    let serde_rename_all = parse_wire_item_struct_serde_rename_all(&input_ast);

    let fields = match parse_named_fields(&input_ast, "WireItemDecode") {
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
        if !attrs.aliases.is_empty() {
            let decode = super::wire_alias_decode::generate_alias_field_decode(
                field_ty,
                &attrs,
                is_vec_or_option_vec_non_u8(field_ty),
                &mut scalar_field_names,
            );
            field_initializers.push(quote! { #field_ident: #decode });
            continue;
        }
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());
        let default_expr = attrs.default.as_ref().map(|default| match default {
            WireItemFieldDefault::Trait => quote! { ::core::default::Default::default() },
            WireItemFieldDefault::Path(path) => quote! { #path() },
        });

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                    }
                }
            } else {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                                .ok_or_else(|| storage::types::StorageError::internal(&format!("invalid boolean field {}", #wire_name_lit)))?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                            .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                    }
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            let decoded = if let Some(parse_with) = parse_with {
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
            };
            if let Some(default_expr) = default_expr {
                quote! {
                    if #raw_value_expr.is_none() && item.attribute_value(#wire_name_lit)?.is_none() {
                        #default_expr
                    } else {
                        #decoded
                    }
                }
            } else {
                decoded
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

    let fields = match parse_named_fields(&input_ast, "WireProjectionDecode") {
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
        if !attrs.aliases.is_empty() {
            let decode = super::wire_alias_decode::generate_alias_field_decode(
                field_ty,
                &attrs,
                !(is_vec_u8_or_option_vec_u8(field_ty) || is_string_or_option_string(field_ty)),
                &mut scalar_field_names,
            );
            field_initializers.push(quote! { #field_ident: #decode });
            continue;
        }
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());
        let default_expr = attrs.default.as_ref().map(|default| match default {
            WireItemFieldDefault::Trait => quote! { ::core::default::Default::default() },
            WireItemFieldDefault::Path(path) => quote! { #path() },
        });

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                    }
                }
            } else {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                                .ok_or_else(|| storage::types::StorageError::internal(&format!("invalid boolean field {}", #wire_name_lit)))?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                            .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                    }
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            let decoded = if let Some(parse_with) = parse_with {
                quote! {
                    #parse_with(#raw_value_expr, #wire_name_lit)?
                }
            } else if is_vec_u8_or_option_vec_u8(field_ty) || is_string_or_option_string(field_ty) {
                quote! {
                    storage::types::decode_wire_field::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::decode_wire_field_json::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            };
            if let Some(default_expr) = default_expr {
                quote! {
                    if #raw_value_expr.is_none() && item.attribute_value(#wire_name_lit)?.is_none() {
                        #default_expr
                    } else {
                        #decoded
                    }
                }
            } else {
                decoded
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

    let fields = match parse_named_fields(&input_ast, "WireEntityDecode") {
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
        if !attrs.aliases.is_empty() {
            let decode = super::wire_alias_decode::generate_alias_field_decode(
                field_ty,
                &attrs,
                !(is_vec_u8_or_option_vec_u8(field_ty) || is_string_or_option_string(field_ty)),
                &mut scalar_field_names,
            );
            field_initializers.push(quote! { #field_ident: #decode });
            continue;
        }
        let wire_name = attrs.wire_name;
        let parse_with = attrs.parse_with;
        let wire_name_lit = LitStr::new(&wire_name, field_ident.span());
        let default_expr = attrs.default.as_ref().map(|default| match default {
            WireItemFieldDefault::Trait => quote! { ::core::default::Default::default() },
            WireItemFieldDefault::Path(path) => quote! { #path() },
        });

        let decode_expr = if parse_with.is_none() && is_bool_or_option_bool(field_ty) {
            if is_option_bool(field_ty) {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                    }
                }
            } else {
                if let Some(default_expr) = default_expr {
                    quote! {
                        if item.attribute_value(#wire_name_lit)?.is_none() {
                            #default_expr
                        } else {
                            storage::types::decode_wire_bool(item, #wire_name_lit)?
                                .ok_or_else(|| storage::types::StorageError::internal(&format!("invalid boolean field {}", #wire_name_lit)))?
                        }
                    }
                } else {
                    quote! {
                        storage::types::decode_wire_bool(item, #wire_name_lit)?
                            .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {}", #wire_name_lit)))?
                    }
                }
            }
        } else {
            let scalar_index = scalar_field_names.len();
            scalar_field_names.push(wire_name_lit.clone());
            let raw_value_expr =
                quote! { values.get(#scalar_index).and_then(|value| value.as_deref()) };
            let decoded = if let Some(parse_with) = parse_with {
                quote! {
                    #parse_with(#raw_value_expr, #wire_name_lit)?
                }
            } else if is_vec_u8_or_option_vec_u8(field_ty) || is_string_or_option_string(field_ty) {
                quote! {
                    storage::types::decode_wire_field::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            } else {
                quote! {
                    storage::types::decode_wire_field_json::<#field_ty>(item, #raw_value_expr, #wire_name_lit)?
                }
            };
            if let Some(default_expr) = default_expr {
                quote! {
                    if #raw_value_expr.is_none() && item.attribute_value(#wire_name_lit)?.is_none() {
                        #default_expr
                    } else {
                        #decoded
                    }
                }
            } else {
                decoded
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
