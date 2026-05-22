use proc_macro::TokenStream;
use quote::quote;
use syn::{Data, DeriveInput, parse_macro_input};

use crate::wire_attribute_helpers::{
    is_single_string_tuple_struct, parse_wire_attribute_type_attributes,
};

pub(crate) fn derive_wire_attribute_decode_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let type_name = &input_ast.ident;
    let (parse_with, from_string) = match parse_wire_attribute_type_attributes(&input_ast) {
        Ok(attrs) => attrs,
        Err(error) => return error.to_compile_error().into(),
    };
    let is_enum = matches!(input_ast.data, Data::Enum(_));
    let is_single_string_tuple_struct = is_single_string_tuple_struct(&input_ast.data);

    let decode_expr = if let Some(parse_with) = parse_with {
        quote! {
            #parse_with(raw, field)
        }
    } else if from_string {
        quote! {
            Ok(<Self as From<String>>::from(raw.to_string()))
        }
    } else if is_enum {
        quote! {
            storage::types::decode_wire_serde_string::<Self>(raw, field)
        }
    } else if is_single_string_tuple_struct {
        quote! {
            Ok(Self(raw.to_string()))
        }
    } else {
        quote! {
            raw.parse::<Self>()
                .map_err(|err| storage::types::StorageError::internal(&format!("invalid {field} field: {err}")))
        }
    };

    let expanded = quote! {
        impl storage::types::WireAttributeDecode for #type_name {
            fn decode(raw: Option<&str>, field: &str) -> storage::types::StorageResult<Self> {
                let raw = raw
                    .ok_or_else(|| storage::types::StorageError::internal(&format!("missing required field {field}")))?;
                #decode_expr
            }
        }
    };

    TokenStream::from(expanded)
}
