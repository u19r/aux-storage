use quote::ToTokens;
use syn::{Data, DeriveInput, Fields, Type};

pub(crate) fn parse_wire_attribute_type_attributes(
    input_ast: &DeriveInput,
) -> Result<(Option<syn::ExprPath>, bool), syn::Error> {
    let mut parse_with: Option<syn::ExprPath> = None;
    let mut from_string = false;
    for attr in &input_ast.attrs {
        if !attr.path().is_ident("wire_attribute") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let Some(segment) = meta.path.segments.last() else {
                return Err(meta.error("wire_attribute key is missing"));
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "parse_with" => {
                    parse_with = Some(parse_expr_path_value(meta.value()?.parse()?)?);
                }
                "from_string" => {
                    from_string = true;
                }
                other => return Err(meta.error(format!("unknown wire_attribute key: {other}"))),
            }
            Ok(())
        })?;
    }
    Ok((parse_with, from_string))
}

fn parse_expr_path_value(expr: syn::Expr) -> Result<syn::ExprPath, syn::Error> {
    match expr {
        syn::Expr::Path(path) => Ok(path),
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(value),
            ..
        }) => syn::parse_str::<syn::ExprPath>(&value.value()),
        other => syn::parse2::<syn::ExprPath>(other.to_token_stream()).map_err(|_| {
            syn::Error::new_spanned(other, "expected parser path or string literal parser path")
        }),
    }
}

pub(crate) fn is_single_string_tuple_struct(data: &Data) -> bool {
    let Data::Struct(data_struct) = data else {
        return false;
    };
    let Fields::Unnamed(unnamed) = &data_struct.fields else {
        return false;
    };
    if unnamed.unnamed.len() != 1 {
        return false;
    }
    unnamed
        .unnamed
        .first()
        .is_some_and(|field| is_string_type(&field.ty))
}

pub(crate) fn is_string_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "String")
}
