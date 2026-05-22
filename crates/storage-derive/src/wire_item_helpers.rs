use quote::ToTokens;
use syn::{DeriveInput, GenericArgument, LitStr, PathArguments, Token, Type};

pub(crate) fn validate_wire_item_struct_attributes(
    input_ast: &DeriveInput,
) -> Result<(), syn::Error> {
    for attr in &input_ast.attrs {
        if !attr.path().is_ident("wire_item") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let Some(segment) = meta.path.segments.last() else {
                return Err(meta.error("wire_item key is missing"));
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "decode_with" => Err(meta.error(
                    "wire_item decode_with is no longer supported; use field-level decode via \
                     WireAttributeDecode and #[wire_item(parse_with = ...)] only when necessary",
                )),
                other => Err(meta.error(format!("unknown wire_item struct key: {other}"))),
            }
        })?;
    }
    Ok(())
}
pub(crate) fn parse_wire_item_struct_serde_rename_all(input_ast: &DeriveInput) -> Option<String> {
    let mut rename_all: Option<String> = None;

    for attr in &input_ast.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let value: LitStr = meta.value()?.parse()?;
                rename_all = Some(value.value());
            }
            Ok(())
        });
    }

    rename_all
}

pub(crate) struct WireItemFieldAttributes {
    pub(crate) wire_name: String,
    pub(crate) parse_with: Option<syn::ExprPath>,
    pub(crate) serialize_with: Option<syn::ExprPath>,
    pub(crate) skip_serializing_if_option_is_none: bool,
}

pub(crate) fn parse_wire_item_field_attributes(
    field: &syn::Field,
    serde_rename_all: Option<&str>,
) -> Result<WireItemFieldAttributes, syn::Error> {
    let mut wire_item_rename: Option<String> = None;
    let mut parse_with: Option<syn::ExprPath> = None;
    let mut serialize_with: Option<syn::ExprPath> = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("wire_item") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            let Some(segment) = meta.path.segments.last() else {
                return Err(meta.error("wire_item key is missing"));
            };
            let ident = segment.ident.to_string();
            match ident.as_str() {
                "rename" => {
                    let value: LitStr = meta.value()?.parse()?;
                    wire_item_rename = Some(value.value());
                }
                "parse_with" => {
                    parse_with = Some(parse_expr_path_value(meta.value()?.parse()?)?);
                }
                "serialize_with" => {
                    serialize_with = Some(parse_expr_path_value(meta.value()?.parse()?)?);
                }
                other => return Err(meta.error(format!("unknown wire_item field key: {other}"))),
            }
            Ok(())
        })?;
    }

    let serde_rename = parse_wire_item_serde_field_rename(field);
    let skip_serializing_if_option_is_none =
        parse_wire_item_serde_skip_option_none(field).unwrap_or(false);
    let Some(field_ident) = field.ident.as_ref() else {
        return Err(syn::Error::new_spanned(
            field,
            "wire_item requires named fields",
        ));
    };
    let field_name = field_ident.to_string();
    let wire_name = wire_item_rename
        .or(serde_rename)
        .or_else(|| serde_rename_all.map(|rule| apply_serde_rename_all_rule(&field_name, rule)))
        .or_else(|| default_wire_timestamp_alias(field_name.as_str()).map(str::to_string))
        .unwrap_or(field_name);
    Ok(WireItemFieldAttributes {
        wire_name,
        parse_with,
        serialize_with,
        skip_serializing_if_option_is_none,
    })
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

pub(crate) fn default_wire_timestamp_alias(field_name: &str) -> Option<&'static str> {
    match field_name {
        "created_at" => Some("c_at"),
        "updated_at" => Some("u_at"),
        "expires_at" => Some("e_at"),
        _ => None,
    }
}

pub(crate) fn parse_wire_item_serde_field_rename(field: &syn::Field) -> Option<String> {
    let mut deserialize_rename: Option<String> = None;
    let mut plain_rename: Option<String> = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("rename") {
                return Ok(());
            }

            if meta.input.peek(Token![=]) {
                let value: LitStr = meta.value()?.parse()?;
                plain_rename = Some(value.value());
                return Ok(());
            }

            let _ = meta.parse_nested_meta(|nested| {
                if nested.path.is_ident("deserialize") {
                    let value: LitStr = nested.value()?.parse()?;
                    deserialize_rename = Some(value.value());
                    return Ok(());
                }
                if nested.path.is_ident("serialize") && plain_rename.is_none() {
                    let value: LitStr = nested.value()?.parse()?;
                    plain_rename = Some(value.value());
                }
                Ok(())
            });
            Ok(())
        });
    }

    deserialize_rename.or(plain_rename)
}

pub(crate) fn parse_wire_item_serde_skip_option_none(field: &syn::Field) -> Option<bool> {
    let mut skip_option_none = None;

    for attr in &field.attrs {
        if !attr.path().is_ident("serde") {
            continue;
        }
        let _ = attr.parse_nested_meta(|meta| {
            if !meta.path.is_ident("skip_serializing_if") {
                return Ok(());
            }
            let value: LitStr = meta.value()?.parse()?;
            let path = value.value();
            skip_option_none = Some(path.ends_with("Option::is_none"));
            Ok(())
        });
    }

    skip_option_none
}

pub(crate) fn apply_serde_rename_all_rule(field_name: &str, rule: &str) -> String {
    match rule {
        "camelCase" => snake_to_camel_case(field_name, false),
        "PascalCase" => snake_to_camel_case(field_name, true),
        "kebab-case" => field_name.replace('_', "-"),
        "SCREAMING_SNAKE_CASE" => field_name.to_ascii_uppercase(),
        "SCREAMING-KEBAB-CASE" => field_name.replace('_', "-").to_ascii_uppercase(),
        "lowercase" => field_name.to_ascii_lowercase(),
        "UPPERCASE" => field_name.to_ascii_uppercase(),
        _ => field_name.to_string(),
    }
}

pub(crate) fn snake_to_camel_case(field_name: &str, upper_first: bool) -> String {
    let mut iter = field_name.split('_').filter(|segment| !segment.is_empty());
    let Some(first) = iter.next() else {
        return String::new();
    };

    let mut out = String::new();
    if upper_first {
        push_capitalized(&mut out, first);
    } else {
        out.push_str(first);
    }

    for segment in iter {
        push_capitalized(&mut out, segment);
    }
    out
}

pub(crate) fn push_capitalized(out: &mut String, segment: &str) {
    let mut chars = segment.chars();
    if let Some(first) = chars.next() {
        out.extend(first.to_uppercase());
        out.push_str(chars.as_str());
    }
}

pub(crate) fn is_bool_or_option_bool(ty: &Type) -> bool {
    is_bool(ty) || is_option_bool(ty)
}

pub(crate) fn is_vec_or_option_vec_non_u8(ty: &Type) -> bool {
    is_vec_non_u8(ty) || option_inner_type(ty).is_some_and(is_vec_non_u8)
}

pub(crate) fn is_vec_u8_or_option_vec_u8(ty: &Type) -> bool {
    is_vec_u8(ty) || option_inner_type(ty).is_some_and(is_vec_u8)
}

pub(crate) fn is_vec_non_u8(ty: &Type) -> bool {
    vec_inner_type(ty).is_some_and(|inner| !is_u8(inner))
}

pub(crate) fn is_vec_u8(ty: &Type) -> bool {
    vec_inner_type(ty).is_some_and(is_u8)
}

pub(crate) fn vec_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let last = type_path.path.segments.last()?;
    if last.ident != "Vec" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let arg = args.args.first()?;
    let GenericArgument::Type(inner_ty) = arg else {
        return None;
    };
    Some(inner_ty)
}

pub(crate) fn is_option_bool(ty: &Type) -> bool {
    option_inner_type(ty).is_some_and(is_bool)
}

pub(crate) fn is_bool(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "bool")
}

pub(crate) fn is_u8(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    type_path
        .path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "u8")
}

pub(crate) fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let last = type_path.path.segments.last()?;
    if last.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let arg = args.args.first()?;
    let GenericArgument::Type(inner_ty) = arg else {
        return None;
    };
    Some(inner_ty)
}
