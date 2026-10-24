use quote::ToTokens;
use syn::{
    Data, DeriveInput, Fields, GenericArgument, LitStr, Meta, PathArguments, Token, Type,
    punctuated::Punctuated,
};

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
    parse_serde_options(&input_ast.attrs)
        .ok()?
        .into_iter()
        .find_map(|option| {
            if let Meta::NameValue(option) = option
                && option.path.is_ident("rename_all")
                && let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(value),
                    ..
                }) = option.value
            {
                return Some(value.value());
            }
            None
        })
}

pub(crate) struct WireItemFieldAttributes {
    pub(crate) wire_name: String,
    pub(crate) parse_with: Option<syn::ExprPath>,
    pub(crate) serialize_with: Option<syn::ExprPath>,
    pub(crate) skip_serializing_if: Option<syn::ExprPath>,
    pub(crate) default: Option<WireItemFieldDefault>,
    pub(crate) aliases: Vec<LitStr>,
}

pub(crate) enum WireItemFieldDefault {
    Trait,
    Path(syn::ExprPath),
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
    let (serde_default, aliases) = parse_wire_item_serde_field_default(field)?;
    let skip_serializing_if = parse_wire_item_serde_skip(field);
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
    let aliases = normalize_field_aliases(&wire_name, aliases);
    Ok(WireItemFieldAttributes {
        wire_name,
        parse_with,
        serialize_with,
        skip_serializing_if,
        default: serde_default,
        aliases,
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
    let mut deserialize_rename = None;
    let mut plain_rename = None;
    for option in parse_serde_options(&field.attrs).ok()? {
        match option {
            Meta::NameValue(value) if value.path.is_ident("rename") => {
                if let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(value),
                    ..
                }) = value.value
                {
                    plain_rename = Some(value.value());
                }
            }
            Meta::List(list) if list.path.is_ident("rename") => {
                let _ = list.parse_nested_meta(|nested| {
                    let value: LitStr = nested.value()?.parse()?;
                    if nested.path.is_ident("deserialize") {
                        deserialize_rename = Some(value.value());
                    } else if nested.path.is_ident("serialize") {
                        plain_rename = Some(value.value());
                    }
                    Ok(())
                });
            }
            _ => {}
        }
    }
    deserialize_rename.or(plain_rename)
}

fn parse_serde_options(attributes: &[syn::Attribute]) -> Result<Vec<Meta>, syn::Error> {
    let mut options = Vec::new();
    for attribute in attributes {
        if attribute.path().is_ident("serde")
            && let Meta::List(list) = &attribute.meta
        {
            options.extend(list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?);
        }
    }
    Ok(options)
}

fn parse_wire_item_serde_field_default(
    field: &syn::Field,
) -> Result<(Option<WireItemFieldDefault>, Vec<LitStr>), syn::Error> {
    let mut default = None;
    let mut aliases = Vec::new();
    for meta in parse_serde_options(&field.attrs)? {
        match meta {
            Meta::Path(path) if path.is_ident("default") => {
                default = Some(WireItemFieldDefault::Trait);
            }
            Meta::NameValue(value) if value.path.is_ident("default") => {
                default = Some(WireItemFieldDefault::Path(parse_expr_path_value(
                    value.value,
                )?));
            }
            Meta::NameValue(value) if value.path.is_ident("alias") => {
                let syn::Expr::Lit(syn::ExprLit {
                    lit: syn::Lit::Str(alias),
                    ..
                }) = value.value
                else {
                    return Err(syn::Error::new_spanned(
                        value,
                        "serde alias must be a string",
                    ));
                };
                aliases.push(alias);
            }
            _ => {}
        }
    }
    Ok((default, aliases))
}

fn parse_wire_item_serde_skip(field: &syn::Field) -> Option<syn::ExprPath> {
    for option in parse_serde_options(&field.attrs).ok()? {
        if let Meta::NameValue(option) = option
            && option.path.is_ident("skip_serializing_if")
            && let syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Str(value),
                ..
            }) = option.value
        {
            return syn::parse_str::<syn::ExprPath>(&value.value()).ok();
        }
    }
    None
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

pub(crate) fn is_string_or_option_string(ty: &Type) -> bool {
    use crate::wire_attribute_helpers::is_string_type;
    is_string_type(ty) || option_inner_type(ty).is_some_and(is_string_type)
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

pub(crate) fn parse_named_fields<'a>(
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

fn normalize_field_aliases(canonical: &str, aliases: Vec<LitStr>) -> Vec<LitStr> {
    let mut distinct: Vec<LitStr> = Vec::new();
    for alias in aliases {
        if alias.value() != canonical
            && !distinct.iter().any(|other| other.value() == alias.value())
        {
            distinct.push(alias);
        }
    }
    distinct
}
