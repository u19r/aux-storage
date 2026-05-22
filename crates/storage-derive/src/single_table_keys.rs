use proc_macro::TokenStream;
use quote::quote;
use syn::{DeriveInput, Expr, Lit, LitStr, parse_macro_input};

use crate::derive_helpers::struct_has_timestamp_millis_field;

pub(crate) fn derive_single_table_keys_impl(input: TokenStream) -> TokenStream {
    let input_ast = parse_macro_input!(input as DeriveInput);
    let name = &input_ast.ident;

    // Collected raw values from attribute
    let mut entity_type: Option<Expr> = None;
    let mut pk_lit: Option<String> = None;
    let mut pk_expr: Option<String> = None;
    let mut sk_expr: Option<String> = None;
    let mut gsi1_pk_expr: Option<String> = None;
    let mut gsi1_sk_expr: Option<String> = None;
    let mut gsi2_pk_expr: Option<String> = None;
    let mut gsi2_sk_expr: Option<String> = None;
    let mut gsi3_pk_expr: Option<String> = None;
    let mut gsi3_sk_expr: Option<String> = None;
    let mut gsi4_pk_expr: Option<String> = None;
    let mut gsi4_sk_expr: Option<String> = None;
    let mut gsi5_pk_expr: Option<String> = None;
    let mut gsi5_sk_expr: Option<String> = None;
    // Pattern metadata (optional) for registry documentation
    let mut gsi_patterns: Vec<(String, String)> = Vec::new();

    for attr in &input_ast.attrs {
        if attr.path().is_ident("single_table") {
            let parse_result = attr.parse_nested_meta(|meta| {
                let Some(segment) = meta.path.segments.last() else {
                    return Err(meta.error("single_table attribute key is missing"));
                };
                let ident = segment.ident.to_string();
                if ident == "entity_type" {
                    let expr: Expr = meta.value()?.parse()?;
                    entity_type = Some(expr);
                    return Ok(());
                }

                let val: LitStr = meta.value()?.parse()?;
                let s = val.value();
                match ident.as_str() {
                    "pk_lit" => pk_lit = Some(s),
                    "pk_expr" => pk_expr = Some(s),
                    "sk_expr" => sk_expr = Some(s),
                    "gsi1_pk_expr" => gsi1_pk_expr = Some(s),
                    "gsi1_sk_expr" => gsi1_sk_expr = Some(s),
                    "gsi2_pk_expr" => gsi2_pk_expr = Some(s),
                    "gsi2_sk_expr" => gsi2_sk_expr = Some(s),
                    "gsi3_pk_expr" => gsi3_pk_expr = Some(s),
                    "gsi3_sk_expr" => gsi3_sk_expr = Some(s),
                    "gsi4_pk_expr" => gsi4_pk_expr = Some(s),
                    "gsi4_sk_expr" => gsi4_sk_expr = Some(s),
                    "gsi5_pk_expr" => gsi5_pk_expr = Some(s),
                    "gsi5_sk_expr" => gsi5_sk_expr = Some(s),
                    // Pattern metadata keys (pure documentation, not used for logic)
                    key if key.starts_with("gsi_pattern_") => {
                        // Expect format gsi_pattern_<n> = "GPK|GSK" or "GPK" (if missing sk
                        // pattern)
                        if let Some((gpk, gsk)) = s.split_once('|') {
                            gsi_patterns.push((gpk.trim().to_string(), gsk.trim().to_string()));
                        } else {
                            // If only one side given, we store second as placeholder
                            gsi_patterns.push((s.trim().to_string(), "<sk>".to_string()));
                        }
                    }
                    other => {
                        return Err(
                            meta.error(format!("unknown single_table attribute key: {other}"))
                        );
                    }
                }
                Ok(())
            });
            if let Err(error) = parse_result {
                return error.to_compile_error().into();
            }
        }
    }
    let Some(entity_type) = entity_type else {
        return syn::Error::new_spanned(name, "entity_type required in #[single_table]")
            .to_compile_error()
            .into();
    };
    if pk_lit.is_none() && pk_expr.is_none() {
        return syn::Error::new_spanned(name, "one of pk_lit or pk_expr required")
            .to_compile_error()
            .into();
    }
    let Some(sk_expr) = sk_expr else {
        return syn::Error::new_spanned(name, "sk_expr required")
            .to_compile_error()
            .into();
    };

    // Build optional GSI methods: each *_expr returns Option<String>
    let gsi1_present = gsi1_pk_expr.is_some() && gsi1_sk_expr.is_some();
    let gsi1_impl = if gsi1_present {
        let (pk_code, sk_code) = match parse_gsi_expressions(
            name,
            1,
            gsi1_pk_expr.as_deref(),
            gsi1_sk_expr.as_deref(),
        ) {
            Ok(expressions) => expressions,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            fn gsi1(&self) -> Option<(String,String)> {
                let pk_opt: Option<String> = { #pk_code };
                let sk_opt: Option<String> = { #sk_code };
                match (pk_opt, sk_opt) {
                    (Some(p), Some(s)) => {
                        #[cfg(debug_assertions)] {
                            let _ = storage::types::validate_key_segment(&p);
                            let _ = storage::types::validate_full_key(&p, &s);
                        }
                        Some((p,s))
                    }
                    _ => None,
                }
            }
        }
    } else {
        quote! {}
    };
    let gsi2_present = gsi2_pk_expr.is_some() && gsi2_sk_expr.is_some();
    let gsi2_impl = if gsi2_present {
        let (pk_code, sk_code) = match parse_gsi_expressions(
            name,
            2,
            gsi2_pk_expr.as_deref(),
            gsi2_sk_expr.as_deref(),
        ) {
            Ok(expressions) => expressions,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            fn gsi2(&self) -> Option<(String,String)> {
                let pk_opt: Option<String> = { #pk_code };
                let sk_opt: Option<String> = { #sk_code };
                match (pk_opt, sk_opt) {
                    (Some(p), Some(s)) => {
                        #[cfg(debug_assertions)] {
                            let _ = storage::types::validate_key_segment(&p);
                            let _ = storage::types::validate_full_key(&p, &s);
                        }
                        Some((p,s))
                    }
                    _ => None,
                }
            }
        }
    } else {
        quote! {}
    };

    let gsi3_present = gsi3_pk_expr.is_some() && gsi3_sk_expr.is_some();
    let gsi3_impl = if gsi3_present {
        let (pk_code, sk_code) = match parse_gsi_expressions(
            name,
            3,
            gsi3_pk_expr.as_deref(),
            gsi3_sk_expr.as_deref(),
        ) {
            Ok(expressions) => expressions,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            fn gsi3(&self) -> Option<(String,String)> {
                let pk_opt: Option<String> = { #pk_code };
                let sk_opt: Option<String> = { #sk_code };
                match (pk_opt, sk_opt) {
                    (Some(p), Some(s)) => {
                        #[cfg(debug_assertions)] {
                            let _ = storage::types::validate_key_segment(&p);
                            let _ = storage::types::validate_full_key(&p, &s);
                        }
                        Some((p,s))
                    }
                    _ => None,
                }
            }
        }
    } else {
        quote! {}
    };

    let gsi4_present = gsi4_pk_expr.is_some() && gsi4_sk_expr.is_some();
    let gsi4_impl = if gsi4_present {
        let (pk_code, sk_code) = match parse_gsi_expressions(
            name,
            4,
            gsi4_pk_expr.as_deref(),
            gsi4_sk_expr.as_deref(),
        ) {
            Ok(expressions) => expressions,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            fn gsi4(&self) -> Option<(String,String)> {
                let pk_opt: Option<String> = { #pk_code };
                let sk_opt: Option<String> = { #sk_code };
                match (pk_opt, sk_opt) {
                    (Some(p), Some(s)) => {
                        #[cfg(debug_assertions)] {
                            let _ = storage::types::validate_key_segment(&p);
                            let _ = storage::types::validate_full_key(&p, &s);
                        }
                        Some((p,s))
                    }
                    _ => None,
                }
            }
        }
    } else {
        quote! {}
    };

    let gsi5_present = gsi5_pk_expr.is_some() && gsi5_sk_expr.is_some();
    let has_updated_at_millis = struct_has_timestamp_millis_field(&input_ast, "updated_at");
    if !has_updated_at_millis {
        return syn::Error::new_spanned(
            name,
            "single_table entity must include `updated_at: TimestampMillis`",
        )
        .to_compile_error()
        .into();
    }
    let gsi5_impl = if gsi5_present {
        let (pk_code, sk_code) = match parse_gsi_expressions(
            name,
            5,
            gsi5_pk_expr.as_deref(),
            gsi5_sk_expr.as_deref(),
        ) {
            Ok(expressions) => expressions,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            fn gsi5(&self) -> Option<(String,String)> {
                let pk_opt: Option<String> = { #pk_code };
                let sk_opt: Option<String> = { #sk_code };
                match (pk_opt, sk_opt) {
                    (Some(p), Some(s)) => {
                        #[cfg(debug_assertions)] {
                            let _ = storage::types::validate_key_segment(&p);
                            let _ = storage::types::validate_full_key(&p, &s);
                        }
                        Some((p,s))
                    }
                    _ => None,
                }
            }
        }
    } else {
        quote! {}
    };

    // Parse sk_expr as expression returning String
    let sk_expr_code = match syn::parse_str::<syn::Expr>(&sk_expr) {
        Ok(expr) => expr,
        Err(error) => return error.to_compile_error().into(),
    };

    // registration: build a static EntityLayout and submit via inventory
    let name_str = name.to_string();
    let _ = &gsi_patterns;
    let storage_entity_type_expr = match entity_type {
        Expr::Lit(syn::ExprLit {
            lit: Lit::Str(entity_type_literal),
            ..
        }) => quote! { #entity_type_literal },
        expr => quote! { #expr },
    };
    let entity_type_expr = quote! { #storage_entity_type_expr };
    let key_helpers = quote! {
        impl #name {
            #[must_use]
            pub fn pk_key(&self) -> storage::types::PartitionKey {
                storage::types::PartitionKey::string(
                    <Self as storage::types::single_table_entity::SingleTableEntity>::pk(self),
                )
            }

            #[must_use]
            pub fn sk_key(&self) -> storage::types::SortKey {
                storage::types::SortKey::string(
                    <Self as storage::types::single_table_entity::SingleTableEntity>::sk(self),
                )
            }

            #[must_use]
            pub fn table_keys(&self) -> storage::types::EntityKey {
                storage::types::EntityKey::pk_sk(self.pk_key(), self.sk_key())
            }

            #[must_use]
            pub fn gsi1_keys(&self) -> Option<(storage::types::PartitionKey, storage::types::SortKey)> {
                <Self as storage::types::single_table_entity::SingleTableEntity>::gsi1(self)
                    .map(|(pk, sk)| (
                        storage::types::PartitionKey::string(pk),
                        storage::types::SortKey::string(sk),
                    ))
            }

            #[must_use]
            pub fn gsi2_keys(&self) -> Option<(storage::types::PartitionKey, storage::types::SortKey)> {
                <Self as storage::types::single_table_entity::SingleTableEntity>::gsi2(self)
                    .map(|(pk, sk)| (
                        storage::types::PartitionKey::string(pk),
                        storage::types::SortKey::string(sk),
                    ))
            }

            #[must_use]
            pub fn gsi3_keys(&self) -> Option<(storage::types::PartitionKey, storage::types::SortKey)> {
                <Self as storage::types::single_table_entity::SingleTableEntity>::gsi3(self)
                    .map(|(pk, sk)| (
                        storage::types::PartitionKey::string(pk),
                        storage::types::SortKey::string(sk),
                    ))
            }

            #[must_use]
            pub fn gsi4_keys(&self) -> Option<(storage::types::PartitionKey, storage::types::SortKey)> {
                <Self as storage::types::single_table_entity::SingleTableEntity>::gsi4(self)
                    .map(|(pk, sk)| (
                        storage::types::PartitionKey::string(pk),
                        storage::types::SortKey::string(sk),
                    ))
            }

            #[must_use]
            pub fn gsi5_keys(&self) -> Option<(storage::types::PartitionKey, storage::types::SortKey)> {
                <Self as storage::types::single_table_entity::SingleTableEntity>::gsi5(self)
                    .map(|(pk, sk)| (
                        storage::types::PartitionKey::string(pk),
                        storage::types::SortKey::string(sk),
                    ))
            }

            #[must_use]
            pub fn key_ref_for<'a>(pk: &'a str, sk: &'a str) -> storage::types::KeyRef<'a> {
                storage::types::KeyRef::pk_sk(
                    storage::types::ScalarValueRef::S(pk),
                    storage::types::ScalarValueRef::S(sk),
                )
            }

            #[must_use]
            pub fn entity_key_for(pk: impl Into<String>, sk: impl Into<String>) -> storage::types::EntityKey {
                storage::types::EntityKey::pk_sk(pk.into(), sk.into())
            }

            #[must_use]
            pub fn key_owned_for(pk: impl Into<String>, sk: impl Into<String>) -> storage::types::KeyOwned {
                Self::entity_key_for(pk, sk).into()
            }
        }
    };

    let expanded = if let Some(pk_lit_val) = pk_lit.clone() {
        quote! {
            impl storage::types::single_table_entity::SingleTableEntity for #name {
                const STORAGE_ENTITY_TYPE: &'static str = #storage_entity_type_expr;
                const ENTITY_TYPE: &'static str = #entity_type_expr;
                fn pk(&self) -> String { #pk_lit_val.to_string() }
                fn sk(&self) -> String {
                    let v: String = { #sk_expr_code };
                    #[cfg(debug_assertions)] {
                        let _ = storage::types::validate_full_key(#pk_lit_val, &v);
                    }
                    v
                }
                #gsi1_impl
                #gsi2_impl
                #gsi3_impl
                #gsi4_impl
                #gsi5_impl
            }

            storage::types::inventory::submit! { storage::types::layout_registry::EntityLayout {
                name: #name_str,
                storage_entity_type: #storage_entity_type_expr,
                entity_type: #entity_type_expr,
                has_gsi5: #gsi5_present,
                has_updated_at_millis: #has_updated_at_millis,
            }}

            #key_helpers
        }
    } else {
        let Some(pk_expr) = pk_expr.as_deref() else {
            return syn::Error::new_spanned(name, "pk_expr required")
                .to_compile_error()
                .into();
        };
        let pk_expr_code = match syn::parse_str::<syn::Expr>(pk_expr) {
            Ok(expr) => expr,
            Err(error) => return error.to_compile_error().into(),
        };
        quote! {
            impl storage::types::single_table_entity::SingleTableEntity for #name {
                const STORAGE_ENTITY_TYPE: &'static str = #storage_entity_type_expr;
                const ENTITY_TYPE: &'static str = #entity_type_expr;
                fn pk(&self) -> String {
                    let p: String = { #pk_expr_code };
                    #[cfg(debug_assertions)] {
                        let _ = storage::types::validate_key_segment(&p);
                    }
                    p
                }
                fn sk(&self) -> String {
                    let v: String = { #sk_expr_code };
                    #[cfg(debug_assertions)] {
                        // dynamic pk not known at compile time for validation pair
                    }
                    v
                }
                #gsi1_impl
                #gsi2_impl
                #gsi3_impl
                #gsi4_impl
                #gsi5_impl
            }

            storage::types::inventory::submit! { storage::types::layout_registry::EntityLayout {
                name: #name_str,
                storage_entity_type: #storage_entity_type_expr,
                entity_type: #entity_type_expr,
                has_gsi5: #gsi5_present,
                has_updated_at_millis: #has_updated_at_millis,
            }}

            #key_helpers
        }
    };
    TokenStream::from(expanded)
}

pub(crate) fn parse_gsi_expressions(
    span: &syn::Ident,
    gsi_number: u8,
    pk_expr: Option<&str>,
    sk_expr: Option<&str>,
) -> Result<(syn::Expr, syn::Expr), syn::Error> {
    let pk_expr = pk_expr.ok_or_else(|| {
        syn::Error::new_spanned(span, format!("gsi{gsi_number}_pk_expr required"))
    })?;
    let sk_expr = sk_expr.ok_or_else(|| {
        syn::Error::new_spanned(span, format!("gsi{gsi_number}_sk_expr required"))
    })?;
    let pk_code = syn::parse_str::<syn::Expr>(pk_expr).map_err(|error| {
        syn::Error::new_spanned(
            span,
            format!("invalid gsi{gsi_number}_pk_expr expression: {error}"),
        )
    })?;
    let sk_code = syn::parse_str::<syn::Expr>(sk_expr).map_err(|error| {
        syn::Error::new_spanned(
            span,
            format!("invalid gsi{gsi_number}_sk_expr expression: {error}"),
        )
    })?;
    Ok((pk_code, sk_code))
}
