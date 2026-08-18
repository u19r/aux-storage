use std::collections::HashSet;

use syn::{Data, DeriveInput, Fields, LitInt};

use crate::wire_item_helpers::{
    parse_wire_item_field_attributes, parse_wire_item_struct_serde_rename_all,
};

const MAX_ENTITY_INDEXERS: usize = 32;

#[derive(Debug)]
pub(crate) struct EntityIndexerField {
    pub(crate) field: syn::Ident,
    pub(crate) attribute_name: String,
    pub(crate) ordinal: u8,
}

pub(crate) fn parse_entity_indexers(
    input: &DeriveInput,
) -> Result<Vec<EntityIndexerField>, syn::Error> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "SingleTableKeys requires a struct",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "SingleTableKeys requires named fields",
        ));
    };
    let rename_all = parse_wire_item_struct_serde_rename_all(input);
    let mut indexers = Vec::new();
    for field in &fields.named {
        let Some(ordinal) = parse_indexer_ordinal(field)? else {
            continue;
        };
        let Some(field_name) = field.ident.clone() else {
            return Err(syn::Error::new_spanned(
                field,
                "indexer field name is missing",
            ));
        };
        let attributes = parse_wire_item_field_attributes(field, rename_all.as_deref())?;
        indexers.push(EntityIndexerField {
            field: field_name,
            attribute_name: attributes.wire_name,
            ordinal,
        });
    }
    validate_indexers(&mut indexers)?;
    Ok(indexers)
}

fn parse_indexer_ordinal(field: &syn::Field) -> Result<Option<u8>, syn::Error> {
    let mut ordinal = None;
    for attribute in &field.attrs {
        if !attribute.path().is_ident("single_table") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("indexer") {
                return Err(meta.error("only indexer is valid on single-table entity fields"));
            }
            if ordinal.is_some() {
                return Err(meta.error("indexer ordinal is declared more than once"));
            }
            let value: LitInt = meta.value()?.parse()?;
            ordinal = Some(value.base10_parse::<u8>()?);
            Ok(())
        })?;
    }
    Ok(ordinal)
}

fn validate_indexers(indexers: &mut [EntityIndexerField]) -> Result<(), syn::Error> {
    if indexers.len() > MAX_ENTITY_INDEXERS {
        return Err(syn::Error::new_spanned(
            &indexers[MAX_ENTITY_INDEXERS].field,
            "single-table entities support at most 32 indexers",
        ));
    }
    indexers.sort_unstable_by_key(|indexer| indexer.ordinal);
    let mut names = HashSet::with_capacity(indexers.len());
    for (expected, indexer) in indexers.iter().enumerate() {
        if usize::from(indexer.ordinal) != expected {
            return Err(syn::Error::new_spanned(
                &indexer.field,
                format!("indexer ordinals must be contiguous from 0; expected {expected}"),
            ));
        }
        if indexer.attribute_name.is_empty() {
            return Err(syn::Error::new_spanned(
                &indexer.field,
                "indexer attribute name cannot be empty",
            ));
        }
        if !names.insert(indexer.attribute_name.as_str()) {
            return Err(syn::Error::new_spanned(
                &indexer.field,
                format!(
                    "indexer attribute name `{}` is duplicated",
                    indexer.attribute_name
                ),
            ));
        }
    }
    Ok(())
}
