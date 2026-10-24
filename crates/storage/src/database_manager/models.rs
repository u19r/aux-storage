use storage_types::{
    AllOld, IndexName, KeyAttributes, QueryTableRequest, ReturnValuesOldNewUpdated, TableName,
    TransactEncodeItem,
};
use typed_builder::TypedBuilder;

use super::wire_item_ops::PutItemPayload;

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct PutItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub item: PutItemPayload,
    pub indexers: Option<Vec<String>>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    pub return_values: Option<AllOld>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct PutItemEntityEncodeInput<'a, T> {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option, !into))]
    pub item: &'a T,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    pub return_values: Option<AllOld>,
}

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct DeleteItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(setter(!strip_option))]
    pub key: KeyAttributes,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    pub return_values: Option<AllOld>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct QueryTableInput {
    #[builder(!default)]
    pub table_name: TableName,
    #[builder(!default)]
    pub key_condition_expression: String,
    #[builder(setter(strip_option))]
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    #[builder(setter(strip_option))]
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[builder(setter(strip_option))]
    pub exclusive_start_key: Option<String>,
    #[builder(setter(strip_option))]
    pub scan_index_forward: Option<bool>,
    pub consistent_read: bool,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct QueryIndexInput {
    #[builder(!default)]
    pub table_name: TableName,
    #[builder(!default)]
    pub index_name: IndexName,
    #[builder(!default)]
    pub key_condition_expression: String,
    #[builder(setter(strip_option))]
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    #[builder(setter(strip_option))]
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    #[builder(setter(strip_option))]
    pub projection_expression: Option<String>,
    #[builder(setter(strip_option))]
    pub limit: Option<u32>,
    #[builder(setter(strip_option))]
    pub exclusive_start_key: Option<String>,
    #[builder(setter(strip_option))]
    pub scan_index_forward: Option<bool>,
}

impl From<QueryTableInput> for QueryTableRequest {
    fn from(input: QueryTableInput) -> Self {
        Self {
            table_name: input.table_name,
            index_name: None,
            key_condition_expression: input.key_condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            projection_expression: None,
            limit: input.limit,
            exclusive_start_key: input.exclusive_start_key,
            scan_index_forward: input.scan_index_forward,
            consistent_read: input.consistent_read,
        }
    }
}

impl From<QueryIndexInput> for QueryTableRequest {
    fn from(input: QueryIndexInput) -> Self {
        Self {
            table_name: input.table_name,
            index_name: Some(input.index_name),
            key_condition_expression: input.key_condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            projection_expression: input.projection_expression,
            limit: input.limit,
            exclusive_start_key: input.exclusive_start_key,
            scan_index_forward: input.scan_index_forward,
            consistent_read: false,
        }
    }
}

#[derive(Clone, TypedBuilder)]
#[builder(field_defaults(default, setter(strip_option, into)))]
pub struct UpdateItemInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub key: KeyAttributes,
    #[builder(!default, setter(!strip_option))]
    pub update_expression: String,
    pub indexers: Option<Vec<String>>,
    pub condition_expression: Option<String>,
    pub expression_attribute_names: Option<storage_types::VecMap<String, String>>,
    pub expression_attribute_values: Option<storage_types::AttributeMap>,
    pub return_values: Option<ReturnValuesOldNewUpdated>,
    #[builder(setter(!strip_option))]
    pub return_old_on_condition_failure: bool,
    pub aux_item_stream_ttl_hours: Option<storage_types::StreamRetentionDuration>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct CreateCappedEntityInput<'a, T> {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option, !into))]
    pub item: &'a T,
    #[builder(!default, setter(!strip_option))]
    pub counted_entity_type: String,
    #[builder(!default, setter(!strip_option))]
    pub max_value: u64,
    pub additional_transact_items: Vec<TransactEncodeItem>,
    /// Native worker authority for the entity, counter, and side items.
    pub job_effect_authority: Option<scheduled_bg_jobs::JobEffectAuthority>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
pub struct DeleteCappedEntityInput {
    #[builder(!default, setter(!strip_option))]
    pub table_name: TableName,
    #[builder(!default, setter(!strip_option))]
    pub key: KeyAttributes,
    #[builder(!default, setter(!strip_option))]
    pub counted_entity_type: String,
}

impl TryFrom<UpdateItemInput> for storage_types::TransactUpdateRequest {
    type Error = storage_types::StorageError;

    fn try_from(input: UpdateItemInput) -> Result<Self, Self::Error> {
        match input.return_values {
            None | Some(ReturnValuesOldNewUpdated::None) => {}
            Some(_) => {
                return Err(storage_types::StorageError::validation(
                    "transaction updates cannot return item attributes",
                ));
            }
        }
        Ok(Self {
            table_name: input.table_name,
            key: input.key,
            update_expression: input.update_expression,
            indexers: input.indexers,
            condition_expression: input.condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            return_values_on_condition_check_failure: input
                .return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours: input.aux_item_stream_ttl_hours,
        })
    }
}
