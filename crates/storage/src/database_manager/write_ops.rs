use storage_types::{
    AttributeMap, DeleteItemRequest, ExprNameRef, ExprValueRef, KeyRef, PutItemRequest,
    PutItemResponse, ReturnValuesOldNewUpdated, StorageResult, TableName, UpdateItemRequest,
    UpdateItemResponse, WireItem, expr_names_to_map, expr_values_to_map,
};

use crate::{
    admission::AdmissionClass,
    database_manager::{
        DatabaseManager, DeleteItemInput, PutItemInput, UpdateItemInput, record_storage_operation,
    },
    updated_at_apply::{inject_updated_at_into_update_expression, stamp_wire_item_now},
};

impl DatabaseManager {
    pub async fn put_item(&self, input: PutItemInput) -> StorageResult<PutItemResponse> {
        let mut item = WireItem::try_from(input.item)?;
        if self.single_table_mode_enabled() {
            stamp_wire_item_now(&mut item)?;
        }
        let mut request = PutItemRequest::new(input.table_name, item);
        request.indexers = input.indexers;
        request.condition_expression = input.condition_expression;
        request.expression_attribute_names = input.expression_attribute_names;
        request.expression_attribute_values = input.expression_attribute_values;
        request.return_values = input.return_values;
        request.return_values_on_condition_check_failure = input
            .return_old_on_condition_failure
            .then(|| "ALL_OLD".to_string());
        request.aux_item_stream_ttl_hours = input.aux_item_stream_ttl_hours;
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("put_item", dynamo.put_item(request)).await
        })
        .await
    }

    pub async fn delete_item(&self, input: DeleteItemInput) -> StorageResult<Option<AttributeMap>> {
        let request = DeleteItemRequest {
            table_name: input.table_name,
            key: input.key,
            condition_expression: input.condition_expression,
            expression_attribute_names: input.expression_attribute_names,
            expression_attribute_values: input.expression_attribute_values,
            expected: None,
            conditional_operator: None,
            return_values: input.return_values,
            return_consumed_capacity: None,
            return_item_collection_metrics: None,
            return_values_on_condition_check_failure: input
                .return_old_on_condition_failure
                .then(|| "ALL_OLD".to_string()),
            aux_item_stream_ttl_hours: input.aux_item_stream_ttl_hours,
        };
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("delete_item", dynamo.delete_item(request))
                .await
                .map(|response| response.attributes)
        })
        .await
    }

    pub async fn update_item(
        &self,
        mut input: UpdateItemInput,
    ) -> StorageResult<UpdateItemResponse> {
        if self.single_table_mode_enabled() && !input.update_expression.trim().is_empty() {
            inject_updated_at_into_update_expression(
                &mut input.update_expression,
                &mut input.expression_attribute_names,
                &mut input.expression_attribute_values,
            )?;
        }
        let mut request = UpdateItemRequest::builder()
            .table_name(input.table_name)
            .key(input.key)
            .indexers(input.indexers)
            .condition_expression(input.condition_expression)
            .expression_attribute_names(input.expression_attribute_names)
            .expression_attribute_values(input.expression_attribute_values)
            .return_values(input.return_values)
            .return_values_on_condition_check_failure(
                input
                    .return_old_on_condition_failure
                    .then(|| "ALL_OLD".to_string()),
            )
            .aux_item_stream_ttl_hours(input.aux_item_stream_ttl_hours)
            .build();
        request.update_expression =
            (!input.update_expression.trim().is_empty()).then_some(input.update_expression);
        self.run_dynamo_admitted("default", AdmissionClass::Write, |dynamo| async move {
            record_storage_operation("update_item", dynamo.update_item(request)).await
        })
        .await
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "API shape follows update_item request fields"
    )]
    pub async fn update_item_ref(
        &self,
        table_name: TableName,
        key: KeyRef<'_>,
        update_expression: String,
        condition_expression: Option<String>,
        expression_attribute_names: Option<&[ExprNameRef<'_>]>,
        expression_attribute_values: Option<&[ExprValueRef<'_>]>,
        return_values: Option<ReturnValuesOldNewUpdated>,
    ) -> StorageResult<UpdateItemResponse> {
        let expression_attribute_names = expression_attribute_names.map(expr_names_to_map);
        let expression_attribute_values = expression_attribute_values.map(expr_values_to_map);
        self.update_item(UpdateItemInput {
            table_name,
            key: key.to_map().into(),
            update_expression,
            indexers: None,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            return_old_on_condition_failure: false,
            aux_item_stream_ttl_hours: None,
        })
        .await
    }
}
