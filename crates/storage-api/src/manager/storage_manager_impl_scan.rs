use http_error::HttpApiError;
use storage::ScanTableInput;
use storage_types::{ScanRequest, ScanResponse, validate_expression_attribute_usage};

use crate::{
    manager::{
        StorageApiManagerImpl,
        storage_manager_impl_consumed_capacity::calculate_consumed_capacity_from_inputs,
        storage_manager_impl_expression::{
            apply_filter_expression_refs, apply_projection_expression_refs,
        },
        storage_manager_impl_read_pagination::{
            page_token_to_key_attributes, paginate_items_by_response_bytes,
            resolve_exclusive_start_key,
        },
    },
    types::Response,
};

/// Handles a `DynamoDB` Scan operation
///
/// # Errors
/// Returns `HttpApiError` if:
/// - The table does not exist
/// - The scan operation fails
/// - Filter or projection expressions are invalid
impl StorageApiManagerImpl {
    pub(super) async fn scan_internal(
        &self,
        request: ScanRequest,
    ) -> Result<Response, HttpApiError> {
        let mut scan_expressions = Vec::new();
        if let Some(filter_expr) = request.filter_expression.as_deref() {
            scan_expressions.push(filter_expr);
        }
        if let Some(projection_expr) = request.projection_expression.as_deref() {
            scan_expressions.push(projection_expr);
        }
        validate_expression_attribute_usage(
            request.expression_attribute_names.as_ref(),
            request.expression_attribute_values.as_ref(),
            scan_expressions,
        )
        .map_err(HttpApiError::from)?;

        if request.index_name.is_some() && request.consistent_read == Some(true) {
            return Err(HttpApiError::dynamodb_protocol_error(
                "ValidationException",
                "Consistent reads are not supported on global secondary indexes",
                400,
            ));
        }
        self.ensure_sync_read_barrier(request.consistent_read.unwrap_or(false))
            .await?;

        let table_info = self.db().get_table_info(&request.table_name).await?;
        let exclusive_start_key = resolve_exclusive_start_key(
            request.exclusive_start_key.as_ref(),
            &table_info,
            request.index_name.as_ref(),
            "The provided starting key is invalid: The provided key element does not match the \
             schema",
        )
        .map_err(HttpApiError::from)?;

        let scan_input = ScanTableInput {
            table_name: request.table_name.clone(),
            index_name: request.index_name.clone(),
            limit: request.limit,
            exclusive_start_key,
            consistent_read: request.consistent_read.unwrap_or(false),
        };

        let (items, last_evaluated_key) = self.db().scan_table_map(scan_input).await?;

        #[expect(clippy::cast_possible_truncation)]
        let (filtered_items, count, scanned_count) =
            if let Some(filter_expr) = &request.filter_expression {
                let filtered = apply_filter_expression_refs(
                    &items,
                    filter_expr,
                    request.expression_attribute_names.as_ref(),
                    request.expression_attribute_values.as_ref(),
                )?;
                let filtered_len = filtered.len() as u32;
                (filtered, filtered_len, items.len() as u32)
            } else {
                let count = items.len() as u32;
                (items.iter().collect::<Vec<_>>(), count, count)
            };

        let (response_items, count, last_evaluated_key) =
            if request.select.as_deref() == Some("COUNT") {
                (None, count, last_evaluated_key)
            } else {
                let final_items = if let Some(projection_expr) = &request.projection_expression {
                    apply_projection_expression_refs(
                        &filtered_items,
                        projection_expr,
                        request.expression_attribute_names.as_ref(),
                    )
                } else if let Some(attributes) = &request.attributes_to_get {
                    let projection_expr = attributes.join(", ");
                    apply_projection_expression_refs(
                        &filtered_items,
                        &projection_expr,
                        request.expression_attribute_names.as_ref(),
                    )
                } else {
                    let mut out = Vec::with_capacity(filtered_items.len());
                    for item in &filtered_items {
                        out.push((*item).clone());
                    }
                    out
                };
                let (page_items, page_lek) = paginate_items_by_response_bytes(
                    &request.table_name,
                    &table_info,
                    request.index_name.as_ref(),
                    &filtered_items,
                    final_items,
                    last_evaluated_key,
                )
                .map_err(HttpApiError::from)?;
                #[expect(clippy::cast_possible_truncation)]
                let page_count = page_items.len() as u32;
                (Some(page_items), page_count, page_lek)
            };
        let last_evaluated_key = page_token_to_key_attributes(
            last_evaluated_key.as_deref(),
            &table_info,
            request.index_name.as_ref(),
        )
        .map_err(HttpApiError::from)?;

        let response = ScanResponse {
            items: response_items,
            count,
            scanned_count,
            last_evaluated_key,
            consumed_capacity: calculate_consumed_capacity_from_inputs(
                request.return_consumed_capacity.as_deref(),
                &request.table_name,
                request.index_name.as_ref(),
                scanned_count,
            ),
        };

        Ok(Response::Scan(response))
    }
}
