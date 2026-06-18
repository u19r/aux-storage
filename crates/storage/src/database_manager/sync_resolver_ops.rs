use std::collections::HashMap;

use async_trait::async_trait;
use storage_condition::evaluate_condition;
use storage_provider::{
    apply_bound_update_operations, before_update_item_optional, update_item_response,
};
use storage_sync::{
    ResolvedSyncMutation, ResolvedSyncMutationBatch, SyncCreateTableMutation, SyncDeleteMutation,
    SyncDeleteTableMutation, SyncItemBaseVersion, SyncMutationId, SyncMutationResolver,
    SyncMutationResponse, SyncProposalBatch, SyncPutMutation, SyncReadSet, SyncUpdateTableMutation,
    SyncUpdateTimeToLiveMutation, SyncWriteProposalRequest, SyncWriteRequest,
};
use storage_types::{
    AttributeValue, DurablePointReadProof, DurablePointReadRequest, ItemStreamVersion,
    KeyAttributes, StorageEnum, StorageError, StorageResult, TableName,
    validate_expression_attribute_usage,
};

use crate::{
    DatabaseManager,
    database_manager::{
        PutItemPayload, refresh_existing_updated_at_on_put_payload,
        sync_condition_ops::evaluate_optional_condition,
        sync_serialization::{stable_attribute_json, stable_key_json},
        validate_update_expression_usage,
    },
};

#[async_trait]
impl SyncMutationResolver for DatabaseManager {
    type Request = SyncWriteProposalRequest;

    async fn resolve_sync_mutation(
        &self,
        request: Self::Request,
    ) -> StorageResult<SyncProposalBatch> {
        let mut resolver = SyncWriteResolver::new(self, request.proposal_id.clone());
        match request.request {
            SyncWriteRequest::PutItem(request) => {
                resolver
                    .resolve_put(
                        request.table_name,
                        request.item,
                        request.condition_expression,
                        request.expression_attribute_names,
                        request.expression_attribute_values,
                        request.return_values,
                    )
                    .await?;
            }
            SyncWriteRequest::DeleteItem(request) => {
                resolver
                    .resolve_delete(
                        request.table_name,
                        request.key,
                        request.condition_expression,
                        request.expression_attribute_names,
                        request.expression_attribute_values,
                    )
                    .await?;
            }
            SyncWriteRequest::BatchWriteItem(request) => {
                for (table_name, writes) in request.request_items {
                    for write in writes {
                        match (write.put_request, write.delete_request) {
                            (Some(put), None) => {
                                resolver
                                    .resolve_put(
                                        table_name.clone(),
                                        put.item,
                                        None,
                                        None,
                                        None,
                                        None,
                                    )
                                    .await?;
                            }
                            (None, Some(delete)) => {
                                resolver
                                    .resolve_delete(
                                        table_name.clone(),
                                        delete.key,
                                        None,
                                        None,
                                        None,
                                    )
                                    .await?;
                            }
                            _ => {
                                return Err(StorageError::validation(
                                    "BatchWriteItem entries must contain exactly one PutRequest \
                                     or DeleteRequest",
                                ));
                            }
                        }
                    }
                }
            }
            SyncWriteRequest::UpdateItem(request) => {
                resolver.resolve_update(request).await?;
            }
            SyncWriteRequest::TransactWriteItems(request) => {
                resolver.resolve_transact_write_items(request).await?;
            }
            SyncWriteRequest::CreateTable(request) => resolver.resolve_create_table(request)?,
            SyncWriteRequest::UpdateTable(request) => resolver.resolve_update_table(request)?,
            SyncWriteRequest::DeleteTable(request) => resolver.resolve_delete_table(request)?,
            SyncWriteRequest::UpdateTimeToLive(request) => {
                resolver.resolve_update_time_to_live(request)?;
            }
        }
        Ok(resolver.finish())
    }
}

pub(super) struct SyncWriteResolver<'a> {
    db: &'a DatabaseManager,
    proposal_id: storage_sync::SyncProposalId,
    mutations: Vec<ResolvedSyncMutation>,
    read_set: Vec<SyncItemBaseVersion>,
    overlay: Vec<OverlayItem>,
}

struct OverlayItem {
    table_name: TableName,
    key_json: String,
    state: SyncItemState,
}

#[derive(Clone)]
pub(super) struct SyncItemState {
    pub(super) item: Option<HashMap<String, AttributeValue>>,
    pub(super) item_stream_version: ItemStreamVersion,
}

impl<'a> SyncWriteResolver<'a> {
    fn new(db: &'a DatabaseManager, proposal_id: storage_sync::SyncProposalId) -> Self {
        Self {
            db,
            proposal_id,
            mutations: Vec::new(),
            read_set: Vec::new(),
            overlay: Vec::new(),
        }
    }

    fn resolve_create_table(
        &mut self,
        request: storage_types::CreateTableRequest,
    ) -> StorageResult<()> {
        self.mutations
            .push(ResolvedSyncMutation::CreateTable(SyncCreateTableMutation {
                mutation_id: self.next_mutation_id()?,
                table_name: request.table_name.clone(),
                request_json: serde_json::to_string(&request)?,
            }));
        Ok(())
    }

    fn resolve_update_table(
        &mut self,
        request: storage_types::UpdateTableRequest,
    ) -> StorageResult<()> {
        self.mutations
            .push(ResolvedSyncMutation::UpdateTable(SyncUpdateTableMutation {
                mutation_id: self.next_mutation_id()?,
                table_name: request.table_name.clone(),
                request_json: serde_json::to_string(&request)?,
            }));
        Ok(())
    }

    fn resolve_delete_table(
        &mut self,
        request: storage_types::DeleteTableRequest,
    ) -> StorageResult<()> {
        self.mutations
            .push(ResolvedSyncMutation::DeleteTable(SyncDeleteTableMutation {
                mutation_id: self.next_mutation_id()?,
                table_name: request.table_name.clone(),
                request_json: serde_json::to_string(&request)?,
            }));
        Ok(())
    }

    fn resolve_update_time_to_live(
        &mut self,
        request: storage_types::UpdateTimeToLiveRequest,
    ) -> StorageResult<()> {
        self.mutations.push(ResolvedSyncMutation::UpdateTimeToLive(
            SyncUpdateTimeToLiveMutation {
                mutation_id: self.next_mutation_id()?,
                table_name: request.table_name.clone(),
                request_json: serde_json::to_string(&request)?,
            },
        ));
        Ok(())
    }

    pub(super) async fn resolve_put(
        &mut self,
        table_name: TableName,
        item: HashMap<String, AttributeValue>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
        return_values: Option<storage_types::AllOld>,
    ) -> StorageResult<()> {
        validate_expression_attribute_usage(
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            condition_expression.as_deref(),
        )?;
        let table_info = self
            .db
            .storage_provider()
            .get_table_info(&table_name)
            .await?;
        let mut payload = PutItemPayload::from(item);
        refresh_existing_updated_at_on_put_payload(&mut payload)?;
        let item = payload.into_attribute_map()?;
        let key = self
            .db
            .storage_provider()
            .get_key_attributes(&item, &table_info.key_schema)?;
        let key_json = stable_key_json(&key)?;
        let old_state = self.current_item(&table_name, &key, &key_json).await?;
        let old_item = old_state.item.as_ref();
        evaluate_optional_condition(
            old_item,
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        self.record_read(&table_name, &key_json, &old_state);

        let target_item_stream_version =
            next_sync_item_stream_version(old_state.item_stream_version)?;
        let mutation_id = self.next_mutation_id()?;
        let item_json = stable_attribute_json(&item)?;
        self.record_overlay(
            &table_name,
            &key_json,
            SyncItemState {
                item: Some(item),
                item_stream_version: target_item_stream_version,
            },
        );
        let response = match return_values {
            Some(storage_types::AllOld::AllOld) => {
                sync_response_json(&storage_types::PutItemResponse {
                    attributes: old_state.item.clone().map(Into::into),
                })?
            }
            None | Some(storage_types::AllOld::None) => SyncMutationResponse::default(),
        };
        self.mutations
            .push(ResolvedSyncMutation::Put(SyncPutMutation {
                mutation_id,
                table_name,
                key_json,
                item_json,
                old_item_json: old_item.map(stable_attribute_json).transpose()?,
                target_item_stream_version,
                response,
            }));
        Ok(())
    }

    pub(super) async fn resolve_delete(
        &mut self,
        table_name: TableName,
        key: KeyAttributes,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> StorageResult<()> {
        validate_expression_attribute_usage(
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
            condition_expression.as_deref(),
        )?;
        let key_json = stable_key_json(&key)?;
        let old_state = self.current_item(&table_name, &key, &key_json).await?;
        let old_item = old_state.item.as_ref();
        evaluate_optional_condition(
            old_item,
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        self.record_read(&table_name, &key_json, &old_state);

        let target_item_stream_version =
            next_sync_item_stream_version(old_state.item_stream_version)?;
        let mutation_id = self.next_mutation_id()?;
        self.record_overlay(
            &table_name,
            &key_json,
            SyncItemState {
                item: None,
                item_stream_version: target_item_stream_version,
            },
        );
        self.mutations
            .push(ResolvedSyncMutation::Delete(SyncDeleteMutation {
                mutation_id,
                table_name,
                key_json,
                old_item_json: old_item.map(stable_attribute_json).transpose()?,
                target_item_stream_version,
                response: SyncMutationResponse::default(),
            }));
        Ok(())
    }

    pub(super) async fn resolve_update(
        &mut self,
        request: storage_types::UpdateItemRequest,
    ) -> StorageResult<()> {
        let storage_types::UpdateItemRequest {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values,
            ..
        } = request;
        validate_update_expression_usage(
            update_expression.as_deref(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let (operations, condition) = before_update_item_optional(
            update_expression.as_deref(),
            condition_expression.as_deref(),
            expression_attribute_names.as_ref(),
            expression_attribute_values.as_ref(),
        )?;
        let key_json = stable_key_json(&key)?;
        let old_state = self.current_item(&table_name, &key, &key_json).await?;
        let old_item = old_state.item.as_ref();
        if let Some(condition) = condition {
            let empty_item;
            let condition_item = match old_item {
                Some(item) => item,
                None => {
                    empty_item = HashMap::new();
                    &empty_item
                }
            };
            if !evaluate_condition(condition_item, &condition) {
                return Err(StorageEnum::ConditionalCheckFailed.into());
            }
        }
        self.record_read(&table_name, &key_json, &old_state);

        let item_to_update = old_state
            .item
            .clone()
            .unwrap_or_else(|| key.to_attribute_map());
        let updated_item = apply_bound_update_operations(item_to_update, &operations)?;
        let table_info = self
            .db
            .storage_provider()
            .get_table_info(&table_name)
            .await?;
        self.db
            .storage_provider()
            .get_key_attributes(&updated_item, &table_info.key_schema)?;
        let response = match return_values.as_ref() {
            None | Some(storage_types::ReturnValuesOldNewUpdated::None) => {
                SyncMutationResponse::default()
            }
            Some(return_values) => sync_response_json(&update_item_response(
                &operations,
                old_state.item.clone(),
                Some(updated_item.clone()),
                Some(return_values),
            )?)?,
        };
        let target_item_stream_version =
            next_sync_item_stream_version(old_state.item_stream_version)?;
        let mutation_id = self.next_mutation_id()?;
        self.record_overlay(
            &table_name,
            &key_json,
            SyncItemState {
                item: Some(updated_item.clone()),
                item_stream_version: target_item_stream_version,
            },
        );
        self.mutations
            .push(ResolvedSyncMutation::Put(SyncPutMutation {
                mutation_id,
                table_name,
                key_json,
                item_json: stable_attribute_json(&updated_item)?,
                old_item_json: old_item.map(stable_attribute_json).transpose()?,
                target_item_stream_version,
                response,
            }));
        Ok(())
    }

    pub(super) async fn current_item(
        &self,
        table_name: &TableName,
        key: &KeyAttributes,
        key_json: &str,
    ) -> StorageResult<SyncItemState> {
        if let Some(state) = self.overlay_item(table_name, key_json) {
            return Ok(state.clone());
        }
        let proof = self
            .db
            .storage_provider()
            .get_item_with_durable_proof(DurablePointReadRequest {
                table_name: table_name.clone(),
                key: key.clone(),
                consistent_read: true,
            })
            .await?;
        sync_item_state_from_proof(proof)
    }

    fn overlay_item(&self, table_name: &TableName, key_json: &str) -> Option<&SyncItemState> {
        self.overlay
            .iter()
            .rev()
            .find(|item| item.table_name == *table_name && item.key_json == key_json)
            .map(|item| &item.state)
    }

    fn record_overlay(&mut self, table_name: &TableName, key_json: &str, state: SyncItemState) {
        self.overlay.push(OverlayItem {
            table_name: table_name.clone(),
            key_json: key_json.to_string(),
            state,
        });
    }

    pub(super) fn record_read(
        &mut self,
        table_name: &TableName,
        key_json: &str,
        state: &SyncItemState,
    ) {
        self.read_set.push(SyncItemBaseVersion {
            table_name: table_name.clone(),
            key_json: key_json.to_string(),
            item_stream_version: state.item.as_ref().map(|_| state.item_stream_version),
        });
    }

    fn next_mutation_id(&self) -> StorageResult<SyncMutationId> {
        let mutation_index = self.mutations.len();
        let mut mutation_id = String::with_capacity(
            self.proposal_id
                .as_str()
                .len()
                .saturating_add(1)
                .saturating_add(decimal_digits(mutation_index)),
        );
        mutation_id.push_str(self.proposal_id.as_str());
        mutation_id.push('#');
        mutation_id.push_str(&mutation_index.to_string());
        SyncMutationId::new(mutation_id)
            .map_err(|error| StorageError::validation(error.to_string()))
    }

    fn finish(self) -> SyncProposalBatch {
        SyncProposalBatch::new(
            self.proposal_id,
            ResolvedSyncMutationBatch::new(self.mutations),
        )
        .with_read_set(SyncReadSet::new(self.read_set))
    }
}

fn decimal_digits(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn sync_item_state_from_proof(proof: DurablePointReadProof) -> StorageResult<SyncItemState> {
    match proof {
        DurablePointReadProof::Present { item, revision } => Ok(SyncItemState {
            item: Some(item.into_attribute_map()?),
            item_stream_version: ItemStreamVersion::try_from(revision.as_bytes()).map_err(
                |error| {
                    StorageError::validation(format!("durable item revision is invalid: {error}"))
                },
            )?,
        }),
        DurablePointReadProof::Absent { proof } => Ok(SyncItemState {
            item: None,
            item_stream_version: ItemStreamVersion::try_from(proof.as_bytes()).map_err(
                |error| {
                    StorageError::validation(format!("durable absence proof is invalid: {error}"))
                },
            )?,
        }),
    }
}

fn next_sync_item_stream_version(
    current_version: ItemStreamVersion,
) -> StorageResult<ItemStreamVersion> {
    current_version
        .checked_increment()
        .ok_or_else(|| StorageError::validation("item stream version overflow during sync resolve"))
}

fn sync_response_json<T>(response: &T) -> StorageResult<SyncMutationResponse>
where T: serde::Serialize {
    Ok(SyncMutationResponse {
        response_json: Some(serde_json::to_string(response)?),
    })
}
