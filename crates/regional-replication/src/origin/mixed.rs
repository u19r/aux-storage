use std::sync::Arc;

use infrastructure_provider::physical_attempt::PhysicalRecordAttempt;
use replication_types::event::content::MutationCause;
use storage_provider::{
    AcceptedBatchWriteMember, AcceptedMutationRef, AcceptedTransactWriteMember, CachePublication,
    DynamoAttempt, DynamoBatchWriteShape, DynamoPort, DynamoTransactWriteShape, WriteTableBinding,
    resolve_tables_batch_in_attempt,
};
use storage_types::{
    BatchWriteItemResponse, ItemProvenance, StorageError, StorageResult, TableName,
    TransactWriteItemsResponse,
};

use super::{
    OriginExecution, OriginFailure, OriginOperationKind,
    executor::{OriginAttemptContext, OriginAttemptOutput, OriginOperation, execute_operation},
};
use crate::{
    journal::{
        TableStateObservation,
        origin::{
            CapturedOriginMutation, OriginCaptureInput, OriginMutation, capture_origin_mutations,
            select_batch_mutations,
        },
    },
    native_record::corrupt_record,
};

struct OperationTable {
    name: TableName,
    binding: WriteTableBinding,
    capture: Option<TableStateObservation>,
}

async fn load_tables<A: DynamoAttempt + PhysicalRecordAttempt>(
    attempt: &mut A,
    context: &OriginAttemptContext,
    names: Vec<TableName>,
) -> StorageResult<(Vec<OperationTable>, Vec<CachePublication>)> {
    let mut tables = Vec::with_capacity(names.len());
    let mut publications = Vec::new();
    let resolved = resolve_tables_batch_in_attempt(attempt, &context.cache, &names)
        .await?
        .into_iter()
        .collect::<StorageResult<Vec<_>>>()?;
    let bound = resolved
        .iter()
        .map(|(table, _)| table.as_ref())
        .collect::<Vec<_>>();
    let capture = context.journal.load_capture_states(attempt, &bound).await?;
    for state in &capture {
        context.journal.require_origin_admission(state.as_ref())?;
    }
    let ttl = attempt.read_ttl_configs(&bound).await?;
    for (((name, (table, publication)), capture), ttl) in
        names.into_iter().zip(resolved).zip(capture).zip(ttl)
    {
        let capture = capture.filter(|state| state.state().captures_origin());
        tables.push(OperationTable {
            name,
            binding: WriteTableBinding { table, ttl },
            capture,
        });
        publications.extend(publication);
    }
    Ok((tables, publications))
}

fn lookup(tables: &[OperationTable], name: &TableName) -> StorageResult<WriteTableBinding> {
    let table = tables
        .iter()
        .find(|table| &table.name == name)
        .ok_or_else(corrupt_record)?;
    Ok(WriteTableBinding {
        table: Arc::clone(&table.binding.table),
        ttl: table.binding.ttl.clone(),
    })
}

fn selected_mutation<'accepted>(
    tables: &'accepted [OperationTable],
    input: (usize, AcceptedMutationRef<'accepted>, MutationCause),
) -> StorageResult<Option<OriginMutation<'accepted>>> {
    let (ordinal, mutation, cause) = input;
    let table = tables
        .iter()
        .find(|table| table.binding.table.history_id == mutation.table().history_id)
        .ok_or_else(corrupt_record)?;
    let ordinal = u32::try_from(ordinal).map_err(|_| corrupt_record())?.into();
    Ok(table.capture.as_ref().map(|table| OriginMutation {
        table,
        mutation,
        cause,
        ordinal,
    }))
}

fn take_provenance(
    captured: &mut std::collections::VecDeque<CapturedOriginMutation>,
    ordinal: usize,
) -> StorageResult<ItemProvenance> {
    let ordinal = u32::try_from(ordinal).map_err(|_| corrupt_record())?;
    match captured.front() {
        Some(mutation) if u32::from(mutation.ordinal) == ordinal => {
            Ok(captured.pop_front().ok_or_else(corrupt_record)?.provenance)
        }
        Some(mutation) if u32::from(mutation.ordinal) < ordinal => Err(corrupt_record()),
        _ => Ok(ItemProvenance::default()),
    }
}

pub(crate) async fn execute_batch_write<P: DynamoPort>(
    port: &P,
    execution: OriginExecution,
    shape: DynamoBatchWriteShape,
) -> Result<BatchWriteItemResponse, OriginFailure>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    match execute_operation(port, execution, BatchOperation(shape.clone())).await {
        Err(failure) => match shape.unprocessed_after_known_abort(&failure.error) {
            Some(response) => Ok(response),
            None => Err(failure),
        },
        result => result,
    }
}

#[derive(Clone)]
struct BatchOperation(DynamoBatchWriteShape);

impl OriginOperation for BatchOperation {
    type Response = BatchWriteItemResponse;
    const KIND: OriginOperationKind = OriginOperationKind::Batch;

    async fn execute<A: DynamoAttempt + PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        context: &OriginAttemptContext,
    ) -> StorageResult<OriginAttemptOutput<Self::Response>> {
        let (tables, publications) = load_tables(attempt, context, self.0.table_names())
            .await
            .map_err(storage_provider::batch_write_table_lookup_error)?;
        let bound = self.0.bind(Ok(|name: &TableName| lookup(&tables, name)))?;
        attempt.prepare_put_items(&bound.put_items())?;
        let mut accepted = bound.prepare(attempt).await?;
        let mutations = accepted
            .members()
            .map(|(ordinal, member)| {
                let cause = match member {
                    AcceptedBatchWriteMember::Put(_) => MutationCause::Put,
                    AcceptedBatchWriteMember::Delete(_) => MutationCause::Delete,
                };
                selected_mutation(&tables, (ordinal, member.mutation(), cause))
            })
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let admitted = select_batch_mutations(&context.journal, &mutations, &context.codec)?;
        let mut selected = vec![true; accepted.members().count()];
        for mutation in &mutations {
            selected
                [usize::try_from(u32::from(mutation.ordinal)).map_err(|_| corrupt_record())?] =
                admitted.contains(&mutation.ordinal);
        }
        drop(mutations);
        accepted.select(&self.0, &selected)?;
        let mutations = accepted
            .members()
            .map(|(ordinal, member)| {
                let cause = match member {
                    AcceptedBatchWriteMember::Put(_) => MutationCause::Put,
                    AcceptedBatchWriteMember::Delete(_) => MutationCause::Delete,
                };
                selected_mutation(&tables, (ordinal, member.mutation(), cause))
            })
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        let captured = capture_origin_mutations(
            &context.journal,
            attempt,
            OriginCaptureInput {
                mutations,
                wall_ms: context.wall_ms,
                transaction: None,
                observations: Vec::new(),
            },
            &context.codec,
        )
        .await?;
        let events = captured
            .iter()
            .map(|mutation| mutation.event.clone())
            .collect();
        let mut captured = captured.into();
        let response = accepted
            .apply(attempt, context.created_at, |ordinal, _| {
                take_provenance(&mut captured, ordinal)
            })
            .await?;
        if !captured.is_empty() {
            return Err(corrupt_record());
        }
        Ok(OriginAttemptOutput {
            response,
            publications,
            events,
        })
    }
}

pub(crate) async fn execute_transact_write<P: DynamoPort>(
    port: &P,
    execution: OriginExecution,
    shape: DynamoTransactWriteShape,
) -> Result<TransactWriteItemsResponse, OriginFailure>
where
    for<'native> P::Attempt<'native>: PhysicalRecordAttempt,
{
    execute_operation(port, execution, TransactOperation(shape)).await
}

#[derive(Clone)]
struct TransactOperation(DynamoTransactWriteShape);

impl OriginOperation for TransactOperation {
    type Response = TransactWriteItemsResponse;
    const KIND: OriginOperationKind = OriginOperationKind::Transaction;

    async fn execute<A: DynamoAttempt + PhysicalRecordAttempt>(
        self,
        attempt: &mut A,
        context: &OriginAttemptContext,
    ) -> StorageResult<OriginAttemptOutput<Self::Response>> {
        if let Some(response) = self
            .0
            .read_replay(attempt, context.created_at)
            .await?
            .into_result()?
        {
            return Ok(OriginAttemptOutput {
                response,
                publications: Vec::new(),
                events: Vec::new(),
            });
        }
        let (tables, publications) = load_tables(attempt, context, self.0.table_names()).await?;
        let bound = self.0.bind(
            Ok(|name: &TableName| lookup(&tables, name)),
            context.created_at,
        );
        attempt.prepare_put_items(&bound.put_items())?;
        let accepted = bound
            .prepare(attempt, context.clock.as_ref())
            .await?
            .into_result()?;
        let mutations: Vec<_> = accepted
            .members()
            .iter()
            .enumerate()
            .filter_map(|(ordinal, member)| {
                let cause = match member {
                    AcceptedTransactWriteMember::Put(_) => MutationCause::Put,
                    AcceptedTransactWriteMember::Update(_) => MutationCause::Update,
                    AcceptedTransactWriteMember::Delete(_) => MutationCause::Delete,
                    AcceptedTransactWriteMember::ConditionCheck(_) => return None,
                };
                Some(
                    member
                        .mutation()
                        .ok_or_else(corrupt_record)
                        .and_then(|mutation| {
                            selected_mutation(&tables, (ordinal, mutation, cause))
                        }),
                )
            })
            .collect::<StorageResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();
        let observations = if mutations.is_empty() {
            Vec::new()
        } else {
            let mut observations =
                super::transaction_observation::observe_transaction_dependencies(
                    accepted.members(),
                    &context.codec,
                )?;
            observations.extend(
                observe_condition_dependencies(attempt, context, &tables, accepted.members())
                    .await?,
            );
            observations
        };
        let captured = capture_origin_mutations(
            &context.journal,
            attempt,
            OriginCaptureInput {
                mutations,
                wall_ms: context.wall_ms,
                transaction: Some(context.operation),
                observations,
            },
            &context.codec,
        )
        .await?;
        let events = captured
            .iter()
            .map(|mutation| mutation.event.clone())
            .collect();
        let mut captured = captured.into();
        let response = accepted
            .apply(attempt, context.created_at, |ordinal, _| {
                take_provenance(&mut captured, ordinal)
            })
            .await?;
        if !captured.is_empty() {
            return Err(StorageError::internal(
                "captured transaction member was not applied",
            ));
        }
        Ok(OriginAttemptOutput {
            response,
            publications,
            events,
        })
    }
}

async fn observe_condition_dependencies<A: DynamoAttempt + PhysicalRecordAttempt>(
    attempt: &mut A,
    context: &OriginAttemptContext,
    tables: &[OperationTable],
    members: &[AcceptedTransactWriteMember],
) -> StorageResult<Vec<replication_types::event::order::WinnerProvenance>> {
    let mut conditions = Vec::new();
    for member in members {
        let AcceptedTransactWriteMember::ConditionCheck(condition) = member else {
            continue;
        };
        let table = tables
            .iter()
            .find(|table| table.binding.table.history_id == condition.table.history_id)
            .ok_or_else(corrupt_record)?;
        if let Some(table) = &table.capture {
            conditions.push((table, condition.as_ref()));
        }
    }
    context
        .journal
        .observe_transaction_conditions(attempt, &conditions, &context.codec)
        .await
}
