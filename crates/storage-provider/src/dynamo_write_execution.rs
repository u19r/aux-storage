//! One protected native attempt for primary, index, TTL and eligible history
//! effects.

use storage_types::{
    BoundTableInfo, ItemProvenance, RegionalMark, StorageError, StorageResult, TimestampMillis,
};
use stream_provider::{DynamoMutationRoute, StreamEntryInput};

use crate::{
    AcceptedDeleteItem, AcceptedUpsert, DeleteCause, ItemWriteAttempt,
    regional_write::{self, WriteAdmission},
};

pub(crate) fn prepare_history_route(
    table: &BoundTableInfo,
    key: &storage_types::KeyAttributes,
) -> StorageResult<Option<DynamoMutationRoute>> {
    table
        .info
        .requires_stream_entry()
        .then(|| DynamoMutationRoute::try_new(&table.table_name, &table.key_schema, key))
        .transpose()
}

pub struct UpsertResponseImages {
    pub new_item: storage_types::VerifiedWireItem<std::sync::Arc<storage_types::WireItem>>,
    pub old_item: Option<storage_types::WireItem>,
}

fn missing_route() -> StorageError {
    StorageError::internal("a paired table write has no stream route")
}

pub async fn prepare_accepted_upsert<A: ItemWriteAttempt>(
    attempt: &mut A,
    mut item: AcceptedUpsert,
    created_at: TimestampMillis,
) -> StorageResult<UpsertResponseImages> {
    item.regional = match regional_write::admit(
        item.table.binding.regional.as_deref(),
        &item.table.table_name,
        DeleteCause::Customer,
    )? {
        WriteAdmission::Unpaired => None,
        WriteAdmission::Paired(local) => Some(regional_write::local_upsert(
            local,
            item.old_provenance.as_ref(),
            item.old_marker,
            created_at,
        )?),
    };
    let provenance = item
        .regional
        .map_or_else(ItemProvenance::default, |effect| {
            ItemProvenance::regional(effect.order)
        });
    let mark = item
        .regional
        .map(|effect| RegionalMark::origin(effect.order));
    if mark.is_some() && item.history_route.is_none() {
        return Err(missing_route());
    }
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        attempt.prepare_item_write(&item.table).await?;
        let mut ttl = item.ttl_effects()?;
        let writes = ttl.take_writes();
        let effects = ttl.metadata(provenance);
        attempt
            .prepare_primary_upsert(&item, &effects, writes)
            .await?;
        if item.regional.is_some_and(|effect| effect.clear_marker) {
            attempt.stage_regional_marker(&item.table, &item.key, None)?;
        }
        if let Some(route) = item.history_route.take() {
            item.capture_history(attempt, route, created_at, mark)
                .await?;
        }
        let gsi = item.prepared_gsi.finish(
            &item.gsi_lifecycle,
            &item.table.info,
            item.old_item.as_deref(),
        )?;
        attempt.prepare_gsi_writes(&item.table, gsi).await?;
        Ok(UpsertResponseImages {
            new_item: item.new_item,
            old_item: item.old_item.map(storage_types::WireItem::from),
        })
    }
    .await;
    settlement.complete(result)
}

pub async fn prepare_accepted_delete<A: ItemWriteAttempt>(
    attempt: &mut A,
    item: &mut AcceptedDeleteItem,
    created_at: TimestampMillis,
) -> StorageResult<()> {
    item.regional = match regional_write::admit(
        item.table.binding.regional.as_deref(),
        &item.table.table_name,
        item.cause,
    )? {
        WriteAdmission::Unpaired => None,
        WriteAdmission::Paired(local) => regional_write::local_delete(
            local,
            item.cause,
            item.old_provenance.as_ref(),
            item.old_marker,
            created_at,
        )?,
    };
    if item.old_item.is_none() && item.regional.is_none() {
        return Ok(());
    }
    let item = &*item;
    let mark = item
        .regional
        .map(|effect| RegionalMark::origin(effect.marker.order));
    if mark.is_some() && item.history_route.is_none() {
        return Err(missing_route());
    }
    let mut settlement =
        infrastructure_provider::physical_attempt::PhysicalRecordOperation::new(attempt);
    let attempt = settlement.as_mut();
    let result = async {
        attempt.prepare_item_write(&item.table).await?;
        if item.old_item.is_none() {
            // P2: an absent-key delete of a paired table stores its marker and
            // a hidden entry.
            let (Some(effect), Some(mark), Some(route)) =
                (item.regional, mark, item.history_route.as_ref())
            else {
                return Err(missing_route());
            };
            let entry = StreamEntryInput::hidden(&item.table, route, created_at, mark)?;
            attempt
                .prepare_logical_stream_write(&item.table, entry, route.routing_key())
                .await?;
            return attempt.stage_regional_marker(&item.table, &item.key, Some(effect.marker));
        }
        let ttl = item.ttl_writes()?;
        attempt
            .prepare_gsi_writes(&item.table, item.gsi_writes()?)
            .await?;
        if let Some(route) = &item.history_route {
            item.capture_history(attempt, route, created_at, mark)
                .await?;
        }
        attempt.stage_primary_delete(item, ttl)?;
        if let Some(effect) = item.regional {
            attempt.stage_regional_marker(&item.table, &item.key, Some(effect.marker))?;
        }
        Ok(())
    }
    .await;
    settlement.complete(result)
}

impl AcceptedUpsert {
    pub(crate) async fn capture_history(
        &self,
        attempt: &mut impl crate::DynamoStreamWriteAttempt,
        route: DynamoMutationRoute,
        created_at: TimestampMillis,
        mark: Option<RegionalMark>,
    ) -> StorageResult<()> {
        let entry = StreamEntryInput::try_new(
            &self.table,
            &route,
            Some((&self.new_item, &self.declaration)),
            self.old_item
                .as_ref()
                .map(|old| (old, self.old_indexers.as_slice())),
            created_at,
            None,
        )?
        .with_regional(mark);
        attempt
            .prepare_logical_stream_write(&self.table, entry, route.routing_key())
            .await
    }
}

impl AcceptedDeleteItem {
    pub(crate) async fn capture_history(
        &self,
        attempt: &mut impl crate::DynamoStreamWriteAttempt,
        route: &DynamoMutationRoute,
        created_at: TimestampMillis,
        mark: Option<RegionalMark>,
    ) -> StorageResult<()> {
        let entry = StreamEntryInput::try_new(
            &self.table,
            route,
            None,
            self.old_item
                .as_ref()
                .map(|old| (old, self.old_indexers.as_slice())),
            created_at,
            self.user_identity.clone(),
        )?
        .with_regional(mark);
        attempt
            .prepare_logical_stream_write(&self.table, entry, route.routing_key())
            .await
    }
}
