use storage_types::{StorageEnum, StorageError, StorageResult};

use crate::namespace_routing::{NamespaceRoute, RouteTarget};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RoutedWriteTargetRole {
    Primary,
    Migration,
}

impl RoutedWriteTargetRole {
    pub(crate) const fn for_index(index: usize) -> Self {
        if index == 0 {
            Self::Primary
        } else {
            Self::Migration
        }
    }

    pub(crate) const fn metric_label(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Migration => "migration",
        }
    }
}

pub(crate) fn ensure_route_writes_not_paused(route: &NamespaceRoute) -> StorageResult<()> {
    if !route.writes_paused {
        return Ok(());
    }
    Err(StorageError::Base(StorageEnum::Throttled {
        message: format!(
            "namespace {} writes are temporarily paused for storage cutover",
            route.namespace
        ),
    }))
}

pub(crate) struct WriteTargetSet<T> {
    payloads: Vec<Option<T>>,
    payload_name: String,
}

impl<T: Clone> WriteTargetSet<T> {
    pub(crate) fn new(target_count: usize, payload: T, payload_name: &str) -> StorageResult<Self> {
        if target_count == 0 {
            return Err(StorageError::internal(&format!(
                "{payload_name} routing produced no write targets"
            )));
        }

        let mut payloads = Vec::with_capacity(target_count);
        for _ in 0..target_count.saturating_sub(1) {
            payloads.push(Some(payload.clone()));
        }
        payloads.push(Some(payload));

        Ok(Self {
            payloads,
            payload_name: payload_name.to_string(),
        })
    }

    pub(crate) fn take(&mut self, index: usize) -> StorageResult<T> {
        self.payloads
            .get_mut(index)
            .ok_or_else(|| {
                StorageError::internal(&format!(
                    "routed write state '{}' missing target index {}",
                    self.payload_name, index
                ))
            })?
            .take()
            .ok_or_else(|| {
                StorageError::internal(&format!(
                    "routed write state '{}' was already consumed for target {}",
                    self.payload_name, index
                ))
            })
    }
}

pub(crate) fn fan_out_route_write_payload<T: Clone, F>(
    route: &NamespaceRoute,
    payload: T,
    payload_name: &str,
    mut write_target_fn: F,
) -> StorageResult<()>
where
    F: FnMut(&RouteTarget, RoutedWriteTargetRole, T) -> StorageResult<()>,
{
    let mut payloads = WriteTargetSet::new(route.write_targets.len(), payload, payload_name)?;
    for (index, target) in route.write_targets.iter().enumerate() {
        let payload_for_target = payloads.take(index)?;
        write_target_fn(
            target,
            RoutedWriteTargetRole::for_index(index),
            payload_for_target,
        )?;
    }
    Ok(())
}
