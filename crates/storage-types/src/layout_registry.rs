/// Represents a logical entity layout inside a tenant single-table design.
#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub name: &'static str,
    pub storage_entity_type: &'static str,
    pub entity_type: &'static str,
    pub has_gsi5: bool,
    pub has_updated_at_millis: bool,
}

// We use the inventory crate so each entity can self-register its layout via
// the derive macro.
inventory::collect!(EntityLayout);

/// Iterate over all registered layouts (order not guaranteed).
#[must_use]
pub fn entity_layouts() -> Vec<&'static EntityLayout> {
    inventory::iter::<EntityLayout>.into_iter().collect()
}

/// Helper macro for manual registration (used until derive macro emits these
/// entries).
#[macro_export]
macro_rules! register_entity_layout {
    ($name:expr, $entity_type:expr, $pk:expr, $sk:expr,[$(($gpk:expr, $gsk:expr)),* $(,)?]) => {};
    ($name:expr, $entity_type:expr, $pk:expr, $sk:expr) => {
        $crate::register_entity_layout!($name, $entity_type, $pk, $sk, []);
    };
}

// Manual registrations removed: entities now auto-register via the derive
// macro.
