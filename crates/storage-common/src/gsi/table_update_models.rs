use storage_types::{
    CreateGlobalSecondaryIndex, DeleteGlobalSecondaryIndexAction, StoredTableInfo,
};

#[derive(Clone)]
pub enum GsiLifecycleAction {
    Create(CreateGlobalSecondaryIndex),
    Delete(DeleteGlobalSecondaryIndexAction),
}

pub struct GsiTableUpdatePlan {
    pub next_table: StoredTableInfo,
    pub lifecycle: Option<GsiLifecycleAction>,
    pub attribute_definitions_changed: bool,
}
