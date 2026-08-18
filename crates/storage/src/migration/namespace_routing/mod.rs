mod access;
mod cutover_watcher;
mod model;
mod request_rewriter;
mod resolver;

pub(super) use access::{
    is_missing_sys_namespaces_table_error, is_retryable_cutover_watcher_error,
};
pub use access::{
    is_retryable_pause_error, is_shared_table_enabled_namespace_route,
    reject_direct_shared_table_access,
};
pub(crate) use cutover_watcher::CutoverWatcher;
pub(crate) use model::NamespaceRouteRecord;
pub use model::{
    CutoverEvent, CutoverEventStatus, NamespaceRoute, NamespaceSourceTable,
    NamespaceStorageMigrationMode, NamespaceStorageMode, RouteTarget, namespace_source_table,
};
pub(super) use model::{
    CutoverEventSerde, CutoverOverride, LocationBackendKindSerde, LocationDescriptorSerde,
    NamespaceRouteMigrationModeSerde, NamespaceRouteRecordSerde,
};
pub use request_rewriter::NamespaceRequestRewriter;
pub(crate) use resolver::{NamespaceRouteResolver, parse_namespace_route_record};
