pub(super) const fn expiration_progress_revision() -> &'static str {
    "SELECT revision FROM sys_maintenance_task WHERE kind = ?1"
}
