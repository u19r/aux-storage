use super::FoundationDbSettings;

#[test]
fn foundationdb_settings_disable_grv_cache_by_default() {
    assert_eq!(FoundationDbSettings::default().cache_read_version_ms, 0);
}
