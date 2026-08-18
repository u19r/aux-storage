use super::FoundationDbSettings;

#[test]
fn foundationdb_settings_default_to_fifty_millisecond_grv_cache_lag() {
    assert_eq!(FoundationDbSettings::default().cache_read_version_ms, 50);
}
