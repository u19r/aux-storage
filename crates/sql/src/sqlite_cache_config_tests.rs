// SQLite and Turso both use SQLite's `PRAGMA cache_size` semantics here. A
// positive value means "pages"; a negative value means "KiB". The production
// helper returns a negative number so the configured cache size is independent
// of the database page size.
//
// The policy is intentionally conservative:
//
// 1. If memory detection fails, use the default: 128,000 KiB.
// 2. If memory is known, take 10% of that limit, measured in binary KiB.
// 3. Clamp the result to 128,000 KiB..=1,048,576 KiB.
// 4. Return the negative KiB value to the backend, for example `-128000`.
//
// Example calculations:
//
// - 512 MiB host/container limit: 512 * 1024 * 1024 bytes / 10 / 1024 = 52,428
//   KiB, clamped up to 128,000 KiB. The backend receives `-128000`.
//
// - 8 GiB host/container limit: 8 * 1024 * 1024 * 1024 bytes / 10 / 1024 =
//   838,860 KiB, which is inside the clamp range. The backend receives
//   `-838860`.
//
// - Extremely large or bogus memory limit: the 10% calculation may exceed the
//   cap, so it is clamped down to 1,048,576 KiB. The backend receives
//   `-1048576`.
//
// The upper clamp also prevents returning `i32::MIN` as a negative cache size.
// That value is dangerous because downstream code that calls `abs()` on it can
// panic with integer overflow.

use crate::sqlite_cache_config::{
    parse_cgroup_memory_limit, parse_linux_meminfo_bytes, parse_proc_self_cgroup_line,
    sqlite_page_cache_size_kb, sqlite_page_cache_size_kb_for_memory_limit,
};

#[test]
fn cache_size_uses_default_for_small_memory_limits() {
    assert_eq!(
        sqlite_page_cache_size_kb_for_memory_limit(512 * 1024 * 1024),
        128_000
    );
}

#[test]
fn cache_size_uses_ten_percent_for_larger_memory_limits() {
    assert_eq!(
        sqlite_page_cache_size_kb_for_memory_limit(8 * 1024 * 1024 * 1024),
        838_860
    );
}

#[test]
fn cache_size_is_capped_below_panic_prone_i32_min() {
    assert_eq!(
        sqlite_page_cache_size_kb_for_memory_limit(u64::MAX),
        1_048_576
    );
    assert!(sqlite_page_cache_size_kb() > i32::MIN);
}

#[test]
fn linux_meminfo_parser_reads_memtotal_as_bytes() {
    assert_eq!(
        parse_linux_meminfo_bytes("MemTotal:       524288 kB\nMemFree: 1 kB\n"),
        Some(536_870_912)
    );
}

#[test]
fn cgroup_memory_limit_parser_ignores_unlimited_values() {
    assert_eq!(parse_cgroup_memory_limit("max\n"), None);
    assert_eq!(parse_cgroup_memory_limit("0\n"), None);
    assert_eq!(
        parse_cgroup_memory_limit("1073741824\n"),
        Some(1_073_741_824)
    );
}

#[test]
fn proc_self_cgroup_parser_reads_v2_relative_path() {
    let entry =
        parse_proc_self_cgroup_line("0::/system.slice/app.service").expect("parse cgroup v2 entry");

    assert!(entry.controllers.is_empty());
    assert_eq!(
        entry.relative_path,
        std::path::Path::new("system.slice/app.service")
    );
}

#[test]
fn proc_self_cgroup_parser_reads_v1_memory_controller() {
    let entry =
        parse_proc_self_cgroup_line("6:cpu,memory:/docker/abc").expect("parse cgroup v1 entry");

    assert!(entry.controllers.contains(&"memory"));
    assert_eq!(entry.relative_path, std::path::Path::new("docker/abc"));
}
