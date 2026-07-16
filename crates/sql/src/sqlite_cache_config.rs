#[cfg(target_os = "linux")]
use std::fs;
#[cfg(any(target_os = "linux", test))]
use std::path::Path;
#[cfg(target_os = "macos")]
use std::process::Command;
use std::sync::OnceLock;

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

const DEFAULT_SQLITE_PAGE_CACHE_KB: u64 = 128_000;
const MAX_SQLITE_PAGE_CACHE_KB: u64 = 1_048_576;
const PAGE_CACHE_MEMORY_FRACTION: u64 = 10;
const BYTES_PER_SQLITE_CACHE_KB: u64 = 1024;

pub(crate) fn sqlite_page_cache_size_kb() -> i32 {
    static PAGE_CACHE_SIZE_KB: OnceLock<i32> = OnceLock::new();
    *PAGE_CACHE_SIZE_KB.get_or_init(|| {
        let cache_kb = effective_memory_limit_bytes()
            .map_or(DEFAULT_SQLITE_PAGE_CACHE_KB, |limit| {
                sqlite_page_cache_size_kb_for_memory_limit(limit)
            });
        -(cache_kb as i32)
    })
}

pub(crate) fn sqlite_page_cache_size_kb_for_memory_limit(memory_limit_bytes: u64) -> u64 {
    let ten_percent_kb =
        memory_limit_bytes / PAGE_CACHE_MEMORY_FRACTION / BYTES_PER_SQLITE_CACHE_KB;
    ten_percent_kb.clamp(DEFAULT_SQLITE_PAGE_CACHE_KB, MAX_SQLITE_PAGE_CACHE_KB)
}

fn effective_memory_limit_bytes() -> Option<u64> {
    let system_memory = system_memory_bytes()?;
    let cgroup_memory = cgroup_memory_limit_bytes();
    Some(match cgroup_memory {
        Some(limit) => limit.min(system_memory),
        None => system_memory,
    })
}

#[cfg(target_os = "linux")]
fn system_memory_bytes() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    parse_linux_meminfo_bytes(&meminfo)
}

#[cfg(target_os = "macos")]
fn system_memory_bytes() -> Option<u64> {
    let output = Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    parse_u64_limit(std::str::from_utf8(&output.stdout).ok()?)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn system_memory_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    let mut limits = Vec::new();
    collect_cgroup_limit(&mut limits, "/sys/fs/cgroup/memory.max");
    collect_cgroup_limit(&mut limits, "/sys/fs/cgroup/memory/memory.limit_in_bytes");
    collect_process_cgroup_limits(&mut limits);

    limits.into_iter().min()
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn collect_cgroup_limit(limits: &mut Vec<u64>, path: impl AsRef<Path>) {
    if let Ok(raw_limit) = fs::read_to_string(path)
        && let Some(limit) = parse_cgroup_memory_limit(&raw_limit)
    {
        limits.push(limit);
    }
}

#[cfg(target_os = "linux")]
fn collect_process_cgroup_limits(limits: &mut Vec<u64>) {
    let Ok(cgroup) = fs::read_to_string("/proc/self/cgroup") else {
        return;
    };

    for line in cgroup.lines() {
        let Some(entry) = parse_proc_self_cgroup_line(line) else {
            continue;
        };
        if entry.controllers.is_empty() {
            collect_cgroup_limit(
                limits,
                Path::new("/sys/fs/cgroup")
                    .join(entry.relative_path)
                    .join("memory.max"),
            );
        } else if entry.controllers.contains(&"memory") {
            collect_cgroup_limit(
                limits,
                Path::new("/sys/fs/cgroup/memory")
                    .join(entry.relative_path)
                    .join("memory.limit_in_bytes"),
            );
        }
    }
}

#[cfg(any(target_os = "linux", test))]
pub(crate) struct ProcSelfCgroupEntry<'a> {
    pub(crate) controllers: Vec<&'a str>,
    pub(crate) relative_path: &'a Path,
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn parse_proc_self_cgroup_line(line: &str) -> Option<ProcSelfCgroupEntry<'_>> {
    let mut parts = line.splitn(3, ':');
    parts.next()?;
    let controllers = parts.next()?;
    let path = parts.next()?;

    Some(ProcSelfCgroupEntry {
        controllers: controllers
            .split(',')
            .filter(|controller| !controller.is_empty())
            .collect(),
        relative_path: Path::new(path.trim_start_matches('/')),
    })
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn parse_linux_meminfo_bytes(meminfo: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix("MemTotal:")?;
        let mut parts = rest.split_whitespace();
        let total_kb = parts.next()?.parse::<u64>().ok()?;
        Some(total_kb.saturating_mul(1024))
    })
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn parse_cgroup_memory_limit(raw_limit: &str) -> Option<u64> {
    let limit = raw_limit.trim();
    if limit == "max" {
        return None;
    }
    parse_u64_limit(limit).filter(|value| *value > 0)
}

fn parse_u64_limit(raw_limit: &str) -> Option<u64> {
    raw_limit.trim().parse::<u64>().ok()
}
