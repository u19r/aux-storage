pub fn profile_integer_overrides(profile: &str, workload: &str) -> Vec<(&'static str, i64)> {
    match (profile, workload) {
        ("local-soak", "table_atomicity") => vec![
            ("operationCount", 96),
            ("activeClientCount", 2),
            ("keyCount", 12),
            ("sharedKeyCount", 6),
            ("sharedOperationPercent", 35),
            ("historySampleLimit", 256),
        ],
        ("nightly-soak", "table_atomicity") => vec![
            ("operationCount", 256),
            ("activeClientCount", 3),
            ("keyCount", 24),
            ("sharedKeyCount", 12),
            ("sharedOperationPercent", 40),
            ("historySampleLimit", 512),
        ],
        ("local-soak", "queue_visibility") => vec![("operationCount", 3)],
        ("nightly-soak", "queue_visibility") => vec![("operationCount", 10)],
        ("local-soak", "partition_family") => vec![("operationCount", 0)],
        ("nightly-soak", "partition_family") => vec![("operationCount", 8)],
        _ => Vec::new(),
    }
}
