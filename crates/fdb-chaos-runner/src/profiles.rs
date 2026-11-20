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
        ("local-soak", "queue_claim") => {
            vec![
                ("operationCount", 48),
                ("nodeCount", 12),
                ("messageCount", 64),
            ]
        }
        ("nightly-soak", "queue_claim") => {
            vec![
                ("operationCount", 128),
                ("nodeCount", 12),
                ("messageCount", 128),
            ]
        }
        ("local-soak", "queue_dead_letter") => {
            vec![
                ("operationCount", 32),
                ("nodeCount", 12),
                ("messageCount", 48),
            ]
        }
        ("nightly-soak", "queue_dead_letter") => {
            vec![
                ("operationCount", 96),
                ("nodeCount", 12),
                ("messageCount", 96),
            ]
        }
        ("local-soak", "table_recreate") => {
            vec![
                ("operationCount", 64),
                ("writerCount", 8),
                ("recreateCount", 6),
            ]
        }
        ("nightly-soak", "table_recreate") => {
            vec![
                ("operationCount", 160),
                ("writerCount", 11),
                ("recreateCount", 12),
            ]
        }
        _ => Vec::new(),
    }
}
