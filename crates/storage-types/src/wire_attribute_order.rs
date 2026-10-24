/// Single-table wire lookup priority, shared by derive expansion and stored
/// codecs. Indexers retain declaration order; ordinary attributes use their
/// wire names as a deterministic tie-breaker at serialization boundaries.
#[doc(hidden)]
#[must_use]
pub fn wire_attribute_priority(name: &str, indexer: Option<usize>) -> (u8, usize) {
    match name {
        "pk" => (0, 0),
        "sk" => (0, 1),
        "gsi1pk" | "gsi_pk" => (1, 0),
        "gsi1sk" | "gsi_sk" => (1, 1),
        "gsi2pk" => (1, 2),
        "gsi2sk" => (1, 3),
        "gsi3pk" => (1, 4),
        "gsi3sk" => (1, 5),
        "gsi4pk" => (1, 6),
        "gsi4sk" => (1, 7),
        "gsi5pk" => (1, 8),
        "gsi5sk" => (1, 9),
        "ttl" | "e_at" | "expires_at" => (2, 0),
        _ => indexer.map_or((4, 0), |ordinal| (3, ordinal)),
    }
}
