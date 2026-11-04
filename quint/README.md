# Production model-based tests

These bounded tests compare generated model traces with maintained Rust boundaries. They
provide regression evidence for the sampled inputs and trace lengths. They do not establish
exhaustive correctness or qualify a storage provider.

| Boundary | Rust adapter | Checked behavior |
| --- | --- | --- |
| Adaptive admission | `crates/storage/src/admission/quint_admission_tests.rs` | Real adaptive controller transitions across bounded state and signal combinations. |
| GSI maintenance | `crates/storage-provider/src/maintenance/gsi/observation_model_tests.rs` | Shared observation evaluation, build transitions, frontier decisions, and effects. |
| Partition reconciliation | `crates/storage-provider/src/quint_partition_tests.rs` | Shared reconciliation, returned topology and controller state, with required split and admission-gate coverage. |

Run `just quint` for these adapters, or use the normal workspace test suite. Each adapter
invokes its model through `quint-connect`; a passing standalone model is not release evidence.
Keep native acquisition, transaction atomicity, persistence, record ordering, and HTTP behavior
in real-provider contract and conformance tests. In particular, the retention adapter does not
prove failed-write atomicity, and the partition adapter does not prove native split publication.

The partition model uses exact rational controller state. Its bounded adapter traces fit the
simulator's integer range; extending their length requires checking that representation first.

## Design specifications

`just quint-design` checks five abstract protocol models independently of production tests:

| Model | Purpose |
| --- | --- |
| `table_deletion.qnt` | Detach a name for immediate fresh-ID reuse while old IDs remain DELETING, fence old producers/cursors, and pair expiry markers with their states. Focused gate: `just quint-table-deletion`. Contract: [write-simplify.md](../docs/plans/write-simplify.md#table-deletion-and-recreation-security-contract). |
| `write_simplify.qnt` | Explore snapshot history, exact writer predecessors, one custom table duration, unpublished current anchors, quiet baseline replacement/deletion with committed-retirement release, independently retiring lineages, captured-time inversion, table-generation fences, prospective retirement and bounded metadata cleanup/restart. Focused gate: `just quint-write-simplify`. Design contract: [write-simplify.md](../docs/plans/write-simplify.md). |
| `maintenance_commit.qnt` | Explore destination fencing, replacement, observation/commit races, and lost acknowledgments. |
| `gsi_history_lifetime.qnt` | Explore delayed allocation, split-lineage holds, independent shard progress, and image/record reclamation. |
| `system_stream_scope_frontier.qnt` | Explore checkpoint issuance uncertainty, exact replay, split continuation, and protected frontiers. |

These models make transaction and persistence assumptions explicit. Passing them means the
sampled abstract executions satisfy the modeled invariants. It does not show that a driver,
transaction implementation, restart path, or HTTP handler implements those assumptions.
Their scenarios guide provider tests and design review; they are not release acceptance receipts.
