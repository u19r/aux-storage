# DynamoDB API benchmarks

This campaign compares Aux/PostgreSQL, Aux/FoundationDB, ExtendDB/PostgreSQL and
Dynoxide/SQLite over a 20 GB base table with three ALL GSIs. Request latency and
throughput come from a separate runner in the database's availability zone.
Real Amazon DynamoDB is excluded by the revised campaign scope.

## Results

Rates are API requests per second; latencies are milliseconds. Both pooled read
and pooled write p90 must be ≤ 50 ms. The first failing column gives offered req/s
followed by read/write p90. A first-window failure establishes no passing rate
below the configured start.

| Backend and evidence | Read/write split | Last passing offered / achieved req/s | Passing read / write p90 ms | First failing: offered req/s; read / write p90 ms |
| --- | --- | --- | --- | --- |
| [Aux/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/postgres-r90-verification.json) | 90/10 | No passing window | — | 400; 4.62 / 57.18 |
| [Aux/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/postgres-r50-verification.json) | 50/50 | No passing window | — | 400; 4.66 / 59.07 |
| [Aux/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/postgres-r10-verification.json) | 10/90 | No passing window | — | 400; 6.67 / 69.42 |
| [Aux/FoundationDB](../run-artifacts/benchmark-comparison/layered-20261103/foundationdb-r90-verification.json) | 90/10 | 2,272 / 2,262.41 | 27.90 / 45.73 | 2,288; 29.72 / 51.30 |
| [Aux/FoundationDB](../run-artifacts/benchmark-comparison/layered-20261103/foundationdb-r50-verification.json) | 50/50 | 560 / 555.81 | 26.06 / 48.43 | 576; 31.01 / 50.55 |
| [Aux/FoundationDB](../run-artifacts/benchmark-comparison/layered-20261103/foundationdb-r10-verification.json) | 10/90 | 400 / 399.24 | 5.63 / 11.64 | 416; 27.96 / 53.21 |
| [ExtendDB/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/extenddb-r90-low-verification.json) | 90/10 | 114 / 113.06 | 3.79 / 24.65 | 130; 9.07 / 73.23 |
| [ExtendDB/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/extenddb-r50-verification.json) | 50/50 | No passing window | — | 50; 9.47 / 69.92 |
| [ExtendDB/PostgreSQL](../run-artifacts/benchmark-comparison/layered-20261103/extenddb-r10-verification.json) | 10/90 | No passing window | — | 50; 9.67 / 64.09 |
| Dynoxide/SQLite | 90/10 | Pending | Pending | Pending |
| Dynoxide/SQLite | 50/50 | Pending | Pending | Pending |
| Dynoxide/SQLite | 10/90 | Pending | Pending | Pending |

These are individual five-second ramp observations, not repeated steady-state
capacity estimates. No confidence interval or repeatability claim is made.
JSON reports retain every window, operation's p50/p90/p99, work counts and errors.

Aux and Dynoxide start at 400 req/s. After ExtendDB failed there, the user requested
a lower start: its reported rerun and remaining splits start at 50 req/s. Every
ramp adds 16 req/s every five seconds and stops when either p90 exceeds 50 ms.
The different start changes warm-up history and is part of the comparison's scope.

## Dynoxide metadata readiness problem

At **19:18 UTC**, Dynoxide had spent **more than 17 minutes** on the benchmark's
single DescribeTable readiness request, without returning a response. The measured
read/write workload had not started. The original 30-second timeout had already
failed four attempts; a fresh-runner retry permits up to 3,600 seconds.

DescribeTable computes live table size/count figures. For this fixture it scans
the 25-million-row main table and joins each of the three 25-million-entry ALL GSIs
back to the main table to obtain item sizes. This control-plane work therefore
scales with the dataset rather than simply returning cached table metadata.

A one-second sample at 19:18:40 UTC showed about **98 MiB/s of disk reads**, no writes,
and **9% of one CPU core**. The service cgroup held **18.8 GiB including charged file
cache**, below its 24 GiB limit, with no limit hits or OOM events since restart.
Those samples show ongoing read activity; they do not prove the exact native call
stack or a unique underlying bottleneck.

This delay is reported separately from request throughput and p90: excluding setup
from the workload timer does not make the metadata failure disappear.
[Observed request and resource evidence](../run-artifacts/benchmark-comparison/layered-20261103/dynoxide-metadata-readiness-observation.json).

## Dataset and operation mix

The main table contains 25,000,000 items of exactly 800 logical bytes: 20 GB before
indexes and engine overhead. Every item populates all three GSIs with full projection,
exposing about 80 GB of logical table/index data. Each relation has 10,000 logical
partition keys and 2,500 items per partition.

Reads and overwrites sample the full existing keyspace without a recent-write preference.
Payloads vary by item and revision to avoid compressible repeated padding. Writes change
payloads while preserving keys, item size and GSI membership. The operation counter
continues across windows. Seeds are 1, 1000000007 and 2000000011 for 90/10, 50/50 and
10/90, respectively, and match across backends.

Sampling permits every partition to be selected; a short first-window run does not
prove that every partition was actually touched. Dataset census and observed device
I/O are separate evidence from the access-distribution contract.

| Operation | Share within reads or writes | Items per request |
| --- | --- | --- |
| GetItem | One third of reads | 1 |
| BatchGetItem | One third of reads | 10, spread over the keyspace |
| Query | One third of reads | 20–100 full items |
| PutItem | One quarter of writes | 1 |
| UpdateItem | One quarter of writes | 1; replace payload |
| TransactWriteItems | One quarter of writes | 10, spread over the keyspace |
| BatchWriteItem | One quarter of writes | 25, spread over the keyspace |

Queries sample the main table and each GSI equally and select populated sort-key ranges.
The runner rejects incomplete items, unexpected cardinalities and unprocessed members.
GetItem, BatchGetItem and main-table queries request strong consistency; GSI queries
request eventual consistency.

Every PutItem has a condition and one quarter intentionally fail. Thus 25% of write
requests are conditional and 6.25% of all writes intentionally fail. Short windows
retain actual counts and rounding. Read/write percentages describe requests, not
item members, bytes, RCUs or WCUs.

## Infrastructure and durability

All hosts are fresh m8gd.4xlarge Spot instances in eu-north-1, using ARM64 Ubuntu AMI
ami-0335270b14f48b820 and the regional default VPC. Each backend has its own database
host and a separate runner in the same AZ: PostgreSQL/Dynoxide use eu-north-1a;
FoundationDB/ExtendDB use eu-north-1c.

Backend VMs expose six CPUs and 32 GiB RAM. Persistent systemd limits enforce:

| Backend | Native database | Application |
| --- | --- | --- |
| Aux/PostgreSQL | CPUs 0–3, 16 GiB | storage-api: CPUs 4–5, 8 GiB |
| Aux/FoundationDB | CPUs 0–3, 16 GiB | storage-api: CPUs 4–5, 8 GiB |
| ExtendDB/PostgreSQL | CPUs 0–3, 16 GiB | ExtendDB: CPUs 4–5, 8 GiB |
| Dynoxide/SQLite | Combined CPUs 0–5, 24 GiB | SQLite is embedded in the same process |

Runners expose the physical instance's 16 CPUs and 64 GiB, outside those budgets.
A fresh runner is used for tiny practice, preload, each measured split and each retry.
Data, WAL and journals use a 600 GB decimal local NVMe ext4 filesystem with noatime.
Memory ceilings include charged file cache and do not reserve RAM.

PostgreSQL 18.6 uses 4 GiB shared buffers with fsync, full_page_writes and
synchronous_commit enabled. FoundationDB 7.4.8 uses four processes, one per native
core, single replication and ssd-redwood-1. Each process has a 4 GiB ceiling and
1 GiB cache; the service has a 16 GiB aggregate limit.

ExtendDB v0.1.14 uses index_propagation_delay_ms=0 for synchronous GSI maintenance.
Its HTTPS/SigV4 costs are included. Dynoxide v1.5.0 uses its ordinary SQLite WAL API
path; bulk import, which disables fsync, is not used. Releases were checked before launch.

Aux enables immediate GSI consistency, background maintenance and default 72-hour
internal history. Adaptive admission and public DynamoDB Streams are disabled on
fixture tables. Internal history remains a cost of the frozen Aux build.
ExtendDB skips stream-record capture when StreamSpecification is absent, so these
ExtendDB runs do not pay that history-write cost. This functional difference is
part of the comparison. [Stream configuration evidence](../run-artifacts/benchmark-comparison/layered-20261103/stream-configuration-verification.json).

## Preload and dataset qualification

Preload uses durable 25-item BatchWriteItem calls over 50 disjoint ranges, with one
active request per range. No bulk-import durability weakening is used.

| Backend | Minutes | Concurrent requests | Complete batches | Recovered rejections | Reported storage after load |
| --- | --- | --- | --- | --- | --- |
| Aux/PostgreSQL | 37.43 | 16 | 1,000,000 | 56 HTTP 503 | 276.20 GB database |
| Aux/FoundationDB | 59.80 | 50 | 1,000,000 | 0 | 205.34 GB KV; 221.78 GB disk |
| ExtendDB/PostgreSQL | 62.67 | 16 | 1,000,000 | 0 | 150.94 GB data database |
| Dynoxide/SQLite | 66.35 | 16 | 1,000,000 | 0 | 114.97 GB database file |

All loads acknowledged 25 million items without partial responses or transport retries.
Both Aux loads met the under-two-hour target. PostgreSQL's known-rejected 503 requests
were retried successfully.

PostgreSQL's exact native census verifies 25 million rows, 10,000 partitions,
2,500 rows per partition and 800 evaluated bytes in the main table and all GSIs.
ExtendDB and Dynoxide have the same exact row/partition census and 128 sampled
800-byte items per relation. Dynoxide's census runs inside its existing 24 GiB
service cgroup, avoiding an unbudgeted reader warming the data.

FoundationDB combines complete acknowledged fixture writes, provider correctness
proof, API checks and a healthy four-process single/Redwood cluster with no queued
or in-flight data movement. It does not claim a native all-key census.

Storage figures have different accounting boundaries: PostgreSQL database sizes
exclude WAL, while FoundationDB reports KV and engine disk bytes. Logical size alone
does not prove a cache-miss ratio; saved I/O samples establish actual disk activity.

## Measurement and evidence

After load and final preparation, the database rests at least 120 seconds.
Each split uses another fresh runner and a recorded two-minute rest. Latency runs
from the scheduled request instant through preparation, transport, decoding and
validation. Read/write p90 pools individual observations, rather than averaging
per-operation percentiles. An unexpected-error rate above 2% also stops the ramp.

The initial ExtendDB 400 req/s attempt remains separately preserved; the table uses
its lower-start rerun. Dynoxide's first attempt failed before any measured window:
DescribeTable exceeded 30 seconds while computing live table sizes and three full
GSI/base-table joins. Its retry uses a 3,600-second client timeout, including measured
requests; the 50 ms p90 cutoff is unchanged. This timeout difference and the metadata
scan's cache effects are part of the reported configuration.

Runner/backend samples include thread CPU, memory, pressure, NVMe I/O, ENA allowance
and network counters. Native FoundationDB status and PostgreSQL database/WAL/wait
counters accompany the relevant runs. Raw reports, samples and configuration
receipts are downloaded and hash-verified before runner replacement.

[Campaign inputs](../run-artifacts/benchmark-comparison/layered-20261103/campaign-contract.json),
[release versions](../run-artifacts/benchmark-comparison/layered-20261103/release-inputs.json) and per-result links identify the evidence.

## Source attribution and limitations

Aux API SHA256:
0b35f54e982f48152d49111a9be870b0f8787554f979b2eafcffbb393ff48b18.

Runner SHA256:
8536fb9c12222356715ff1078a239494cca88ddf2f1e593897cafd6508a8024c.

Frozen source archive SHA256:
d2f89881e9c93f70f89653f7076b725e407e11f0f301bcc2d1a1274fdb2d2973.

The 19:03 UTC [source recheck](../run-artifacts/benchmark-comparison/layered-20261103/current-source-recheck.json) found 82 archived crate files
changed concurrently after the build, including stream/GSI paths and request metrics.
These results and archived provider acceptance describe the frozen build; they do
not measure or validate those newer changes. Concurrent work is preserved.

The write simplification materializes current items, GSI projections and item-history
snapshots, uses thin table/system pointers and removes per-item retention and the
ownership graph. Native ordered GSI rows replace the manual trie.
[Source acceptance](../run-artifacts/benchmark-comparison/resumed-20261103/ordered-gsi-final-acceptance.json)
and [equal-work measurements](../run-artifacts/write-simplify/equal-work-local-results.md)
retain exact proof scope.

## Historical diagnostics

Earlier two-CPU/eight-GiB whole-host Aux loads took 77.45 minutes for PostgreSQL and
105.02 minutes for FoundationDB. They do not populate the current comparison.

The five-process FoundationDB diagnostic used five native cores, a 26 GiB native
ceiling, 8 GiB aggregate cache and four Aux cores. It passed 3,248 offered req/s
(3,218.72 achieved), read/write p90 28.72/48.72 ms, and failed at 3,264 on write p90
54.77 ms. Its 1,648,800 requests had no unexpected errors, retries or unknown work.
[Diagnostic evidence](../run-artifacts/benchmark-comparison/resumed-20261103/fiveprocess-results.md)
records why this is not a controlled one-variable comparison with the current campaign.
