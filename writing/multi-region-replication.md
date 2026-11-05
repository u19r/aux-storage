# Operating multi-region replication

Last updated: 2026-11-05

Aux-storage replicates selected DynamoDB-compatible tables between regions. Each active
replica can accept item writes, and replicas exchange those writes asynchronously.
Conflicts converge to one whole-item winner. This resembles DynamoDB Global Tables with
multi-Region eventual consistency (MREC). Aux-storage has no multi-Region strong
consistency (MRSC) mode.

Replication is opt-in. Ordinary aux-fn and aux-analytics consumers keep their existing
configuration and APIs. SQS,
SNS, queues, pubsub and unrelated streams remain regional.

## What a successful write means

A successful item write means the originating region durably committed the item effects
and the original replication event together. Delivery to other regions is async.

```mermaid
flowchart LR
    A[Application writes in region A] --> B[Durable local transaction]
    B --> C[Successful application response]
    B --> D[Background delivery of original event]
    D --> E[Region B commits history and winner effects]
    E --> F[Durable delivery acknowledgment]
```

| Question                                                             | Operational answer                                                                                 |
| -------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Does a successful write mean another region already has it?          | No. Delivery is asynchronous.                                                                      |
| Does a strongly consistent read include every region's latest write? | No. It observes the local committed state; an undelivered remote write can be missing.             |
| Can two regions both pass the same condition?                        | Yes. Each condition checks its regional state.                                                     |
| Is a transaction atomic?                                             | Within its originating native database. Remote replicas can observe its items separately.          |
| Can a transaction mix ordinary and global tables?                    | Yes, within one regional database boundary. Only global-table mutations are exported.              |
| Is delivery exactly once?                                            | No. Delivery can repeat; durable identity and content checks make repeated application idempotent. |
| Does a timeout prove the write failed?                               | No. A lost response can leave a committed operation whose outcome must be reconciled.              |

There is no fixed convergence deadline. Network delay, native contention, background work,
bootstrap and outage backlog all affect visibility. An application that writes in region A
and immediately reads in region B must tolerate an older result.

Client transaction-token idempotency is regional. Moving an uncertain request to another
region does not transfer its original response or deduplication scope. Design retries with
stable application identities and retain the original region when reconciling uncertainty.

## Last-writer-wins conflict resolution

### One winner for the complete item

Last-writer-wins (LWW) chooses one complete resolved item, including deletion state and
ordered indexer declarations. It does not merge individual attributes or replay an
`UpdateExpression` at the destination. Conditions and expressions run at the origin;
replication carries their resulting state.

Suppose both replicas initially hold `{ "name": "Ada", "city": "London" }`:

| Accepted write                       | Resolved regional item                  |
| ------------------------------------ | --------------------------------------- |
| Region A changes `name`              | `{ "name": "Grace", "city": "London" }` |
| Region B concurrently changes `city` | `{ "name": "Ada", "city": "Paris" }`    |

After delivery, the higher-order write wins the whole item. The result is one of those two
images, not an automatic combination of `Grace` and `Paris`. Both accepted events remain
in retained canonical history even though only one is the current item.

Concurrent increments have the same issue. If two isolated regions each increment a
counter from 10 to 11, convergence selects 11, not 12. LWW is not a distributed counter
or a conflict-free replicated data type.

### How writes are ordered

Aux-storage assigns an internal hybrid logical clock (HLC) with two components: a physical
millisecond value and a logical counter. A write considers local wall time, its persisted
clock floor and the versions it observes. The logical component advances when physical
time cannot advance. These clocks are internal metadata; setting an item attribute such
as `updated_at` does not choose the replication winner.

Writes compare in this order, stopping at the first difference:

1. HLC physical milliseconds, then logical counter.
2. Stable origin identity, including region, database group, journal partition and generation.
3. Origin position, then the mutation's ordinal within that position.
4. Effect rank, which lets an expiration outrank the particular version it expires.

The greatest order wins. Exact clock ties have deterministic origin tie-breaks; arrival
order and retransmission time do not affect the result. There is no configurable preferred
region or per-attribute conflict resolver. Pre-enable baseline items sort below accepted
mutations.

This order preserves causality that the storage operation observes, such as a prior
version of the same item. It does not infer dependencies from arbitrary client reads on
other connections. It also cannot establish real-time precedence between disconnected
writers: a fast clock can make a physically earlier write outrank a later slow-clock write.

A backward clock uses its saved floor. A valid future-clock event is retained and advances
the receiver's floor, including when that event loses; configured clock alerts are a reason
to investigate clock health, not a mechanism for dropping history. Clock overflow or a
corrupt floor can block new origination until recovery. Keep hosts' time synchronized, but
do not assume synchronization turns this feature into global strong consistency.

### Clock offset and travel time: a worked example

Clock **offset** is the difference between wall clocks at a given instant. Clock **drift**
is a change in that difference over time. Replication does not require equal clocks to
converge, but unequal clocks can change which successful write survives. Network delay
also leaves a window in which each region can accept writes without observing the other.

Assume A's wall clock is 100 ms ahead of B's, and **30 ms is one-way network travel time**.
For this example, delivery has no additional batching, scheduling or apply delay, both
clock floors initially trail wall time, and all writes replace the same item. Times in the
first column are elapsed real time; the wall-clock values are illustrative milliseconds.

| Real time | Event | Ordering and visible result |
| --- | --- | --- |
| 0 ms | A commits a write at wall time 1100. B's wall time is 1000. | A assigns HLC `(1100, 0)` and returns local success. B still has the older item. |
| 20 ms | Before importing A's write, B commits a write at wall time 1020. | B assigns `(1020, 0)` and returns local success. This physically later write has a lower order than A's. |
| 30 ms | B imports and commits A's original event. | A's `(1100, 0)` replaces B's `(1020, 0)`. Both successful writes remain in canonical history; B's edit disappears from the current item. The imported HLC is 70 ms ahead of B's wall time, now 1030. |
| 40 ms | B writes the item again, after that import, at wall time 1040. | B observes the imported version and issues `(1100, 1)`. This write outranks A's original despite B's slower wall clock. |
| 70 ms or later | A imports B's final write. | It outranks A's original. Both replicas converge to B's final item once all events arrive. |

The 100 ms offset is **not** an extra 100 ms replication wait, and the 30 ms travel time
is not subtracted from the event timestamp. A receiver can issue a causally later write
immediately after committing the relevant import; it does not wait for wall time to catch
up. This guarantee covers versions observed by the storage operation and the durable
floors of its journal partitions, not a universal clock barrier across all keys or client
connections.

Before observing A's event, a B write made less than 100 ms after A's write can still have
a lower physical timestamp in this example. At exactly equal physical timestamps, logical
counters and origin tie-breaks decide; there is no guarantee that the physically later write
wins. Normally delivery shortens this unobserved window. A backlog or partition can extend
it beyond the nominal 30 ms network flight. A continuously fast writer can repeatedly
supersede slow-region writes made without observing its latest events.

If “30 ms between regions” means round-trip time instead, symmetric one-way travel would
be about 15 ms. Actual paths can be asymmetric. Neither RTT nor geographic distance alone
establishes the one-way delay. Public write acknowledgments do not wait for a WAN round
trip; remote visibility also needs background dispatch, transfer, admission and a durable
apply. Receipt of bytes alone is not a committed import.

### What clock information crosses the network

Replication pages carry the **original event**, including its HLC physical milliseconds
and logical counter in the winner order. Retries preserve that timestamp. Import acceptance
advances the relevant persisted journal-partition floor even for losing events. Subsequent
origination takes the maximum of local wall time, applicable persisted floors and observed
version clocks, then advances the logical counter when physical time cannot advance.

Ordinary delivery pages and acknowledgments do **not** exchange a four-timestamp clock
probe, current sender/receiver wall times, a measured clock offset, or a one-way-delay
estimate. Replication does not synchronize host clocks or adjust LWW order for WAN travel.
The HLC physical component can already reflect an earlier imported future clock, so it is
not necessarily the sender's wall clock at event creation, much less at transmission.

For the first transfer above, `event physical time - receiver wall time` is 70 ms on arrival:
100 ms clock offset minus 30 ms elapsed delivery time. Extra queueing would reduce this
number further. A zero or negative difference does not prove synchronized clocks; an old
event or long delay can hide a fast source. A positive difference can also reflect a floor
inherited from a third region. Use independent host time monitoring to diagnose actual
clock offset and frequency drift.

### Clock-related failure conditions and operator actions

| Condition | Current behavior | Operator implication |
| --- | --- | --- |
| Concurrent or unobserved writes with unequal clocks | The greatest complete-item HLC order wins, even if it was written earlier in real time. | Do not use wall-clock LWW as a global business sequence. Route sensitive keys to one fenced writer or retain independent facts. |
| A valid event is far ahead of local wall time | Import retains it and advances the applicable clock floor. New writes can advance logically beyond it. | A clock correction does not retract accepted timestamps or restore overwritten values. Investigate the source and any propagated floors; reconcile business state with a new write. |
| Local wall clock moves backward | Durable floors and observed versions prevent applicable new timestamps from moving backward. | Preserve floor state through restart and recovery. Resetting clocks or deleting floors is not a repair procedure. |
| HLC is ahead by more than `timing.clock_alert_ms` | Import/origination emits a warning with `clock_skew_ms` and `clock_alert_ms`; ordering and acceptance are unchanged. | This is a strict greater-than, one-sided HLC-ahead alert, not a configured maximum permitted drift or a complete clock-health check. A 70 ms lead does not warn at a 100 ms threshold. |
| Logical counter overflows, or required floor state is unavailable/corrupt | Clock issuance or state validation fails instead of silently resetting order. | New origination can fail until authoritative state is recovered. A 100 ms offset by itself does not cause overflow. |
| Delivery slows, disconnects or exceeds its exchange deadline | Remote state remains stale; work can be retried and retained backlog grows. | Clock synchronization cannot remove lag. Sustained backlog can exhaust safe history capacity and reject new origin writes. A transport timeout does not establish non-commit. |
| Qualified time evidence for public-history trimming is missing, stale or discontinuous | Age qualification cannot authorize the trim. A future publication clock can also delay the minimum-age check. | Retention can grow even while item replication continues. Restore qualified time evidence and monitor capacity; do not bypass retention safety. |

Public-history trimming has a separate time-trust path: local Chrony monitoring, configured
trusted/authenticated sources, bounded measurement age and uncertainty, and wall/monotonic
continuity checks. Trim coordination carries proposals and grants, but qualified local time
evidence is process-local, not peer authority to assert clock accuracy. This mechanism does
not correct LWW timestamps or supply a peer-to-peer clock-offset estimate.

Monitor host time independently in **every** region, as well as HLC-ahead warnings and
replication backlog. Test clock steps together with delayed delivery before relying on
failover behavior. Good synchronization reduces physically misordered conflicts; it does
not prevent concurrent lost updates or turn local conditions into global conditions.

### Choosing an application strategy

The storage engine supplies one LWW strategy. These are application designs for deciding
where conflicts are acceptable or avoiding them:

| Strategy                                    | Suitable use                                                                                     | Limitation                                                                                                                                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Accept whole-item LWW                       | Replaceable preferences, caches and other state where one concurrent value can supersede another | Successful competing edits may disappear from the current item.                                                                                                                                |
| Route each logical key to one writer region | Counters, account state and coordinated updates                                                  | Routing is an application responsibility. Failover needs fencing of the former writer and an explicit decision about undelivered writes; local conditions alone do not provide a global fence. |
| Store independent facts under distinct keys | Append-only events, immutable records, per-region contributions                                  | The application must provide stable IDs, deduplicate retries and compute the desired aggregate.                                                                                                |
| Perform explicit business reconciliation    | Conflicts whose meaning needs human or domain decisions                                          | Retained history can explain competing writes. A reconciled item is a new ordinary write; there is no built-in merge hook.                                                                     |

For global uniqueness, inventory reservation or financial invariants, determine where the
authoritative decision runs before choosing active-active writes. A regional conditional
write or transaction cannot by itself protect an invariant against another isolated region.

## Enabling tables and changing membership

Deployment preparation and table membership are separate. First run a binary with the
regional feature, enabled background jobs, qualified native storage and private peer
endpoints. Select each participating connection's startup JSON through
`features.regional_connections`. Ordinary connections need no regional startup file.
Remote-provider connections cannot be selected as regional origins.

After both regions are configured, submit an `UpdateTable` request to the source table's
region to enable replication into a configured peer. For example:

```json
{
  "TableName": "Orders",
  "ReplicaUpdates": [{ "Create": { "RegionName": "region-2" } }]
}
```

An existing populated table is supported. The service establishes capture before scanning,
copies a baseline into a hidden target, catches up retained events, verifies coverage and
required indexes, then activates the replica. Existing active replicas keep serving. The
joining target accepts no public item traffic before activation, and a conflicting existing
target table is not overwritten. Poll `DescribeTable` and the required index/replica states;
an accepted control request is not proof that its background operation has completed.

Each global table has a **configuration-primary region**. This owner serializes membership,
schema, TTL, stream and other table controls. It is not an item-write leader: active replicas
can still accept item writes locally. Only one control is active per table; a conflicting
control can be rejected as busy.

Transfer ownership by submitting a separate `UpdateTable` control to the current primary:

```json
{
  "TableName": "Orders",
  "AuxConfigurationPrimaryRegion": "region-2"
}
```

The destination must be an active replica. Wait until `DescribeTable` reports the installed
`AuxConfigurationPrimaryRegion` before sending subsequent controls there. This field is an
Aux extension available in regional builds.

| Operation                        | What must finish                                                                                                                                                       |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Add a replica                    | Hidden baseline and history catch-up, integrity verification, required indexes and committed activation.                                                               |
| Remove a replica                 | Stop its new origination, drain already accepted work through final cuts, install removal, then delete that regional copy. Remaining regions continue item traffic.    |
| Remove the configuration primary | Transfer configuration ownership first, then gracefully remove the old primary.                                                                                        |
| Disable replication              | Gracefully remove the secondaries. The surviving primary retains its data and required history.                                                                        |
| Re-enable or re-add              | Create a fresh lineage or incarnation, bootstrap current state and explicitly adopt retained history. Intervening ordinary writes are represented by the new baseline. |

There is no automatic WAN election and no force-remove option that promises lossless
history from an unreachable origin. An unavailable configuration primary can block new
controls while permitted item traffic continues under the installed configuration. A
disconnected secondary can prevent lossless removal or disable from completing.

## Limits and capacity planning

### Public limits and additional work

| Limit                        | Current behavior                                                                                                                                                                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Ordered indexer declarations | At most 32 names within the table's `MaxIndexers` allowance, and at most 1,024 UTF-8 bytes total across the item's declared names, including absent attributes. JSON escaping/framing is excluded from that name limit. This also applies to ordinary writes. |
| Journal partitions           | A power of two from 64 through 65,536, fixed for an origin generation. Changing an existing generation's layout is rejected; online repartitioning is unsupported.                                                                                            |
| Regional startup file        | At most 1 MiB per configured connection.                                                                                                                                                                                                                      |

A legal item can require much more storage and transport than its logical size. JSON
escaping and binary encoding can expand it; an event can contain old and new images;
indexes, streams, receipts and history add further work. Measure encoded event sizes and
complete native effects, rather than sizing replication from the 400 KiB ceiling alone.

### Operator budgets

Budgets are explicit deployment inputs, not an AWS throughput mode or a promise that every
legal request fits the chosen provider:

| Budget family                | What it bounds                                                                                                                                                                        |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Frames and imports           | Encoded frame bytes/events and decompressed import bytes/events. Compression does not bypass decompressed limits.                                                                     |
| Transport and apply work     | Combined source/inbound/outbound reservations, apply window and concurrency.                                                                                                          |
| Complete native attempts     | Cumulative data and call admission across observations, writes, projections, history and finalization. These counters are not a measurement of peak heap or native transaction bytes. |
| Native records and originals | Event, source-position, declaration and receipt envelopes, alongside the backing engine's real limits.                                                                                |
| Retention and recovery       | Hot history, safe acceptance capacity, reserved recovery capacity, transfer rate and concurrent joins.                                                                                |
| Time budgets                 | Bounded responses, transport exchanges and reconciliation work. A timeout does not prove physical native settlement.                                                                  |

Startup checks budget relationships and the initialized provider before publishing regional
services. A syntactically valid public request can still fail safely when its complete work
exceeds admission or native capacity. Atomic operations are not split to make them fit.
Proven BatchWrite capacity rejection returns affected original requests as unprocessed;
an unknown commit outcome must not be reported as proven nonacceptance.

Recovery needs reserved capacity. Peer lag alone does not reject a write, but an outage
grows retained history until local safe capacity can reject new origin acceptance. A
required archive that is unavailable cannot be replaced by silently deleting history.
Opaque SDK memory, an exact application heap maximum and physical settlement duration
are not guaranteed. Native permits remain held until actual settlement, even if the caller
times out, so unsettled operations can temporarily reduce available capacity.

Size each full replica for its own writes **and the accepted global traffic it imports**.
Adding API processes does not multiply a single SQLite writer's capacity. Postgres and
FoundationDB can serve multiple API nodes over one regional database; SQLite and RocksDB
retain their native single-owner deployment constraints. Backend availability and backups
remain operator responsibilities.

For planning, measure the actual rates and use:

```text
backlog growth ≈ retained incoming bytes/second × outage seconds
catch-up time  ≈ backlog bytes / (recovery apply rate - ongoing incoming rate)
```

The second estimate is useful only when recovery applies faster than ongoing arrivals.
Include losing writes, repeated delivery, old/new images, archive I/O, bootstrap and index
work in measurements. There is no universal throughput or one-second convergence guarantee.

## History, deletion, TTL and indexes

**Canonical history** contains retained original accepted events, including losers and
same-image writes. After complete delivery, replicas converge on the same retained event
identities/content and current winners. Physical placement, delivery order and local
sequence numbers can differ. A duplicated identity with different original bytes is
corruption, not a new competing value.

**Standard streams** describe regional materialization changes. Importing a loser does
not change the current item, and stream records need not be identical across regions.
Global enable ensures streams are enabled, preserving an existing view type or choosing
`NEW_AND_OLD_IMAGES` when none exists. Streams cannot be disabled while replicas remain.
The internal journal retains complete mutation images independently of the public view.

Public stream retention is separate from replication custody. Trimming a public view does
not authorize deleting the last copy still needed by delivery, bootstrap or retained
history. Canonical views coordinate a shared trim frontier; an offline participant can
delay its advancement. A trimmed cursor is not permission to skip a replication gap.
Do not assume indefinite public history availability.

Archives use an operator-owned durable filesystem location. Segments are verified before
hot custody is released; archive failure retains the hot hold. Protect and back up archive
data as part of the deployment, and test retrieval before depending on it as a required copy.
An archive of events is not a complete database and control-state backup.

Deletes participate in LWW and retain deletion knowledge so delayed older writes cannot
resurrect an item. TTL workers identify the exact expired winning version. Independent
expiration of the same victim produces the same canonical event, and a stale expiration
cannot delete a newer refreshed winner. TTL cleanup is asynchronous; it is not a precise
wall-clock deletion deadline.

Global GSI definitions propagate through controls, with backfills in each required region.
The shared index stays `CREATING` until required replicas report readiness. Item traffic
continues during additive index backfill, but the creating index is unavailable for queries.
Joining replicas build required projections before activation. Immediate local GSI
consistency, when selected, does not make undelivered remote writes visible.

## Partitions, failover and recovery

| Situation                                                | How to reason about it                                                                                                                                                      |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Peer or WAN unavailable                                  | Active replicas can serve local reads and accept writes within safe capacity. Staleness and backlog increase; controls may wait for required participants.                  |
| Lost delivery acknowledgment                             | The sender can retransmit. The receiver reconciles the same event/page identity rather than originating another write.                                                      |
| Application fails over to another region                 | Routing is your responsibility. The destination may be missing accepted source writes; decide what stale reads and conflicting new writes mean for the application.         |
| Origin process restarts with intact storage              | Durable jobs, receipts, clock floors and control state resume. Preserve the configured identity and journal layout.                                                         |
| Old database backup is restored                          | Isolate it. Recover current authority or bootstrap a fresh generation before allowing origination; do not treat rollback as an ordinary same-generation restart.            |
| Gap or corrupt event is detected                         | Preserve good copies and recover exact history from a verified peer/archive. Do not skip the gap or substitute a latest-item snapshot for required originals.               |
| Configuration primary is permanently lost                | Restore authoritative control state before metadata operations resume. Item availability does not imply control ownership has transferred.                                  |
| All durable copies of an undelivered write are destroyed | Asynchronous replication cannot recover bytes that no surviving copy holds. A successful local acknowledgment is not zero-RPO protection against total origin-storage loss. |

Graceful shutdown stops mutation intake and job dispatch, stops new receiver intake, then
drains admitted work. Killing a process or cancelling a request does not establish that an
uncertain native transaction aborted.

## Security and operational checks

Peer transport is plain TCP on configured private endpoints. Deployment and origin identity
checks prevent misbinding; they do not authenticate a peer. This release supplies no peer
authentication or TLS. Restrict reachability to trusted participants and provide the network
isolation and transport protection required by your deployment. Never expose replication
listeners as public client endpoints.

Before routing traffic to a replica, verify its activation and required index readiness.
During operation, watch delivery progress and its oldest outstanding work, backlog growth,
hot/archive usage, safe capacity and recovery reserve, native latency and uncertain outcomes,
clock alerts, archive retrieval, and stuck bootstrap/control phases. Treat these as operational
signals; this guide does not promise a particular metric name or dashboard. Keep keys, item
images and credentials out of logs and metric labels.

Qualify failure recovery with your provider and representative item/index/transaction sizes.
The repository's process tests exercise Toxiproxy at 20, 75 and 150 ms in each direction,
partitions, lost replies and restarts. Those are test conditions, not production latency
limits or recovery-time guarantees.

## Differences from DynamoDB Global Tables

The closest comparison is **DynamoDB Global Tables version 2019.11.21 with MREC**.
AWS also offers MRSC, which synchronously replicates before acknowledgment and supports
globally strong reads. Aux-storage supplies the asynchronous MREC-style behavior only.
[AWS consistency modes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html)

| Area                    | Aux-storage                                                                           | DynamoDB Global Tables                                                            |
| ----------------------- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Consistency             | Asynchronous regional writes/reads; no MRSC option                                    | MREC or MRSC; MREC is the default.                                                |
| LWW order               | Explicit HLC and stable origin tie-breaks; whole-item winner                          | MREC selects by the latest internal item timestamp.                               |
| Conditions              | Evaluate regional state                                                               | MREC evaluates regional state; MRSC evaluates the latest global version.          |
| Transactions            | Native regional atomicity; remote items may appear separately                         | MREC transactions are regional; MRSC does not support transactions.               |
| TTL                     | Regional workers, replicated deterministic victim expiration                          | MREC supports TTL; MRSC does not.                                                 |
| Streams/history         | Separate original-event history and regional materialization streams                  | MREC uses Streams and can combine replicated changes; replica streams can differ. |
| Configuration ownership | Explicit per-table primary, graceful transfer and durable recovery                    | AWS manages the service control plane.                                            |
| Storage and operations  | Operator-managed native databases, private TCP, durable archive and recovery capacity | AWS-managed DynamoDB replicas and replication.                                    |
| RPO                     | Undelivered writes require a surviving durable copy                                   | MRSC offers zero RPO; MREC replication is asynchronous.                           |

The consistency, LWW, transaction, TTL, stream and RPO comparison follows
[AWS's current behavior guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/V2globaltables_HowItWorks.html).
The managed-service distinction follows
[AWS's Global Tables overview](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GlobalTables.html).
Aux's original-event history and ordering rules are its own contracts, not claims about
undocumented AWS internals. DynamoDB request compatibility does not reproduce AWS billing,
capacity management, IAM, KMS, CloudWatch or availability guarantees; see the
[compatibility matrix](dynamodb_parity.md).

## Further reading

- [API extensions](extensions.md): indexers, immediate GSI consistency and history/read extensions.
- [Roadmap](roadmap.md): feature status.

### Implementation references for clock behavior

- [HLC issuance](../crates/replication-provider/src/ordering/clock.rs) and
  [winner comparison](../crates/replication-types/src/event/order.rs).
- [Origin observations](../crates/regional-replication/src/journal/origin/capture.rs),
  [partition allocation](../crates/regional-replication/src/journal/origin/allocation.rs), and
  [persisted import floors](../crates/regional-replication/src/journal/partition.rs).
- [Clock warning policy](../crates/regional-replication/src/journal/clock.rs) and
  [import warnings](../crates/regional-replication/src/journal/acceptance.rs).
- [Delivery page and acknowledgment fields](../crates/regional-replication/src/transport/message.rs).
- [Qualified time acquisition](../crates/regional-replication/src/trim/time/source.rs) and
  [retention-age checks](../crates/regional-replication/src/trim/time/age.rs).
- Existing [regional ordering contract tests](../crates/replication-contract-tests/tests/regional.rs)
  cover imported future clocks followed by backward local time, reordered duplicates, and
  convergence across delivery permutations.
