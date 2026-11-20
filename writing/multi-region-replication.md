# Operating multi-region replication

Last updated: 2026-11-12

Aux-storage pairs one DynamoDB-compatible table between exactly two regions. Both tables accept
item writes and exchange them asynchronously. Conflicts converge to one whole-item winner. This
resembles DynamoDB Global Tables with multi-Region eventual consistency (MREC). Aux-storage has no
multi-Region strong consistency (MRSC) mode.

Replication is opt-in per table. Unpaired tables behave as before. SQS, SNS, queues, pubsub and
unrelated streams remain regional.

## What a successful write means

A successful item write means the originating region durably committed the item effects and the
original replication event together. Delivery to the other region is asynchronous.

| Question                                                         | Operational answer                                                                         |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Does a successful write mean the other region already has it?    | No. Delivery is asynchronous.                                                              |
| Does a strongly consistent read include the peer's latest write? | No. It observes the local committed state; an undelivered remote write can be missing.     |
| Can two regions both pass the same condition?                    | Yes. Each condition checks its regional state.                                             |
| Is a transaction atomic?                                         | Within its originating native database. The peer can observe its items separately.         |
| Is delivery exactly once?                                        | No. Delivery can repeat; durable cursors and markers make repeated application idempotent. |
| Does a timeout prove the write failed?                           | No. A lost response can leave a committed operation whose outcome must be reconciled.      |

There is no fixed convergence deadline. Network delay, native contention, background work, the
bootstrap scan and outage backlog all affect visibility. An application that writes in one region
and immediately reads in the other must tolerate an older result.

Client transaction-token idempotency is regional. Moving an uncertain request to the other region
does not transfer its original response or deduplication scope.

## Pairing

- Configure each region with a regional file (see [Multi-region replication](../CONFIGURATION.md#multi-region-replication)
  in CONFIGURATION.md): its own name, the two regions with ranks and addresses, and the listen address.
- `UpdateTable` with `ReplicaUpdates [{Create: {RegionName: <peer>}}]` at the source region starts the
  pair. The table needs an enabled stream.
- The peer creates the table with the same schema. It is `CREATING` until the source has scanned its
  existing items into it; reads of the new table are not found until the pair is active. Items written
  during the scan are replicated through the table history.
- `DescribeTable` of a paired table shows `Replicas` with the peer's region name and its status
  (`CREATING`, `ACTIVE`, `DELETING`) and `MultiRegionConsistency: EVENTUAL`.
- Pairing fails with a validation error when the table already has a replica or the peer already
  has a table of that name.

## Conflicts

The whole item is last-writer-wins. Each accepted write carries an order of (time, region rank).
The higher order wins the complete item, including deletion. The result is one of the two images,
never a merge of attributes, and concurrent increments do not add. A delete wins against an older
write and loses against a newer one. A removal marker is kept until the peer echoes it, so a late
older write cannot resurrect a deleted item.

Conditions and update expressions run at the originating region; replication carries the resulting
item.

## Table changes are regional

Global secondary indexes, TTL settings and stream view type are per region and are not replicated.
A change in one region does not change the other. TTL deletes replicate like other deletes.

## Removal

- `ReplicaUpdates [{Delete: {RegionName: <peer>}}]` removes the peer gracefully. The peer stops
  accepting customer writes ("Table replica is being removed"), sends everything it accepted, and
  both sides then end the pair. Writes of this region that the peer had not yet received are not
  applied there. The peer must be reachable.
- `ReplicaUpdates [{Delete: {RegionName: <local region>}}]` forces the end: this region unpairs at
  once, in any state, and tells the peer on a best-effort basis. Undelivered changes in either
  direction are dropped. Use it when the peer is unreachable.
- After either, both tables are independent and keep their items. Pair-end cleanup removes all
  markers and progress of the pair in the same commit that unbinds it, so the table can be
  deleted afterwards without leftovers.
- A region whose peer was forced while it was unreachable stays paired and keeps holding history
  (the peer answers every page as an unknown pair). No automatic detach exists; force the end in
  this region too.

## Outage and lag

When the peer is unreachable the pair stays attached and each region keeps retrying. History
needed by the peer is held and is not trimmed until it is delivered, so stream storage grows
during a long outage. No age limit detaches the peer.

Each send turn records the age of the oldest unsent history entry (0 when idle) in
`storage.regional.lag_ms{connection}`, the largest over the connection's pairs that this process ran recently;
the alert log line names the table. The log event `regional.lag.alert` (warn) is written once when the lag
crosses the configured `lag_alert_ms`, and `regional.lag.recovered` (info) once when it falls
back. See [OBSERVABILITY.md](../OBSERVABILITY.md).

## Restore

A table restored from a backup must end any imported pair before it serves traffic:
call `force_unpair(table, None)`. This is the last step of the
[single-table restore runbook](../docs/runbooks/single-table-restore.md). Then pair again with
`ReplicaUpdates Create`.
