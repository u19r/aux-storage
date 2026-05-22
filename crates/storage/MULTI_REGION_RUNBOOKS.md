# Multi-Region Storage Runbooks

## Purpose

This document is the operator companion to `MULTI_REGION_FDB.md`.

It assumes:

- multi-region replication is leaderless
- the system stream is the durable replication journal
- replication endpoints are private and authenticated
- stream trim and background jobs still run, but trim must remain replication-safe and must not cut history sooner than the last 72 hours

## What Operators Need To Watch

For each source and destination region pair, watch:

- replication delay from source watermark to applied watermark
- sender queue depth
- checkpoint freshness
- last successful heartbeat age
- heartbeat round-trip time
- auth failures
- bootstrap progress
- stream growth

## Health States

Suggested interpretation:

- healthy
  - heartbeats are current
  - lag is near steady-state baseline
  - checkpoints are advancing
- degraded
  - heartbeats still arrive, but lag is growing or applies are slowing
- unhealthy
  - heartbeats are stale or absent
  - checkpoints are not moving
  - auth failures or endpoint failures are persistent

## Standard Preconditions

Before enabling or changing multi-region tables:

1. Confirm every region has a reachable replication endpoint.
2. Confirm inbound `storage-replication` auth is enabled.
3. Confirm stream trim is replication-safe for the deployment and enforces the minimum retention floor.
4. Confirm metrics and alerts are live.
5. Confirm enough disk headroom exists for backlog growth.

## Add Replica

Goal:

- attach a new destination region to an existing replicated table

Procedure:

1. Verify the destination region is healthy and reachable.
2. Verify the destination accepts the current or next `storage-replication` token.
3. Ensure region registry and runtime peer configuration are present.
4. Call `UpdateTable` with `ReplicaUpdates.Create`.
5. Confirm the control-plane state shows the new replica in bootstrap or creating state.
6. Watch bootstrap progress and lag until steady-state catchup completes.
7. Confirm the replica transitions to `ACTIVE`.

Expected signals:

- bootstrap cursor advances
- replication lag returns toward steady-state baseline after catchup
- heartbeats remain healthy during bootstrap

If bootstrap stalls:

- check auth failures
- check destination endpoint reachability
- check stream replay worker logs
- check disk and CPU saturation on the destination

## Remove Replica

Goal:

- stop replication to a region and remove it from table membership

Procedure:

1. Call `UpdateTable` with `ReplicaUpdates.Delete`.
2. Confirm the replica transitions to deleting state.
3. Confirm outbound senders stop sending for that table and region.
4. Confirm checkpoints stop advancing for the removed replica.
5. Remove peer runtime configuration only after traffic has stopped.
6. Optionally decommission or delete the destination table if the region is being retired.

Important note:

- removing a replica is a control-plane change and should be audited

## Region Outage

Symptoms:

- heartbeats fail or go stale
- replication lag grows
- checkpoints stop advancing

Expected system behavior:

- local writes continue
- backlog accumulates in the source region
- no checkpoint advancement occurs for failed deliveries

Operator actions:

1. Confirm whether the outage is network, auth, or full region failure.
2. Confirm local writes remain healthy in surviving regions.
3. Watch disk growth in regions producing backlog.
4. Do not reset checkpoints during a normal outage.
5. Wait for the region to return unless there is evidence of data loss or configuration drift.

## Region Return And Catchup

Goal:

- allow a recovered region to catch up from its existing checkpoint

Procedure:

1. Restore region service health.
2. Confirm replication endpoint auth succeeds.
3. Confirm heartbeats resume.
4. Watch backlog drain from the last stored checkpoint.
5. Verify lag returns toward steady-state baseline.

Do not:

- wipe checkpoints during a simple outage
- force a bootstrap replay unless the region lost its replicated data or checkpoint state is corrupt

## Region Rebuild Or Replacement

Use this when a region lost data or must be recreated from scratch.

Procedure:

1. Recreate the region infrastructure and storage backend.
2. Recreate the target table locally.
3. Clear or replace the checkpoint and bootstrap cursor for that region.
4. Re-add the replica or restart bootstrap replay from history.
5. Watch bootstrap progress until the region reaches `ACTIVE`.
6. Verify steady-state lag and heartbeat health afterward.

Operator warning:

- replay-from-history can be expensive for large tables

## Checkpoint Corruption

Symptoms:

- repeated replay failures at the same offset
- checkpoint moves backward unexpectedly
- health looks alive but apply progress never resumes

Procedure:

1. Freeze any manual attempts to reset state repeatedly.
2. Capture logs, current checkpoint values, and destination apply errors.
3. Determine whether the checkpoint is invalid or the destination data is inconsistent.
4. If the checkpoint alone is corrupt and the destination data is intact, reset to the last known good point if available.
5. If safe recovery is unclear, rebuild the region or table replica from bootstrap replay.

Rule:

- prefer a clean rebuild over a clever but risky manual repair

## Token Rotation With Overlap

Goal:

- rotate replication credentials without downtime

Procedure:

1. Mint a new `storage-replication` token.
2. Distribute the new token to all regions as an accepted inbound token.
3. Add the new token to outbound runtime config as the next token.
4. Confirm both old and new tokens are accepted inbound.
5. Switch outbound senders to the new token.
6. Watch heartbeat and apply success with the new token.
7. Revoke the old token after the overlap window closes.

Failure mode:

- if auth failures spike immediately after switch, re-enable the old token inbound, restore outbound use of the old token, and diagnose propagation drift

## Partition Detection

A partition may exist even when user traffic is low.

Primary signals:

- missed heartbeats
- asymmetrical region-to-region lag
- one-way auth or endpoint failures

Procedure:

1. Check the region pair matrix from the health endpoint.
2. Compare heartbeat staleness in both directions.
3. Confirm whether the issue is one-way or two-way.
4. Check network path, TLS, DNS, and auth separately.
5. Keep user writes local and avoid unnecessary operator churn unless data loss or misconfiguration is suspected.

## Growing Lag Or Backpressure

Symptoms:

- lag keeps increasing
- sender queue depth grows
- checkpoints move slowly
- heartbeats may still be healthy

Likely causes:

- destination CPU or disk saturation
- large bootstrap competing with steady-state apply
- payload size too large for the network path
- pathological hot-key contention causing high conflict churn

Procedure:

1. Check destination resource saturation.
2. Check whether bootstrap replay is active.
3. Check batch size and request error rates.
4. Check conflict counters for hot-key churn.
5. Scale or tune the destination before touching checkpoints.

## Disk Growth

Because trim must preserve at least a 72-hour replay window, disk growth is still expected.

Procedure:

1. Measure current stream growth rate.
2. Determine whether backlog is caused by a lagging peer.
3. Remove permanently dead replicas so operational policy can be reconsidered later.
4. Verify the trim job is still running and honoring the minimum retention floor.
5. Verify trim is not deleting history needed for acceptable peer recovery.
6. Escalate if projected capacity crosses agreed headroom limits.

## Security Incident On Replication Auth

Use this when a token is believed to be exposed or misused.

Procedure:

1. Mint a replacement token immediately.
2. Add the replacement as accepted inbound across all regions.
3. Switch outbound senders to the new token.
4. Revoke the compromised token as soon as healthy traffic is confirmed.
5. Review logs for unexpected replication endpoint usage.

## Recommended Chaos Drills

Run these regularly once the feature exists:

1. Kill one region while writes continue in another.
2. Partition one region pair while keeping the rest of the mesh healthy.
3. Rotate tokens during active write traffic.
4. Rebuild one region from empty state.
5. Force a large backlog and verify steady recovery without local write impact.

## Escalation Guide

Escalate immediately when:

- replication auth fails across multiple pairs
- loop prevention appears broken
- lag continues rising after the peer is confirmed healthy
- checkpoint corruption is suspected
- disk growth threatens service capacity

Escalate after observation when:

- a short outage is recovering normally
- lag is falling and checkpoints are moving
- health recovered after token rotation
