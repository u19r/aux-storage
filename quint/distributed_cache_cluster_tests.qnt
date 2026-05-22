/// Test scenarios for the distributed cache cluster model.
///
/// Each `run` exercises a specific scenario involving leader election,
/// network partitions, shard migration, and 3-node quorum semantics.
module distributed_cache_cluster_tests {
  import distributed_cache_cluster.* from "distributed_cache_cluster"

  // -------------------------------------------------------------------------
  //  Helper assertions
  // -------------------------------------------------------------------------

  def nodeIsClusterLeader(node) =
    nodes.get(node).role == "leader"

  def nodeIsFollower(node) =
    nodes.get(node).role == "follower"

  def nodeIsCandidate(node) =
    nodes.get(node).role == "candidate"

  def shardLeaderIs(shard, node) =
    nodes.get(node).shardState.get(shard).role == "leader"
    and nodes.get(node).shardState.get(shard).serving

  def shardHasAuthority(shard, node) =
    nodes.get(node).shardState.get(shard).itemAuthority

  def canReadShard(node, shard) =
    readRoute(nodes, ring, node, shard, nodes.get(node).currentTerm) == ROUTE_OK

  def cannotReadShard(node, shard) =
    readRoute(nodes, ring, node, shard, nodes.get(node).currentTerm) != ROUTE_OK

  def writeAllowed(node, shard) =
    canWrite(nodes, ring, node, shard)

  def writeBlocked(node, shard) =
    not(canWrite(nodes, ring, node, shard))

  // -------------------------------------------------------------------------
  //  Initial state tests
  // -------------------------------------------------------------------------

  /// Verify the initial state has node 0 as cluster leader.
  run initialStateHasLeader =
    init
      .expect(nodeIsClusterLeader(0))
      .expect(nodeIsFollower(1))
      .expect(nodeIsFollower(2))

  /// Each shard has exactly one leader in initial state.
  run initialStateEachShardHasOneLeader =
    init
      .expect(shardLeaderIs(0, 0))
      .expect(shardLeaderIs(1, 1))
      .expect(shardLeaderIs(2, 2))

  /// All shards can be read/written in initial state.
  run initialStateAllShardsAccessible =
    init
      .expect(canReadShard(0, 0))
      .expect(canReadShard(1, 1))
      .expect(canReadShard(2, 2))
      .expect(writeAllowed(0, 0))
      .expect(writeAllowed(1, 1))
      .expect(writeAllowed(2, 2))

  /// Non-leader nodes cannot serve writes for their non-primary shards.
  run followerCannotServeWrites =
    init
      .expect(writeBlocked(1, 0))
      .expect(writeBlocked(0, 1))
      .expect(writeBlocked(0, 2))

  // -------------------------------------------------------------------------
  //  Leader election tests
  // -------------------------------------------------------------------------

  /// Node 1 starts an election after node 0 is partitioned.
  run partitionTriggersElection =
    init
      .then(partitionNode(0))
      .expect(not(nodes.get(1).reachable.contains(0)))
      .expect(not(nodes.get(2).reachable.contains(0)))
      .then(stepDownOnQuorumLoss(0))
      .expect(nodeIsFollower(0))
      .then(startElection(1))
      .expect(nodeIsCandidate(1))
      .expect(nodes.get(1).currentTerm == 1)

  /// Node 1 wins election with vote from node 2.
  run electionWithQuorumSucceeds =
    init
      .then(partitionNode(0))
      .then(stepDownOnQuorumLoss(0))
      .then(startElection(1))
      .expect(nodeIsCandidate(1))
      // Deliver vote request to node 2
      .then(
        deliverMessage({
          kind: "vote_request",
          from: 1,
          to: 2,
          term: 1,
          shard: -1,
          granted: false,
          epoch: 0,
        })
      )
      // Deliver vote response from node 2 back to node 1
      .then(
        deliverMessage({
          kind: "vote_response",
          from: 2,
          to: 1,
          term: 1,
          shard: -1,
          granted: true,
          epoch: 0,
        })
      )
      .expect(nodeIsClusterLeader(1))
      .expect(nodes.get(1).currentTerm == 1)

  /// Partitioned node cannot win election (no quorum).
  run partitionedNodeCannotWinElection =
    init
      .then(partitionNode(2))
      .then(startElection(2))
      .expect(nodeIsCandidate(2))
      // Node 2 can only reach itself, so votesReceived = {2}
      .expect(nodes.get(2).votesReceived == Set(2))
      // Cannot become leader with 1 vote (need 2)
      .expect(not(nodeIsClusterLeader(2)))

  // -------------------------------------------------------------------------
  //  Network partition + quorum loss tests
  // -------------------------------------------------------------------------

  /// Stepping down on quorum loss strips all shard authority.
  run quorumLossStripsAllAuthority =
    init
      .then(partitionNode(0))
      .then(stepDownOnQuorumLoss(0))
      .expect(nodeIsFollower(0))
      .expect(writeBlocked(0, 0))
      .expect(writeBlocked(0, 1))
      .expect(writeBlocked(0, 2))
      .expect(cannotReadShard(0, 0))

  /// After heal, a new election can restore leadership.
  run healAndReelectRestoresService =
    init
      .then(partitionNode(0))
      .then(stepDownOnQuorumLoss(0))
      .expect(writeBlocked(0, 0))
      .then(healPartition)
      .expect(nodes.get(0).reachable == NODES)
      // Node 0 can start a new election
      .then(startElection(0))
      .expect(nodeIsCandidate(0))
      .expect(nodes.get(0).currentTerm == 1)

  /// Partial partition: node 0 loses contact with node 2 only.
  /// Node 0 still has quorum (0, 1) so it can still serve.
  run partialPartitionKeepsQuorum =
    init
      .then(partialPartition(0, 2))
      .expect(nodes.get(0).reachable == Set(0, 1))
      .expect(hasQuorum(nodes.get(0)))
      .expect(canReadShard(0, 0))
      .expect(writeAllowed(0, 0))

  // -------------------------------------------------------------------------
  //  Shard migration tests
  // -------------------------------------------------------------------------

  /// Initiate migration of shard 0 from node 0 to node 2.
  run migrationInitiationStripQueryAuthority =
    init
      .then(initiateMigration(0, 2))
      .expect(ring.migrationSource.get(0) == 0)
      .expect(ring.migrationTarget.get(0) == 2)
      .expect(nodes.get(0).shardState.get(0).migrating)
      .expect(not(nodes.get(0).shardState.get(0).queryAuthority))
      // But item authority is still held (point reads ok)
      .expect(nodes.get(0).shardState.get(0).itemAuthority)

  /// Migration drain sends transfer message and strips source authority.
  run migrationDrainSendsTransfer =
    init
      .then(initiateMigration(0, 2))
      .then(completeMigrationDrain(0))
      .expect(not(nodes.get(0).shardState.get(0).serving))
      .expect(not(nodes.get(0).shardState.get(0).itemAuthority))
      .expect(network.size() > 0)

  /// Complete migration: target receives shard.
  run migrationCompleteTransfersOwnership =
    init
      .then(initiateMigration(0, 2))
      .then(completeMigrationDrain(0))
      .then(
        deliverMessage({
          kind: "shard_transfer",
          from: 0,
          to: 2,
          term: 0,
          shard: 0,
          granted: false,
          epoch: 0,
        })
      )
      .expect(nodes.get(2).shardState.get(0).role == "leader")
      .expect(nodes.get(2).shardState.get(0).serving)
      .expect(nodes.get(2).shardState.get(0).itemAuthority)
      .expect(ring.assignment.get(0) == 2)
      .expect(not(isMigrating(ring, 0)))

  /// During migration, writes are blocked on the migrating shard.
  run migrationBlocksWrites =
    init
      .then(initiateMigration(0, 2))
      .expect(writeBlocked(0, 0))
      .expect(readRoute(nodes, ring, 0, 0, 0) == ROUTE_MIGRATING)

  /// Abort migration restores source authority.
  run abortMigrationRestoresAuthority =
    init
      .then(initiateMigration(0, 2))
      .expect(not(nodes.get(0).shardState.get(0).queryAuthority))
      .then(abortMigration(0))
      .expect(nodes.get(0).shardState.get(0).queryAuthority)
      .expect(not(nodes.get(0).shardState.get(0).migrating))
      .expect(not(isMigrating(ring, 0)))

  // -------------------------------------------------------------------------
  //  Epoch tests
  // -------------------------------------------------------------------------

  /// Bumping epoch doesn't break serving.
  run epochBumpPreservesService =
    init
      .then(bumpEpoch(0, 0))
      .expect(nodes.get(0).shardState.get(0).epoch == 1)
      .expect(canReadShard(0, 0))
      .expect(writeAllowed(0, 0))

  /// Stale term read is rejected.
  run staleTermReadsRejected =
    init
      .expect(readRoute(nodes, ring, 0, 0, -1) == ROUTE_STALE_TERM)

  // -------------------------------------------------------------------------
  //  Replication (append) tests
  // -------------------------------------------------------------------------

  /// Leader sends append and follower acknowledges.
  run appendReplicationRoundTrip =
    init
      .then(sendAppend(0, 1, 0))
      .expect(network.size() == 1)
      .then(
        deliverMessage({
          kind: "append",
          from: 0,
          to: 1,
          term: 0,
          shard: 0,
          granted: false,
          epoch: 0,
        })
      )
      .expect(nodes.get(1).shardState.get(0).role == "follower")
      .expect(nodes.get(1).shardState.get(0).epoch == 0)
      // An ack should be in the network
      .then(
        deliverMessage({
          kind: "append_ack",
          from: 1,
          to: 0,
          term: 0,
          shard: 0,
          granted: true,
          epoch: 0,
        })
      )
      .expect(nodes.get(0).appendAcks.get(0).contains(1))

  /// Dropped append doesn't break anything.
  run droppedAppendIsHarmless =
    init
      .then(sendAppend(0, 1, 0))
      .then(
        dropMessage({
          kind: "append",
          from: 0,
          to: 1,
          term: 0,
          shard: 0,
          granted: false,
          epoch: 0,
        })
      )
      .expect(network.size() == 0)
      .expect(canReadShard(0, 0))

  // -------------------------------------------------------------------------
  //  Split-brain prevention tests
  // -------------------------------------------------------------------------

  /// After partition + election, old leader that stepped down cannot serve.
  run splitBrainPreventedByStepDown =
    init
      .then(partitionNode(0))
      .then(stepDownOnQuorumLoss(0))
      .then(startElection(1))
      .then(
        deliverMessage({
          kind: "vote_request",
          from: 1,
          to: 2,
          term: 1,
          shard: -1,
          granted: false,
          epoch: 0,
        })
      )
      .then(
        deliverMessage({
          kind: "vote_response",
          from: 2,
          to: 1,
          term: 1,
          shard: -1,
          granted: true,
          epoch: 0,
        })
      )
      .expect(nodeIsClusterLeader(1))
      .expect(nodeIsFollower(0))
      .expect(writeBlocked(0, 0))
      // Node 1 is now leader for shard 0 (it's the primary under default ring
      // assignment, but node 0 was primary - after election node 1 gets shards
      // where it is primary)
      .expect(writeAllowed(1, 1))

  /// Migration during partition: if source is partitioned mid-migration,
  /// the shard transfer message is lost and migration can be aborted when healed.
  run migrationDuringPartitionIsSafe =
    init
      .then(initiateMigration(0, 2))
      .then(completeMigrationDrain(0))
      // Now partition node 2 so it can't receive the transfer
      .then(partitionNode(2))
      // The transfer message is in the network but can't be delivered
      .then(
        dropMessage({
          kind: "shard_transfer",
          from: 0,
          to: 2,
          term: 0,
          shard: 0,
          granted: false,
          epoch: 0,
        })
      )
      // Source has already given up authority, target never got it
      .expect(not(nodes.get(0).shardState.get(0).serving))
      .expect(not(nodes.get(2).shardState.get(0).serving))
      // The shard is unavailable until healed — this is correct behavior
      .expect(not(shardHasActiveServer(nodes, 0)))

  // -------------------------------------------------------------------------
  //  3-node quorum edge cases
  // -------------------------------------------------------------------------

  /// With 3 nodes, losing 2 means no quorum at all.
  run twoNodeFailurePreventsAllWrites =
    init
      .then(partitionNode(1))
      .then(partitionNode(2))
      // Node 0 now can only reach itself
      .expect(nodes.get(0).reachable == Set(0))
      .expect(not(hasQuorum(nodes.get(0))))
      .then(stepDownOnQuorumLoss(0))
      .expect(writeBlocked(0, 0))
      .expect(writeBlocked(0, 1))
      .expect(writeBlocked(0, 2))

  /// After healing from a 2-node failure, election restores service.
  run recoveryFromTwoNodeFailure =
    init
      .then(partitionNode(1))
      .then(partitionNode(2))
      .then(stepDownOnQuorumLoss(0))
      .then(healPartition)
      .then(startElection(0))
      // Need votes from others
      .then(
        deliverMessage({
          kind: "vote_request",
          from: 0,
          to: 1,
          term: 1,
          shard: -1,
          granted: false,
          epoch: 0,
        })
      )
      .then(
        deliverMessage({
          kind: "vote_response",
          from: 1,
          to: 0,
          term: 1,
          shard: -1,
          granted: true,
          epoch: 0,
        })
      )
      .expect(nodeIsClusterLeader(0))
      .expect(writeAllowed(0, 0))
}
