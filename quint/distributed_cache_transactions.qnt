module distributed_cache_transactions {
  pure val SHARDS = Set(0, 1)
  pure val SLOT_SET = Set(0, 1, 2, 3)

  pure val TXN_PUTS = Set(0)
  pure val TXN_DELETES = Set(2)
  pure val INITIAL_DB_PRESENT = Set(2)
  pure val COMMITTED_DB_PRESENT =
    INITIAL_DB_PRESENT.union(TXN_PUTS).exclude(TXN_DELETES)

  pure val TXN_NONE = "none"
  pure val TXN_COMMIT = "commit"
  pure val TXN_ABORT = "abort"

  type ShardState = {
    payloadKeys: Set[int],
    negativeKeys: Set[int],
    knownManifestKeys: Set[int],
    coveredSlots: Set[int],
    preparedPuts: Set[int],
    preparedDeletes: Set[int],
    currentLeader: bool,
    authoritativeEpoch: bool,
    cachedWritesOnly: bool,
    itemAuthority: bool,
    queryAuthority: bool,
    shardAssigned: bool,
  }

  type TxnState = {
    dbPresent: Set[int],
    leftLeader: ShardState,
    rightLeader: ShardState,
    leftFollower: ShardState,
    rightFollower: ShardState,
    preparedShards: Set[int],
    followerPreparedShards: Set[int],
    appliedShards: Set[int],
    followerAppliedShards: Set[int],
    servingFromFollower: Set[int],
    txnOutcome: str,
  }

  var txn: TxnState

  pure def shardOf(slot) =
    if (slot < 2) 0 else 1

  pure def slotsForShard(shard) =
    SLOT_SET.filter(slot => shardOf(slot) == shard)

  pure def txnPutsForShard(shard) =
    TXN_PUTS.intersect(slotsForShard(shard))

  pure def txnDeletesForShard(shard) =
    TXN_DELETES.intersect(slotsForShard(shard))

  pure def makeShardCopy(dbPresent, shard, currentLeaderValue) = {
    pure val shardSlots = slotsForShard(shard)
    pure val present = shardSlots.intersect(dbPresent)
    pure val absent = shardSlots.exclude(dbPresent)
    {
      payloadKeys: present,
      negativeKeys: absent,
      knownManifestKeys: present,
      coveredSlots: shardSlots,
      preparedPuts: Set(),
      preparedDeletes: Set(),
      currentLeader: currentLeaderValue,
      authoritativeEpoch: true,
      cachedWritesOnly: true,
      itemAuthority: true,
      queryAuthority: true,
      shardAssigned: true,
    }
  }

  pure def leaderShardState(state, shard) =
    if (shard == 0) state.leftLeader else state.rightLeader

  pure def followerShardState(state, shard) =
    if (shard == 0) state.leftFollower else state.rightFollower

  pure def servingShardState(state, shard) =
    if (state.servingFromFollower.contains(shard))
      followerShardState(state, shard)
    else
      leaderShardState(state, shard)

  pure def withLeaderShard(state, shard, shardStateValue) =
    if (shard == 0)
      state.with("leftLeader", shardStateValue)
    else
      state.with("rightLeader", shardStateValue)

  pure def withFollowerShard(state, shard, shardStateValue) =
    if (shard == 0)
      state.with("leftFollower", shardStateValue)
    else
      state.with("rightFollower", shardStateValue)

  pure def unresolvedTxnKeys(shardStateValue) =
    shardStateValue.preparedPuts.union(shardStateValue.preparedDeletes)

  pure def shardPresentKeys(dbPresent, shard) =
    slotsForShard(shard).intersect(dbPresent)

  pure def shardAbsentKeys(dbPresent, shard) =
    slotsForShard(shard).exclude(dbPresent)

  pure def validShardState(shardStateValue, dbPresent, shard) = {
    pure val shardSlots = slotsForShard(shard)
    pure val unresolved = unresolvedTxnKeys(shardStateValue)
    and {
      shardStateValue.payloadKeys.subseteq(shardSlots),
      shardStateValue.negativeKeys.subseteq(shardSlots),
      shardStateValue.knownManifestKeys.subseteq(shardSlots),
      shardStateValue.coveredSlots.subseteq(shardSlots),
      unresolved.subseteq(shardSlots),
      shardStateValue.payloadKeys.exclude(unresolved)
        .subseteq(shardPresentKeys(dbPresent, shard)),
      shardStateValue.negativeKeys.exclude(unresolved)
        .subseteq(shardAbsentKeys(dbPresent, shard)),
      shardStateValue.knownManifestKeys.exclude(unresolved)
        .subseteq(shardPresentKeys(dbPresent, shard)),
      shardPresentKeys(dbPresent, shard)
        .intersect(shardStateValue.coveredSlots)
        .exclude(unresolved)
        .subseteq(shardStateValue.knownManifestKeys),
      shardStateValue.preparedPuts.intersect(shardStateValue.preparedDeletes).size() == 0,
      not(shardStateValue.itemAuthority) or shardStateValue.authoritativeEpoch,
      not(shardStateValue.queryAuthority) or shardStateValue.itemAuthority,
      not(shardStateValue.itemAuthority) or shardStateValue.shardAssigned,
    }
  }

  pure def validTxnState(state) =
    and {
      validShardState(state.leftLeader, state.dbPresent, 0),
      validShardState(state.rightLeader, state.dbPresent, 1),
      validShardState(state.leftFollower, state.dbPresent, 0),
      validShardState(state.rightFollower, state.dbPresent, 1),
      state.appliedShards.subseteq(state.preparedShards),
      state.followerAppliedShards.subseteq(state.followerPreparedShards),
      state.preparedShards.subseteq(SHARDS),
      state.followerPreparedShards.subseteq(SHARDS),
      state.servingFromFollower.subseteq(SHARDS),
      SHARDS.forall(shard =>
        if (state.servingFromFollower.contains(shard))
          and {
            followerShardState(state, shard).currentLeader,
            not(leaderShardState(state, shard).currentLeader),
          }
        else
          leaderShardState(state, shard).currentLeader
      ),
      if (state.txnOutcome == TXN_NONE)
        state.dbPresent == INITIAL_DB_PRESENT
      else if (state.txnOutcome == TXN_ABORT)
        state.dbPresent == INITIAL_DB_PRESENT
      else if (state.txnOutcome == TXN_COMMIT)
        state.dbPresent == COMMITTED_DB_PRESENT
      else
        false,
    }

  pure def itemProofActive(shardStateValue) =
    and {
      shardStateValue.currentLeader,
      shardStateValue.authoritativeEpoch,
      shardStateValue.cachedWritesOnly,
      shardStateValue.itemAuthority,
      shardStateValue.shardAssigned,
    }

  pure def queryProofActive(shardStateValue) =
    and {
      itemProofActive(shardStateValue),
      shardStateValue.queryAuthority,
    }

  pure def canServeEventualGet(state, slot) = {
    pure val shard = shardOf(slot)
    pure val local = servingShardState(state, shard)
    and {
      itemProofActive(local),
      not(unresolvedTxnKeys(local).contains(slot)),
      or {
        local.payloadKeys.contains(slot),
        local.negativeKeys.contains(slot),
      },
    }
  }

  pure def batchGetServedKeys(state, requestedKeys) =
    requestedKeys.filter(slot => canServeEventualGet(state, slot))

  pure def canServeShardQuery(state, shard) = {
    pure val local = servingShardState(state, shard)
    and {
      queryProofActive(local),
      local.coveredSlots == slotsForShard(shard),
      unresolvedTxnKeys(local).size() == 0,
      local.knownManifestKeys == shardPresentKeys(state.dbPresent, shard),
    }
  }

  pure def canServeTransactionalGet(_state, _slot) = false

  pure def prepareShardCopy(local, shard) =
    local
      .with("preparedPuts", local.preparedPuts.union(txnPutsForShard(shard)))
      .with(
        "preparedDeletes",
        local.preparedDeletes.union(txnDeletesForShard(shard))
      )

  pure def applyOutcomeToCopy(local, shard, outcome) = {
    pure val outcomeApplied =
      if (outcome == TXN_COMMIT)
        local
          .with(
            "payloadKeys",
            local.payloadKeys.union(txnPutsForShard(shard)).exclude(
              txnDeletesForShard(shard)
            )
          )
          .with(
            "negativeKeys",
            local.negativeKeys.exclude(txnPutsForShard(shard)).union(
              txnDeletesForShard(shard)
            )
          )
          .with(
            "knownManifestKeys",
            local.knownManifestKeys.union(txnPutsForShard(shard)).exclude(
              txnDeletesForShard(shard)
            )
          )
      else
        local
    outcomeApplied
      .with("preparedPuts", Set())
      .with("preparedDeletes", Set())
  }

  pure def prepareLeaderShard(state, shard) =
    withLeaderShard(
      state.with("preparedShards", state.preparedShards.union(Set(shard))),
      shard,
      prepareShardCopy(leaderShardState(state, shard), shard),
    )

  pure def replicatePrepareToFollower(state, shard) =
    withFollowerShard(
      state
        .with(
          "followerPreparedShards",
          state.followerPreparedShards.union(Set(shard))
        ),
      shard,
      prepareShardCopy(followerShardState(state, shard), shard)
    )

  pure def commitSource(state) =
    state
      .with("dbPresent", COMMITTED_DB_PRESENT)
      .with("txnOutcome", TXN_COMMIT)

  pure def abortSource(state) =
    state.with("txnOutcome", TXN_ABORT)

  pure def applyLeaderOutcome(state, shard) =
    withLeaderShard(
      state.with("appliedShards", state.appliedShards.union(Set(shard))),
      shard,
      applyOutcomeToCopy(
        leaderShardState(state, shard),
        shard,
        state.txnOutcome,
      ),
    )

  pure def applyFollowerOutcome(state, shard) =
    withFollowerShard(
      state
        .with(
          "followerAppliedShards",
          state.followerAppliedShards.union(Set(shard))
        ),
      shard,
      applyOutcomeToCopy(
        followerShardState(state, shard),
        shard,
        state.txnOutcome
      )
    )

  pure def promoteFollower(state, shard) = {
    pure val leader =
      leaderShardState(state, shard).with("currentLeader", false)
    pure val follower =
      followerShardState(state, shard).with("currentLeader", true)
    withFollowerShard(
      withLeaderShard(
        state.with("servingFromFollower", state.servingFromFollower.union(Set(shard))),
        shard,
        leader,
      ),
      shard,
      follower,
    )
  }

  pure def replayClientToken(state) = state

  pure val initialTxnState = {
    dbPresent: INITIAL_DB_PRESENT,
    leftLeader: makeShardCopy(INITIAL_DB_PRESENT, 0, true),
    rightLeader: makeShardCopy(INITIAL_DB_PRESENT, 1, true),
    leftFollower: makeShardCopy(INITIAL_DB_PRESENT, 0, false),
    rightFollower: makeShardCopy(INITIAL_DB_PRESENT, 1, false),
    preparedShards: Set(),
    followerPreparedShards: Set(),
    appliedShards: Set(),
    followerAppliedShards: Set(),
    servingFromFollower: Set(),
    txnOutcome: TXN_NONE,
  }

  action init =
    txn' = initialTxnState

  action prepareLeftAction =
    all {
      txn.txnOutcome == TXN_NONE,
      not(txn.preparedShards.contains(0)),
      unresolvedTxnKeys(leaderShardState(txn, 0)).size() == 0,
      txn' = prepareLeaderShard(txn, 0),
    }

  action prepareRightAction =
    all {
      txn.txnOutcome == TXN_NONE,
      not(txn.preparedShards.contains(1)),
      unresolvedTxnKeys(leaderShardState(txn, 1)).size() == 0,
      txn' = prepareLeaderShard(txn, 1),
    }

  action replicateLeftPrepareAction =
    all {
      txn.preparedShards.contains(0),
      not(txn.followerPreparedShards.contains(0)),
      txn' = replicatePrepareToFollower(txn, 0),
    }

  action replicateRightPrepareAction =
    all {
      txn.preparedShards.contains(1),
      not(txn.followerPreparedShards.contains(1)),
      txn' = replicatePrepareToFollower(txn, 1),
    }

  action commitSourceAction =
    all {
      txn.followerPreparedShards == SHARDS,
      txn.txnOutcome == TXN_NONE,
      txn' = commitSource(txn),
    }

  action abortSourceAction =
    all {
      txn.preparedShards.size() > 0,
      txn.preparedShards == txn.followerPreparedShards,
      txn.txnOutcome == TXN_NONE,
      txn' = abortSource(txn),
    }

  action applyLeftOutcomeAction =
    all {
      txn.txnOutcome != TXN_NONE,
      txn.preparedShards.contains(0),
      not(txn.appliedShards.contains(0)),
      txn' = applyLeaderOutcome(txn, 0),
    }

  action applyRightOutcomeAction =
    all {
      txn.txnOutcome != TXN_NONE,
      txn.preparedShards.contains(1),
      not(txn.appliedShards.contains(1)),
      txn' = applyLeaderOutcome(txn, 1),
    }

  action replicateLeftOutcomeAction =
    all {
      txn.txnOutcome != TXN_NONE,
      txn.followerPreparedShards.contains(0),
      txn.appliedShards.contains(0),
      not(txn.followerAppliedShards.contains(0)),
      txn' = applyFollowerOutcome(txn, 0),
    }

  action replicateRightOutcomeAction =
    all {
      txn.txnOutcome != TXN_NONE,
      txn.followerPreparedShards.contains(1),
      txn.appliedShards.contains(1),
      not(txn.followerAppliedShards.contains(1)),
      txn' = applyFollowerOutcome(txn, 1),
    }

  action promoteLeftFollowerAction =
    all {
      not(txn.servingFromFollower.contains(0)),
      txn.followerPreparedShards.contains(0),
      txn' = promoteFollower(txn, 0),
    }

  action promoteRightFollowerAction =
    all {
      not(txn.servingFromFollower.contains(1)),
      txn.followerPreparedShards.contains(1),
      txn' = promoteFollower(txn, 1),
    }

  action recoverLeftPromotedFollowerAction =
    all {
      txn.servingFromFollower.contains(0),
      txn.txnOutcome != TXN_NONE,
      txn.followerPreparedShards.contains(0),
      not(txn.followerAppliedShards.contains(0)),
      txn' = applyFollowerOutcome(txn, 0),
    }

  action recoverRightPromotedFollowerAction =
    all {
      txn.servingFromFollower.contains(1),
      txn.txnOutcome != TXN_NONE,
      txn.followerPreparedShards.contains(1),
      not(txn.followerAppliedShards.contains(1)),
      txn' = applyFollowerOutcome(txn, 1),
    }

  action replayClientTokenAction =
    all {
      txn.txnOutcome != TXN_NONE,
      txn' = replayClientToken(txn),
    }

  action step =
    any {
      prepareLeftAction,
      prepareRightAction,
      replicateLeftPrepareAction,
      replicateRightPrepareAction,
      commitSourceAction,
      abortSourceAction,
      applyLeftOutcomeAction,
      applyRightOutcomeAction,
      replicateLeftOutcomeAction,
      replicateRightOutcomeAction,
      promoteLeftFollowerAction,
      promoteRightFollowerAction,
      recoverLeftPromotedFollowerAction,
      recoverRightPromotedFollowerAction,
      replayClientTokenAction,
    }

  pure def servingUnresolvedKeys(state) =
    unresolvedTxnKeys(servingShardState(state, 0)).union(
      unresolvedTxnKeys(servingShardState(state, 1))
    )

  val validLiveTxnState = validTxnState(txn)

  val preparedTxnGetsAreFenced =
    SLOT_SET.forall(slot =>
      not(unresolvedTxnKeys(servingShardState(txn, shardOf(slot))).contains(slot))
        or not(canServeEventualGet(txn, slot))
    )

  val preparedTxnBatchGetsExcludeLockedKeys =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetServedKeys(txn, requestedKeys)
        .intersect(servingUnresolvedKeys(txn))
        == Set()
    )

  val preparedTxnQueriesAreFenced =
    SHARDS.forall(shard =>
      unresolvedTxnKeys(servingShardState(txn, shard)).size() == 0
        or not(canServeShardQuery(txn, shard))
    )

  val outcomeOnlyAfterReplicatedPrepare =
    txn.txnOutcome == TXN_NONE
      or txn.preparedShards == txn.followerPreparedShards

  val committedSourceStateIsAtomic =
    txn.txnOutcome != TXN_COMMIT or txn.dbPresent == COMMITTED_DB_PRESENT

  val abortedSourceStateIsUnchanged =
    txn.txnOutcome != TXN_ABORT or txn.dbPresent == INITIAL_DB_PRESENT

  val appliedLeaderCommitMatchesSource =
    SHARDS.forall(shard =>
      txn.txnOutcome != TXN_COMMIT
        or not(txn.appliedShards.contains(shard))
        or and {
          leaderShardState(txn, shard).knownManifestKeys
            == shardPresentKeys(txn.dbPresent, shard),
          leaderShardState(txn, shard).payloadKeys
            == shardPresentKeys(txn.dbPresent, shard),
          leaderShardState(txn, shard).negativeKeys
            == shardAbsentKeys(txn.dbPresent, shard),
        }
    )

  val appliedFollowerCommitMatchesSource =
    SHARDS.forall(shard =>
      txn.txnOutcome != TXN_COMMIT
        or not(txn.followerAppliedShards.contains(shard))
        or and {
          followerShardState(txn, shard).knownManifestKeys
            == shardPresentKeys(txn.dbPresent, shard),
          followerShardState(txn, shard).payloadKeys
            == shardPresentKeys(txn.dbPresent, shard),
          followerShardState(txn, shard).negativeKeys
            == shardAbsentKeys(txn.dbPresent, shard),
        }
    )

  val committedButUnappliedServingKeysStayFenced =
    SHARDS.forall(shard =>
      txn.txnOutcome != TXN_COMMIT
        or (
          if (txn.servingFromFollower.contains(shard))
            txn.followerAppliedShards.contains(shard)
          else
            txn.appliedShards.contains(shard)
        )
        or and {
          txnPutsForShard(shard).forall(slot => not(canServeEventualGet(txn, slot))),
          txnDeletesForShard(shard).forall(slot => not(canServeEventualGet(txn, slot))),
          not(canServeShardQuery(txn, shard)),
        }
    )

  val transactionalGetsAlwaysBypass =
    SLOT_SET.forall(slot => not(canServeTransactionalGet(txn, slot)))

  run commitKeepsLeaderReadsFencedUntilEachLeaderAppliesOutcome =
    init
      .then(prepareLeftAction)
      .then(replicateLeftPrepareAction)
      .then(prepareRightAction)
      .then(replicateRightPrepareAction)
      .then(commitSourceAction)
      .expect(not(canServeEventualGet(txn, 0)))
      .expect(not(canServeEventualGet(txn, 2)))
      .expect(not(canServeShardQuery(txn, 0)))
      .expect(not(canServeShardQuery(txn, 1)))
      .then(applyLeftOutcomeAction)
      .expect(canServeEventualGet(txn, 0))
      .expect(not(canServeEventualGet(txn, 2)))
      .expect(canServeShardQuery(txn, 0))
      .expect(not(canServeShardQuery(txn, 1)))
      .then(applyRightOutcomeAction)
      .expect(canServeEventualGet(txn, 0))
      .expect(canServeEventualGet(txn, 2))
      .expect(canServeShardQuery(txn, 0))
      .expect(canServeShardQuery(txn, 1))

  run promotedFollowerStaysFencedUntilRecovery =
    init
      .then(prepareLeftAction)
      .then(replicateLeftPrepareAction)
      .then(prepareRightAction)
      .then(replicateRightPrepareAction)
      .then(commitSourceAction)
      .then(promoteLeftFollowerAction)
      .expect(not(canServeEventualGet(txn, 0)))
      .expect(not(canServeShardQuery(txn, 0)))
      .then(recoverLeftPromotedFollowerAction)
      .expect(canServeEventualGet(txn, 0))
      .expect(canServeShardQuery(txn, 0))

  run replicatedFollowerOutcomeMakesPromotionImmediate =
    init
      .then(prepareLeftAction)
      .then(replicateLeftPrepareAction)
      .then(prepareRightAction)
      .then(replicateRightPrepareAction)
      .then(commitSourceAction)
      .then(applyLeftOutcomeAction)
      .then(replicateLeftOutcomeAction)
      .then(promoteLeftFollowerAction)
      .expect(canServeEventualGet(txn, 0))
      .expect(canServeShardQuery(txn, 0))

  run abortReleasesLocksWithoutChangingSourceState =
    init
      .then(prepareLeftAction)
      .then(replicateLeftPrepareAction)
      .then(prepareRightAction)
      .then(replicateRightPrepareAction)
      .then(abortSourceAction)
      .expect(not(canServeEventualGet(txn, 0)))
      .expect(not(canServeEventualGet(txn, 2)))
      .then(applyLeftOutcomeAction)
      .then(applyRightOutcomeAction)
      .expect(txn.dbPresent == INITIAL_DB_PRESENT)
      .expect(canServeEventualGet(txn, 0))
      .expect(canServeEventualGet(txn, 2))
      .expect(canServeShardQuery(txn, 0))
      .expect(canServeShardQuery(txn, 1))

  run replayingCommittedClientTokenIsNoOp =
    init
      .then(prepareLeftAction)
      .then(replicateLeftPrepareAction)
      .then(prepareRightAction)
      .then(replicateRightPrepareAction)
      .then(commitSourceAction)
      .then(applyLeftOutcomeAction)
      .then(replicateLeftOutcomeAction)
      .then(applyRightOutcomeAction)
      .then(replicateRightOutcomeAction)
      .then(replayClientTokenAction)
      .expect(txn.dbPresent == COMMITTED_DB_PRESENT)
      .expect(txn.appliedShards == SHARDS)
      .expect(txn.followerAppliedShards == SHARDS)
}
