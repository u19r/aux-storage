module distributed_cache_protocol {
  import distributed_cache_model.* from "distributed_cache_model"

  pure val PROTOCOL_ACTION_SLOTS = Set(0, 1)
  pure val PROTOCOL_ACTION_LOWER_BOUNDS = Set(0, 1)
  pure val PROTOCOL_ACTION_UPPER_BOUNDS = Set(0, 1)

  type CacheState = {
    dbPresent: Set[int],
    gsiPresent: Set[int],
    gsiAltPresent: Set[int],
    leaderPayloadKeys: Set[int],
    leaderNegativeKeys: Set[int],
    leaderKnownManifestKeys: Set[int],
    leaderCoveredSlots: Set[int],
    leaderKnownGsiManifestKeys: Set[int],
    leaderCoveredGsiSlots: Set[int],
    leaderKnownGsiAltManifestKeys: Set[int],
    leaderCoveredGsiAltSlots: Set[int],
    followerPayloadKeys: Set[int],
    followerNegativeKeys: Set[int],
    followerKnownManifestKeys: Set[int],
    followerCoveredSlots: Set[int],
    followerKnownGsiManifestKeys: Set[int],
    followerCoveredGsiSlots: Set[int],
    followerKnownGsiAltManifestKeys: Set[int],
    followerCoveredGsiAltSlots: Set[int],
    preparedPuts: Set[int],
    preparedDeletes: Set[int],
    servingFromFollower: bool,
    actualLeaderNode: int,
    actualEpoch: int,
    cacheEpoch: int,
    cachedWritesOnly: bool,
    itemAuthority: bool,
    queryAuthority: bool,
    gsiQueryAuthority: bool,
    manifestRebuilding: bool,
    continuityBroken: bool,
    actualBaseSchemaVersion: int,
    actualGsiSchemaVersion: int,
    actualGsiPrimaryOrderVersion: int,
    actualGsiAltOrderVersion: int,
    leaderCurrentBaseSchemaCoveredSlots: Set[int],
    followerCurrentBaseSchemaCoveredSlots: Set[int],
    leaderCurrentGsiSchemaCoveredSlots: Set[int],
    followerCurrentGsiSchemaCoveredSlots: Set[int],
    leaderCurrentGsiAltSchemaCoveredSlots: Set[int],
    followerCurrentGsiAltSchemaCoveredSlots: Set[int],
    leaderGsiPrimaryOrderVersion: int,
    followerGsiPrimaryOrderVersion: int,
    leaderGsiAltOrderVersion: int,
    followerGsiAltOrderVersion: int,
    shardBusted: bool,
    shardAssigned: bool,
  }

  var cache: CacheState

  pure def makeCache(payloadKeys, negativeKeys, knownManifestKeys, coveredSlots) = {
    payloadKeys: payloadKeys,
    negativeKeys: negativeKeys,
    knownManifestKeys: knownManifestKeys,
    coveredSlots: coveredSlots,
  }

  pure def makeIndexCache(knownManifestKeys, coveredSlots) = {
    knownManifestKeys: knownManifestKeys,
    coveredSlots: coveredSlots,
  }

  pure def leaderCache(state) =
    makeCache(
      state.leaderPayloadKeys,
      state.leaderNegativeKeys,
      state.leaderKnownManifestKeys,
      state.leaderCoveredSlots,
    )

  pure def followerCache(state) =
    makeCache(
      state.followerPayloadKeys,
      state.followerNegativeKeys,
      state.followerKnownManifestKeys,
      state.followerCoveredSlots,
    )

  pure def leaderGsiCache(state) =
    makeIndexCache(
      state.leaderKnownGsiManifestKeys,
      state.leaderCoveredGsiSlots,
    )

  pure def leaderGsiAltCache(state) =
    makeIndexCache(
      state.leaderKnownGsiAltManifestKeys,
      state.leaderCoveredGsiAltSlots,
    )

  pure def followerGsiCache(state) =
    makeIndexCache(
      state.followerKnownGsiManifestKeys,
      state.followerCoveredGsiSlots,
    )

  pure def followerGsiAltCache(state) =
    makeIndexCache(
      state.followerKnownGsiAltManifestKeys,
      state.followerCoveredGsiAltSlots,
    )

  pure def withLeaderCache(state, leader) =
    state
      .with("leaderPayloadKeys", leader.payloadKeys)
      .with("leaderNegativeKeys", leader.negativeKeys)
      .with("leaderKnownManifestKeys", leader.knownManifestKeys)
      .with("leaderCoveredSlots", leader.coveredSlots)

  pure def withFollowerCache(state, follower) =
    state
      .with("followerPayloadKeys", follower.payloadKeys)
      .with("followerNegativeKeys", follower.negativeKeys)
      .with("followerKnownManifestKeys", follower.knownManifestKeys)
      .with("followerCoveredSlots", follower.coveredSlots)

  pure def withLeaderGsiCache(state, leader) =
    state
      .with("leaderKnownGsiManifestKeys", leader.knownManifestKeys)
      .with("leaderCoveredGsiSlots", leader.coveredSlots)

  pure def withLeaderGsiAltCache(state, leader) =
    state
      .with("leaderKnownGsiAltManifestKeys", leader.knownManifestKeys)
      .with("leaderCoveredGsiAltSlots", leader.coveredSlots)

  pure def withFollowerGsiCache(state, follower) =
    state
      .with("followerKnownGsiManifestKeys", follower.knownManifestKeys)
      .with("followerCoveredGsiSlots", follower.coveredSlots)

  pure def withFollowerGsiAltCache(state, follower) =
    state
      .with("followerKnownGsiAltManifestKeys", follower.knownManifestKeys)
      .with("followerCoveredGsiAltSlots", follower.coveredSlots)

  pure def intervalSlots(lower, upper) =
    SLOT_SET.filter(slot => lower <= slot and slot <= upper)

  pure def withLeaderCurrentBaseSchemaCoveredSlots(state, coveredSlots) =
    state.with("leaderCurrentBaseSchemaCoveredSlots", coveredSlots)

  pure def withFollowerCurrentBaseSchemaCoveredSlots(state, coveredSlots) =
    state.with("followerCurrentBaseSchemaCoveredSlots", coveredSlots)

  pure def withLeaderCurrentGsiSchemaCoveredSlots(state, coveredSlots) =
    state.with("leaderCurrentGsiSchemaCoveredSlots", coveredSlots)

  pure def withFollowerCurrentGsiSchemaCoveredSlots(state, coveredSlots) =
    state.with("followerCurrentGsiSchemaCoveredSlots", coveredSlots)

  pure def withLeaderCurrentGsiAltSchemaCoveredSlots(state, coveredSlots) =
    state.with("leaderCurrentGsiAltSchemaCoveredSlots", coveredSlots)

  pure def withFollowerCurrentGsiAltSchemaCoveredSlots(state, coveredSlots) =
    state.with("followerCurrentGsiAltSchemaCoveredSlots", coveredSlots)

  pure def queryFillCache(cacheValue, dbPresent, lower, upper) = {
    pure val filledSlots = intervalSlots(lower, upper)
    pure val presentInFill = filledSlots.intersect(dbPresent)
    pure val emptyInFill = filledSlots.exclude(dbPresent)
    cacheValue
      .with("coveredSlots", cacheValue.coveredSlots.union(filledSlots))
      .with("knownManifestKeys", cacheValue.knownManifestKeys.union(presentInFill))
      .with("payloadKeys", cacheValue.payloadKeys.union(presentInFill))
      .with("negativeKeys", cacheValue.negativeKeys.union(emptyInFill))
  }

  pure def queryFillIndexCache(cacheValue, sourceKeys, lower, upper) = {
    pure val filledSlots = intervalSlots(lower, upper)
    pure val presentInFill = filledSlots.intersect(sourceKeys)
    cacheValue
      .with("coveredSlots", cacheValue.coveredSlots.union(filledSlots))
      .with("knownManifestKeys", cacheValue.knownManifestKeys.union(presentInFill))
  }

  pure def visiblePut(cacheValue, slot) =
    cacheValue
      .with("payloadKeys", cacheValue.payloadKeys.union(Set(slot)))
      .with("negativeKeys", cacheValue.negativeKeys.exclude(Set(slot)))
      .with("knownManifestKeys", cacheValue.knownManifestKeys.union(Set(slot)))

  pure def visibleDelete(cacheValue, slot) =
    cacheValue
      .with("payloadKeys", cacheValue.payloadKeys.exclude(Set(slot)))
      .with("negativeKeys", cacheValue.negativeKeys.union(Set(slot)))
      .with("knownManifestKeys", cacheValue.knownManifestKeys.exclude(Set(slot)))

  pure def visibleGsiInsert(cacheValue, slot) =
    cacheValue.with("knownManifestKeys", cacheValue.knownManifestKeys.union(Set(slot)))

  pure def visibleGsiDelete(cacheValue, slot) =
    cacheValue.with("knownManifestKeys", cacheValue.knownManifestKeys.exclude(Set(slot)))

  pure def applyOutcomes(cacheValue, committedPuts, committedDeletes) =
    cacheValue
      .with(
        "payloadKeys",
        cacheValue.payloadKeys.union(committedPuts).exclude(committedDeletes)
      )
      .with(
        "negativeKeys",
        cacheValue.negativeKeys.exclude(committedPuts).union(committedDeletes)
      )
      .with(
        "knownManifestKeys",
        cacheValue.knownManifestKeys.union(committedPuts).exclude(committedDeletes)
      )

  pure def evictPayload(cacheValue, slot) =
    cacheValue.with("payloadKeys", cacheValue.payloadKeys.exclude(Set(slot)))

  pure def evictManifestEntry(cacheValue, slot) =
    cacheValue
      .with("knownManifestKeys", cacheValue.knownManifestKeys.exclude(Set(slot)))
      .with(
        "coveredSlots",
        if (cacheValue.knownManifestKeys.contains(slot))
          cacheValue.coveredSlots.exclude(Set(slot))
        else
          cacheValue.coveredSlots
      )

  pure def evictPayloadWithSharedProofDomain(cacheValue, slot) =
    evictManifestEntry(evictPayload(cacheValue, slot), slot)

  pure def queryFillLeader(state, lower, upper) =
    withLeaderCurrentBaseSchemaCoveredSlots(
      withLeaderCache(
        state,
        queryFillCache(leaderCache(state), state.dbPresent, lower, upper),
      ),
      state.leaderCurrentBaseSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def queryFillFollower(state, lower, upper) =
    withFollowerCurrentBaseSchemaCoveredSlots(
      withFollowerCache(
        state,
        queryFillCache(followerCache(state), state.dbPresent, lower, upper),
      ),
      state.followerCurrentBaseSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def queryFillLeaderGsi(state, lower, upper) =
    withLeaderCurrentGsiSchemaCoveredSlots(
      withLeaderGsiCache(
        state,
        queryFillIndexCache(leaderGsiCache(state), state.gsiPresent, lower, upper),
      ),
      state.leaderCurrentGsiSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def queryFillLeaderGsiAlt(state, lower, upper) =
    withLeaderCurrentGsiAltSchemaCoveredSlots(
      withLeaderGsiAltCache(
        state,
        queryFillIndexCache(leaderGsiAltCache(state), state.gsiAltPresent, lower, upper),
      ),
      state.leaderCurrentGsiAltSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def queryFillFollowerGsi(state, lower, upper) =
    withFollowerCurrentGsiSchemaCoveredSlots(
      withFollowerGsiCache(
        state,
        queryFillIndexCache(followerGsiCache(state), state.gsiPresent, lower, upper),
      ),
      state.followerCurrentGsiSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def queryFillFollowerGsiAlt(state, lower, upper) =
    withFollowerCurrentGsiAltSchemaCoveredSlots(
      withFollowerGsiAltCache(
        state,
        queryFillIndexCache(followerGsiAltCache(state), state.gsiAltPresent, lower, upper),
      ),
      state.followerCurrentGsiAltSchemaCoveredSlots.union(intervalSlots(lower, upper)),
    )

  pure def syncFollowerFromLeader(state) =
    withFollowerCurrentGsiAltSchemaCoveredSlots(
      withFollowerCurrentGsiSchemaCoveredSlots(
        withFollowerCurrentBaseSchemaCoveredSlots(
          withFollowerGsiAltCache(
            withFollowerGsiCache(
              withFollowerCache(state, leaderCache(state)),
              leaderGsiCache(state),
            ),
            leaderGsiAltCache(state),
          ),
          state.leaderCurrentBaseSchemaCoveredSlots,
        ),
        state.leaderCurrentGsiSchemaCoveredSlots,
      ),
      state.leaderCurrentGsiAltSchemaCoveredSlots,
    )
      .with("followerGsiPrimaryOrderVersion", state.leaderGsiPrimaryOrderVersion)
      .with("followerGsiAltOrderVersion", state.leaderGsiAltOrderVersion)

  /// Partially sync follower from leader, only covering the specified slots.
  /// Slots outside syncedSlots remain in their current follower state.
  pure def partialSyncFollower(state, syncedSlots) = {
    pure val leaderItems = leaderCache(state)
    pure val followerItems = followerCache(state)
    pure val syncedPayload = leaderItems.payloadKeys.intersect(syncedSlots)
    pure val unsyncedPayload = followerItems.payloadKeys.exclude(syncedSlots)
    pure val syncedNegative = leaderItems.negativeKeys.intersect(syncedSlots)
    pure val unsyncedNegative = followerItems.negativeKeys.exclude(syncedSlots)
    pure val syncedManifest = leaderItems.knownManifestKeys.intersect(syncedSlots)
    pure val unsyncedManifest = followerItems.knownManifestKeys.exclude(syncedSlots)
    pure val syncedCovered = leaderItems.coveredSlots.intersect(syncedSlots)
    pure val unsyncedCovered = followerItems.coveredSlots.exclude(syncedSlots)
    pure val mergedFollower = makeCache(
      syncedPayload.union(unsyncedPayload),
      syncedNegative.union(unsyncedNegative),
      syncedManifest.union(unsyncedManifest),
      syncedCovered.union(unsyncedCovered),
    )
    pure val syncedBaseSchemaCovered =
      state.leaderCurrentBaseSchemaCoveredSlots.intersect(syncedSlots)
        .union(state.followerCurrentBaseSchemaCoveredSlots.exclude(syncedSlots))
    withFollowerCurrentBaseSchemaCoveredSlots(
      withFollowerCache(state, mergedFollower),
      syncedBaseSchemaCovered,
    )
      .with("followerGsiPrimaryOrderVersion", state.leaderGsiPrimaryOrderVersion)
      .with("followerGsiAltOrderVersion", state.leaderGsiAltOrderVersion)
  }

  /// Uninitialized shard base state: no authority until assigned.
  pure def uninitializedShardBaseState = {
    dbPresent: Set(),
    gsiPresent: Set(),
    gsiAltPresent: Set(),
    leaderPayloadKeys: Set(),
    leaderNegativeKeys: Set(),
    leaderKnownManifestKeys: Set(),
    leaderCoveredSlots: Set(),
    leaderKnownGsiManifestKeys: Set(),
    leaderCoveredGsiSlots: Set(),
    leaderKnownGsiAltManifestKeys: Set(),
    leaderCoveredGsiAltSlots: Set(),
    followerPayloadKeys: Set(),
    followerNegativeKeys: Set(),
    followerKnownManifestKeys: Set(),
    followerCoveredSlots: Set(),
    followerKnownGsiManifestKeys: Set(),
    followerCoveredGsiSlots: Set(),
    followerKnownGsiAltManifestKeys: Set(),
    followerCoveredGsiAltSlots: Set(),
    preparedPuts: Set(),
    preparedDeletes: Set(),
    servingFromFollower: false,
    actualLeaderNode: NODE_LEADER,
    actualEpoch: 0,
    cacheEpoch: 0,
    cachedWritesOnly: false,
    itemAuthority: false,
    queryAuthority: false,
    gsiQueryAuthority: false,
    manifestRebuilding: false,
    continuityBroken: false,
    actualBaseSchemaVersion: 0,
    actualGsiSchemaVersion: 0,
    actualGsiPrimaryOrderVersion: 0,
    actualGsiAltOrderVersion: 0,
    leaderCurrentBaseSchemaCoveredSlots: Set(),
    followerCurrentBaseSchemaCoveredSlots: Set(),
    leaderCurrentGsiSchemaCoveredSlots: Set(),
    followerCurrentGsiSchemaCoveredSlots: Set(),
    leaderCurrentGsiAltSchemaCoveredSlots: Set(),
    followerCurrentGsiAltSchemaCoveredSlots: Set(),
    leaderGsiPrimaryOrderVersion: 0,
    followerGsiPrimaryOrderVersion: 0,
    leaderGsiAltOrderVersion: 0,
    followerGsiAltOrderVersion: 0,
    shardBusted: false,
    shardAssigned: false,
  }

  /// Assign the shard: grants all authority.
  pure def assignShard(state) =
    state
      .with("shardAssigned", true)
      .with("cachedWritesOnly", true)
      .with("itemAuthority", true)
      .with("queryAuthority", true)
      .with("gsiQueryAuthority", true)

  /// Drain shard: controlled handoff — strip authority but keep cache data.
  pure def drainShard(state) =
    state
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  /// Drop a single follower replication message for a committed slot.
  /// This models the follower missing a replication update: the follower
  /// still has the old state for that slot.
  pure def dropFollowerReplication(state, slot) = {
    pure val followerItems = followerCache(state)
    // Remove the slot from follower's cache so it diverges from leader
    pure val diverged = followerItems
      .with("payloadKeys", followerItems.payloadKeys.exclude(Set(slot)))
      .with("negativeKeys", followerItems.negativeKeys.exclude(Set(slot)))
      .with("knownManifestKeys", followerItems.knownManifestKeys.exclude(Set(slot)))
      .with("coveredSlots", followerItems.coveredSlots.exclude(Set(slot)))
    withFollowerCurrentBaseSchemaCoveredSlots(
      withFollowerCache(state, diverged),
      state.followerCurrentBaseSchemaCoveredSlots.exclude(Set(slot)),
    )
  }

  pure def bumpSchemaVersion(version) =
    if (version == 0)
      1
    else if (version == 1)
      2
    else
      0

  pure def preparePut(state, slot) =
    state
      .with("preparedPuts", state.preparedPuts.union(Set(slot)))
      .with("preparedDeletes", state.preparedDeletes.exclude(Set(slot)))

  pure def prepareDelete(state, slot) =
    state
      .with("preparedDeletes", state.preparedDeletes.union(Set(slot)))
      .with("preparedPuts", state.preparedPuts.exclude(Set(slot)))

  pure def abortPrepared(state, slot) =
    state
      .with("preparedPuts", state.preparedPuts.exclude(Set(slot)))
      .with("preparedDeletes", state.preparedDeletes.exclude(Set(slot)))

  pure def abortStillMatchesSource(state, slot) =
    if (state.preparedPuts.contains(slot))
      not(state.dbPresent.contains(slot))
    else if (state.preparedDeletes.contains(slot))
      state.dbPresent.contains(slot)
    else
      false

  pure def leaderCommitPut(state, slot) = {
    pure val dbCommitted =
      state.with("dbPresent", state.dbPresent.union(Set(slot)))
    withLeaderCache(dbCommitted, visiblePut(leaderCache(dbCommitted), slot))
  }

  pure def followerAcknowledgePut(state, slot) =
    withFollowerCache(
      state.with("preparedPuts", state.preparedPuts.exclude(Set(slot))),
      visiblePut(followerCache(state), slot),
    )

  pure def leaderCommitDelete(state, slot) = {
    pure val dbCommitted =
      state
        .with("dbPresent", state.dbPresent.exclude(Set(slot)))
        .with("gsiPresent", state.gsiPresent.exclude(Set(slot)))
        .with("gsiAltPresent", state.gsiAltPresent.exclude(Set(slot)))
    withLeaderGsiCache(
      withLeaderGsiAltCache(
        withLeaderCache(dbCommitted, visibleDelete(leaderCache(dbCommitted), slot)),
        visibleGsiDelete(leaderGsiAltCache(dbCommitted), slot),
      ),
      visibleGsiDelete(leaderGsiCache(dbCommitted), slot),
    )
  }

  pure def followerAcknowledgeDelete(state, slot) =
    withFollowerGsiCache(
      withFollowerGsiAltCache(
        withFollowerCache(
          state.with("preparedDeletes", state.preparedDeletes.exclude(Set(slot))),
          visibleDelete(followerCache(state), slot),
        ),
        visibleGsiDelete(followerGsiAltCache(state), slot),
      ),
      visibleGsiDelete(followerGsiCache(state), slot),
    )

  pure def leaderAddGsiMembership(state, slot) =
    withLeaderGsiCache(
      state.with("gsiPresent", state.gsiPresent.union(Set(slot))),
      visibleGsiInsert(leaderGsiCache(state), slot),
    )

  pure def leaderRemoveGsiMembership(state, slot) =
    withLeaderGsiCache(
      state.with("gsiPresent", state.gsiPresent.exclude(Set(slot))),
      visibleGsiDelete(leaderGsiCache(state), slot),
    )

  pure def leaderMoveGsiMembershipToAlt(state, slot) =
    withLeaderGsiAltCache(
      withLeaderGsiCache(
        state
          .with("gsiPresent", state.gsiPresent.exclude(Set(slot)))
          .with("gsiAltPresent", state.gsiAltPresent.union(Set(slot))),
        visibleGsiDelete(leaderGsiCache(state), slot),
      ),
      visibleGsiInsert(leaderGsiAltCache(state), slot),
    )

  pure def leaderMoveGsiMembershipToPrimary(state, slot) =
    withLeaderGsiCache(
      withLeaderGsiAltCache(
        state
          .with("gsiPresent", state.gsiPresent.union(Set(slot)))
          .with("gsiAltPresent", state.gsiAltPresent.exclude(Set(slot))),
        visibleGsiDelete(leaderGsiAltCache(state), slot),
      ),
      visibleGsiInsert(leaderGsiCache(state), slot),
    )

  pure def bumpOrderVersion(version) =
    if (version == 0) 1 else 0

  pure def leaderRewriteGsiPrimarySortOrder(state) = {
    pure val nextVersion = bumpOrderVersion(state.actualGsiPrimaryOrderVersion)
    state
      .with("actualGsiPrimaryOrderVersion", nextVersion)
      .with("leaderGsiPrimaryOrderVersion", nextVersion)
  }

  pure def leaderRewriteGsiAltSortOrder(state) = {
    pure val nextVersion = bumpOrderVersion(state.actualGsiAltOrderVersion)
    state
      .with("actualGsiAltOrderVersion", nextVersion)
      .with("leaderGsiAltOrderVersion", nextVersion)
  }

  pure def loseLeader(state) =
    state.with("actualLeaderNode", otherNode(localServingNode(state)))

  pure def regainLeader(state) =
    state.with("actualLeaderNode", localServingNode(state))

  pure def loseEpochAuthority(state) =
    state
      .with("actualEpoch", staleRequestEpoch(state))
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure def gainEpochAuthority(state) =
    state
      .with("cacheEpoch", state.actualEpoch)
      .with("itemAuthority", true)
      .with("queryAuthority", true)
      .with("gsiQueryAuthority", true)

  pure def promoteFollowerCatchingUp(state) =
    state
      .with("servingFromFollower", true)
      .with("actualLeaderNode", NODE_FOLLOWER)
      .with("cacheEpoch", state.actualEpoch)
      .with("itemAuthority", true)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure def recoverPreparedOnFollower(state) = {
    pure val committedPuts = state.preparedPuts.intersect(state.dbPresent)
    pure val committedDeletes = state.preparedDeletes.exclude(state.dbPresent)
    pure val recoveredFollower =
      applyOutcomes(followerCache(state), committedPuts, committedDeletes)
    pure val recoveredFollowerGsi =
      if (committedDeletes.contains(3))
        visibleGsiDelete(
          if (committedDeletes.contains(2))
            visibleGsiDelete(
              if (committedDeletes.contains(1))
                visibleGsiDelete(
                  if (committedDeletes.contains(0))
                    visibleGsiDelete(followerGsiCache(state), 0)
                  else
                    followerGsiCache(state),
                  1,
                )
              else
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiCache(state), 0)
                else
                  followerGsiCache(state),
              2,
            )
          else
            if (committedDeletes.contains(1))
              visibleGsiDelete(
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiCache(state), 0)
                else
                  followerGsiCache(state),
                1,
              )
            else
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiCache(state), 0)
              else
                followerGsiCache(state),
          3,
        )
      else
        if (committedDeletes.contains(2))
          visibleGsiDelete(
            if (committedDeletes.contains(1))
              visibleGsiDelete(
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiCache(state), 0)
                else
                  followerGsiCache(state),
                1,
              )
            else
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiCache(state), 0)
              else
                followerGsiCache(state),
            2,
          )
        else
          if (committedDeletes.contains(1))
            visibleGsiDelete(
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiCache(state), 0)
              else
                followerGsiCache(state),
              1,
            )
          else
            if (committedDeletes.contains(0))
              visibleGsiDelete(followerGsiCache(state), 0)
            else
              followerGsiCache(state)
    pure val recoveredFollowerGsiAlt =
      if (committedDeletes.contains(3))
        visibleGsiDelete(
          if (committedDeletes.contains(2))
            visibleGsiDelete(
              if (committedDeletes.contains(1))
                visibleGsiDelete(
                  if (committedDeletes.contains(0))
                    visibleGsiDelete(followerGsiAltCache(state), 0)
                  else
                    followerGsiAltCache(state),
                  1,
                )
              else
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiAltCache(state), 0)
                else
                  followerGsiAltCache(state),
              2,
            )
          else
            if (committedDeletes.contains(1))
              visibleGsiDelete(
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiAltCache(state), 0)
                else
                  followerGsiAltCache(state),
                1,
              )
            else
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiAltCache(state), 0)
              else
                followerGsiAltCache(state),
          3,
        )
      else
        if (committedDeletes.contains(2))
          visibleGsiDelete(
            if (committedDeletes.contains(1))
              visibleGsiDelete(
                if (committedDeletes.contains(0))
                  visibleGsiDelete(followerGsiAltCache(state), 0)
                else
                  followerGsiAltCache(state),
                1,
              )
            else
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiAltCache(state), 0)
              else
                followerGsiAltCache(state),
            2,
          )
        else
          if (committedDeletes.contains(1))
            visibleGsiDelete(
              if (committedDeletes.contains(0))
                visibleGsiDelete(followerGsiAltCache(state), 0)
              else
                followerGsiAltCache(state),
              1,
            )
          else
            if (committedDeletes.contains(0))
              visibleGsiDelete(followerGsiAltCache(state), 0)
            else
              followerGsiAltCache(state)
    withFollowerGsiCache(
      withFollowerGsiAltCache(
        withFollowerCache(
          state
            .with("preparedPuts", Set())
            .with("preparedDeletes", Set()),
          recoveredFollower
        ),
        recoveredFollowerGsiAlt
      ),
      recoveredFollowerGsi
    )
  }

  pure def finishCatchUp(state) =
    state
      .with("queryAuthority", true)
      .with("gsiQueryAuthority", true)

  pure def advanceActualBaseSchemaVersion(state) =
    state
      .with("actualBaseSchemaVersion", bumpSchemaVersion(state.actualBaseSchemaVersion))
      .with("leaderCurrentBaseSchemaCoveredSlots", Set())
      .with("followerCurrentBaseSchemaCoveredSlots", Set())

  pure def advanceActualGsiSchemaVersion(state) =
    state
      .with("actualGsiSchemaVersion", bumpSchemaVersion(state.actualGsiSchemaVersion))
      .with("leaderCurrentGsiSchemaCoveredSlots", Set())
      .with("followerCurrentGsiSchemaCoveredSlots", Set())
      .with("leaderCurrentGsiAltSchemaCoveredSlots", Set())
      .with("followerCurrentGsiAltSchemaCoveredSlots", Set())

  pure def bustShard(state) =
    state
      .with("shardBusted", true)
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure def authoritativeLeaderBaseState = {
    dbPresent: Set(),
    gsiPresent: Set(),
    gsiAltPresent: Set(),

    leaderPayloadKeys: Set(),
    leaderNegativeKeys: Set(),
    leaderKnownManifestKeys: Set(),
    leaderCoveredSlots: Set(),
    leaderKnownGsiManifestKeys: Set(),
    leaderCoveredGsiSlots: Set(),
    leaderKnownGsiAltManifestKeys: Set(),
    leaderCoveredGsiAltSlots: Set(),

    followerPayloadKeys: Set(),
    followerNegativeKeys: Set(),
    followerKnownManifestKeys: Set(),
    followerCoveredSlots: Set(),
    followerKnownGsiManifestKeys: Set(),
    followerCoveredGsiSlots: Set(),
    followerKnownGsiAltManifestKeys: Set(),
    followerCoveredGsiAltSlots: Set(),

    preparedPuts: Set(),
    preparedDeletes: Set(),

    servingFromFollower: false,
    actualLeaderNode: NODE_LEADER,
    actualEpoch: 0,
    cacheEpoch: 0,
    cachedWritesOnly: true,
    itemAuthority: true,
    queryAuthority: true,
    gsiQueryAuthority: true,
    manifestRebuilding: false,
    continuityBroken: false,
    actualBaseSchemaVersion: 0,
    actualGsiSchemaVersion: 0,
    actualGsiPrimaryOrderVersion: 0,
    actualGsiAltOrderVersion: 0,
    leaderCurrentBaseSchemaCoveredSlots: Set(),
    followerCurrentBaseSchemaCoveredSlots: Set(),
    leaderCurrentGsiSchemaCoveredSlots: Set(),
    followerCurrentGsiSchemaCoveredSlots: Set(),
    leaderCurrentGsiAltSchemaCoveredSlots: Set(),
    followerCurrentGsiAltSchemaCoveredSlots: Set(),
    leaderGsiPrimaryOrderVersion: 0,
    followerGsiPrimaryOrderVersion: 0,
    leaderGsiAltOrderVersion: 0,
    followerGsiAltOrderVersion: 0,
    shardBusted: false,
    shardAssigned: true,
  }

  action protocolInit =
    cache' = authoritativeLeaderBaseState

  action preparePutAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.cachedWritesOnly,
      not(cache.shardBusted),
      not(unresolvedIntents(cache).contains(slot)),
      cache' = preparePut(cache, slot),
    }
  }

  action prepareDeleteAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.cachedWritesOnly,
      not(cache.shardBusted),
      not(unresolvedIntents(cache).contains(slot)),
      cache' = prepareDelete(cache, slot),
    }
  }

  action abortPreparedAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      unresolvedIntents(cache).contains(slot),
      abortStillMatchesSource(cache, slot),
      cache' = abortPrepared(cache, slot),
    }
  }

  action leaderCommitPutAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.preparedPuts.contains(slot),
      cache' = leaderCommitPut(cache, slot),
    }
  }

  action followerAcknowledgePutAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.preparedPuts.contains(slot),
      cache.dbPresent.contains(slot),
      cache' = followerAcknowledgePut(cache, slot),
    }
  }

  action leaderCommitDeleteAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.preparedDeletes.contains(slot),
      cache' = leaderCommitDelete(cache, slot),
    }
  }

  action followerAcknowledgeDeleteAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.preparedDeletes.contains(slot),
      not(cache.dbPresent.contains(slot)),
      cache' = followerAcknowledgeDelete(cache, slot),
    }
  }

  action queryFillLeaderAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeader(cache, lower, upper),
    }
  }

  action queryFillLeaderGsiAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeaderGsi(cache, lower, upper),
    }
  }

  action queryFillLeaderGsiAltAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeaderGsiAlt(cache, lower, upper),
    }
  }

  action queryFillFollowerAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollower(cache, lower, upper),
    }
  }

  action queryFillFollowerGsiAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollowerGsi(cache, lower, upper),
    }
  }

  action queryFillFollowerGsiAltAction = {
    nondet lower = PROTOCOL_ACTION_LOWER_BOUNDS.oneOf()
    nondet upper = PROTOCOL_ACTION_UPPER_BOUNDS.oneOf()
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollowerGsiAlt(cache, lower, upper),
    }
  }

  action leaderAddGsiMembershipAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.dbPresent.contains(slot),
      cache' = leaderAddGsiMembership(cache, slot),
    }
  }

  action leaderRemoveGsiMembershipAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiPresent.contains(slot),
      cache' = leaderRemoveGsiMembership(cache, slot),
    }
  }

  action leaderMoveGsiMembershipToAltAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiPresent.contains(slot),
      not(cache.gsiAltPresent.contains(slot)),
      cache' = leaderMoveGsiMembershipToAlt(cache, slot),
    }
  }

  action leaderMoveGsiMembershipToPrimaryAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiAltPresent.contains(slot),
      not(cache.gsiPresent.contains(slot)),
      cache' = leaderMoveGsiMembershipToPrimary(cache, slot),
    }
  }

  action leaderRewriteGsiPrimarySortOrderAction =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache' = leaderRewriteGsiPrimarySortOrder(cache),
    }

  action leaderRewriteGsiAltSortOrderAction =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache' = leaderRewriteGsiAltSortOrder(cache),
    }

  action syncFollowerAction =
    cache' = syncFollowerFromLeader(cache)

  action preparePutAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.cachedWritesOnly,
      not(cache.shardBusted),
      not(unresolvedIntents(cache).contains(slot)),
      cache' = preparePut(cache, slot),
    }

  action prepareDeleteAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.cachedWritesOnly,
      not(cache.shardBusted),
      not(unresolvedIntents(cache).contains(slot)),
      cache' = prepareDelete(cache, slot),
    }

  action leaderCommitPutAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.preparedPuts.contains(slot),
      cache' = leaderCommitPut(cache, slot),
    }

  action followerAcknowledgePutAt(slot) =
    all {
      cache.preparedPuts.contains(slot),
      cache.dbPresent.contains(slot),
      cache' = followerAcknowledgePut(cache, slot),
    }

  action leaderCommitDeleteAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.preparedDeletes.contains(slot),
      cache' = leaderCommitDelete(cache, slot),
    }

  action followerAcknowledgeDeleteAt(slot) =
    all {
      cache.preparedDeletes.contains(slot),
      not(cache.dbPresent.contains(slot)),
      cache' = followerAcknowledgeDelete(cache, slot),
    }

  action queryFillLeaderRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeader(cache, lower, upper),
    }

  action queryFillLeaderGsiRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeaderGsi(cache, lower, upper),
    }

  action queryFillLeaderGsiAltRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.itemAuthority,
      cache' = queryFillLeaderGsiAlt(cache, lower, upper),
    }

  action queryFillFollowerRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollower(cache, lower, upper),
    }

  action queryFillFollowerGsiRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollowerGsi(cache, lower, upper),
    }

  action queryFillFollowerGsiAltRange(lower, upper) =
    all {
      lower <= upper,
      cache.actualLeaderNode == NODE_FOLLOWER,
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = queryFillFollowerGsiAlt(cache, lower, upper),
    }

  action leaderAddGsiMembershipAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.dbPresent.contains(slot),
      cache' = leaderAddGsiMembership(cache, slot),
    }

  action leaderRemoveGsiMembershipAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiPresent.contains(slot),
      cache' = leaderRemoveGsiMembership(cache, slot),
    }

  action leaderMoveGsiMembershipToAltAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiPresent.contains(slot),
      not(cache.gsiAltPresent.contains(slot)),
      cache' = leaderMoveGsiMembershipToAlt(cache, slot),
    }

  action leaderMoveGsiMembershipToPrimaryAt(slot) =
    all {
      cache.actualLeaderNode == NODE_LEADER,
      not(cache.servingFromFollower),
      cache.gsiAltPresent.contains(slot),
      not(cache.gsiPresent.contains(slot)),
      cache' = leaderMoveGsiMembershipToPrimary(cache, slot),
    }

  action promoteFollowerNow =
    all {
      cache.servingFromFollower == false,
      not(cache.shardBusted),
      cache' = promoteFollowerCatchingUp(cache),
    }

  action recoverPreparedNow =
    all {
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = recoverPreparedOnFollower(cache),
    }

  action finishCatchUpNow =
    all {
      cache.servingFromFollower,
      cache.itemAuthority,
      not(cache.shardBusted),
      cache' = finishCatchUp(cache),
    }

  action loseLeaderAction =
    cache' = loseLeader(cache)

  action regainLeaderAction =
    cache' = regainLeader(cache)

  action loseEpochAuthorityAction =
    cache' = loseEpochAuthority(cache)

  action gainEpochAuthorityAction =
    all {
      not(cache.shardBusted),
      cache' = gainEpochAuthority(cache),
    }

  action promoteFollowerAction =
    all {
      cache.servingFromFollower == false,
      not(cache.shardBusted),
      cache' = promoteFollowerCatchingUp(cache),
    }

  action recoverPreparedAction =
    all {
      cache.servingFromFollower,
      cache.itemAuthority,
      cache' = recoverPreparedOnFollower(cache),
    }

  action finishCatchUpAction =
    all {
      cache.servingFromFollower,
      cache.itemAuthority,
      not(cache.shardBusted),
      cache' = finishCatchUp(cache),
    }

  action advanceActualBaseSchemaVersionAction =
    cache' = advanceActualBaseSchemaVersion(cache)

  action advanceActualGsiSchemaVersionAction =
    cache' = advanceActualGsiSchemaVersion(cache)

  action evictLeaderPayloadAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    cache' = withLeaderCache(cache, evictPayload(leaderCache(cache), slot))
  }

  action evictLeaderPayloadSharedDomainAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    cache' =
      withLeaderCurrentBaseSchemaCoveredSlots(
        withLeaderCache(
          cache,
          evictPayloadWithSharedProofDomain(leaderCache(cache), slot),
        ),
        cache.leaderCurrentBaseSchemaCoveredSlots.exclude(Set(slot)),
      )
  }

  action evictLeaderManifestAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    cache' =
      withLeaderCurrentBaseSchemaCoveredSlots(
        withLeaderCache(cache, evictManifestEntry(leaderCache(cache), slot)),
        cache.leaderCurrentBaseSchemaCoveredSlots.exclude(Set(slot)),
      )
  }

  action bustShardAction =
    cache' = bustShard(cache)

  action assignShardAction =
    all {
      not(cache.shardAssigned),
      not(cache.shardBusted),
      cache' = assignShard(cache),
    }

  action drainShardAction =
    all {
      cache.shardAssigned,
      not(cache.shardBusted),
      cache' = drainShard(cache),
    }

  action partialSyncFollowerAction = {
    nondet syncedSlots = SLOT_SET.powerset().oneOf()
    all {
      syncedSlots.size() > 0,
      syncedSlots != SLOT_SET,
      cache' = partialSyncFollower(cache, syncedSlots),
    }
  }

  action dropFollowerReplicationAction = {
    nondet slot = PROTOCOL_ACTION_SLOTS.oneOf()
    all {
      cache.followerPayloadKeys.contains(slot) or cache.followerNegativeKeys.contains(slot),
      cache' = dropFollowerReplication(cache, slot),
    }
  }

  action uninitializedProtocolInit =
    cache' = uninitializedShardBaseState

  action protocolStep =
    any {
      preparePutAction,
      prepareDeleteAction,
      abortPreparedAction,
      leaderCommitPutAction,
      followerAcknowledgePutAction,
      leaderCommitDeleteAction,
      followerAcknowledgeDeleteAction,
      queryFillLeaderAction,
      queryFillLeaderGsiAction,
      queryFillLeaderGsiAltAction,
      queryFillFollowerAction,
      queryFillFollowerGsiAction,
      queryFillFollowerGsiAltAction,
      leaderAddGsiMembershipAction,
      leaderRemoveGsiMembershipAction,
      leaderMoveGsiMembershipToAltAction,
      leaderMoveGsiMembershipToPrimaryAction,
      leaderRewriteGsiPrimarySortOrderAction,
      leaderRewriteGsiAltSortOrderAction,
      syncFollowerAction,
      partialSyncFollowerAction,
      loseLeaderAction,
      regainLeaderAction,
      loseEpochAuthorityAction,
      gainEpochAuthorityAction,
      promoteFollowerAction,
      recoverPreparedAction,
      finishCatchUpAction,
      advanceActualBaseSchemaVersionAction,
      advanceActualGsiSchemaVersionAction,
      evictLeaderPayloadAction,
      evictLeaderPayloadSharedDomainAction,
      evictLeaderManifestAction,
      bustShardAction,
      assignShardAction,
      drainShardAction,
      dropFollowerReplicationAction,
    }

  pure val firstForwardPage = makePartitionQuery(0, 1, -1, 1, 1024, false, false, false, GSI_PRIMARY, 0)
  pure val firstForwardGsiPage = makePartitionQuery(0, 1, -1, 1, 1024, false, false, true, GSI_PRIMARY, 0)
  pure val firstForwardAltGsiPage = makePartitionQuery(0, 1, -1, 1, 1024, false, false, true, GSI_ALT, 0)
  pure val widerForwardPage = makePartitionQuery(0, 1, -1, 2, 1024, false, false, false, GSI_PRIMARY, 0)
  pure val widerForwardGsiPage = makePartitionQuery(0, 1, -1, 2, 1024, false, false, true, GSI_PRIMARY, 0)
  pure val widerForwardAltGsiPage = makePartitionQuery(0, 1, -1, 2, 1024, false, false, true, GSI_ALT, 0)
  pure val secondPartitionForwardPage = makePartitionQuery(2, 3, 1, 2, 1024, false, false, false, GSI_PRIMARY, 1)
  pure val projectionByteBoundaryBasePage = makePartitionQuery(0, 1, -1, 2, 384, false, false, false, GSI_PRIMARY, 0)
  pure val projectionByteBoundaryGsiPage = makePartitionQuery(0, 1, -1, 2, 256, false, false, true, GSI_PRIMARY, 0)

  val validProtocolState = validState(cache)

  val protocolStrongReadsUseItemAuthority =
    SLOT_SET.forall(slot =>
      canServeStrongGet(cache, slot) == canServeEventualGet(cache, slot)
    )

  val protocolPreparedIntentGetsBypass =
    SLOT_SET.forall(slot =>
      not(slotHasUnresolvedIntent(cache, slot))
        or not(canServeEventualGet(cache, slot))
    )

  val protocolEventualGetsAreSound =
    SLOT_SET.forall(slot =>
      not(canServeEventualGet(cache, slot))
        or eventualGetMatchesSource(cache, slot)
    )

  val protocolStrongBatchGetsUseAllSafeKeys =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetPlan(cache, true, requestedKeys).servedKeys
        == requestedKeys.filter(slot => canServeStrongGet(cache, slot))
    )

  val protocolEventualBatchGetsAreSound =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetPlan(cache, false, requestedKeys).servedKeys.forall(slot =>
        eventualGetMatchesSource(cache, slot)
      )
    )

  val protocolStrongGsiQueriesRemainInvalid =
    ALL_QUERIES.forall(query =>
      requestRoute(cache, freshRequestEpoch(cache)) != ROUTE_OK
        or not(query.usesGsi)
        or queryDecision(cache, query, true, freshRequestEpoch(cache)).outcome
          == RESPONSE_INVALID_GSI_STRONG
    )

  val protocolBaseQueriesOnlyUseCurrentSchemaCoverage =
    BASE_QUERIES.forall(query =>
      cachePlan(cache, query).cacheEvaluatedKeys
        .select(slot => not(servingCurrentBaseSchemaCoveredSlots(cache).contains(slot)))
        .length() == 0
    )

  val protocolGsiQueriesOnlyUseCurrentSchemaCoverage =
    GSI_QUERIES.forall(query =>
      cachePlan(cache, query).cacheEvaluatedKeys
        .select(slot => not(servingCurrentGsiSchemaCoveredSlots(cache, query).contains(slot)))
        .length() == 0
    )

  val protocolGsiQueriesOnlyServeWithCurrentManifestOrder =
    GSI_QUERIES.forall(query =>
      not(cachePlan(cache, query).serveWholePage)
        or manifestOrderCurrent(cache, query)
    )

  val protocolStaleEpochReadsNeverServeCache =
    and {
      SLOT_SET.forall(slot =>
        and {
          eventualGetDecision(cache, slot, staleRequestEpoch(cache)) == RESPONSE_STALE_EPOCH,
          strongGetDecision(cache, slot, staleRequestEpoch(cache)) == RESPONSE_STALE_EPOCH,
        }
      ),
      SLOT_SET.powerset().forall(requestedKeys =>
        and {
          batchGetDecision(cache, false, requestedKeys, staleRequestEpoch(cache)).outcome
            == RESPONSE_STALE_EPOCH,
          batchGetDecision(cache, false, requestedKeys, staleRequestEpoch(cache)).servedKeys
            == Set(),
        }
      ),
      ALL_QUERIES.forall(query =>
        and {
          queryDecision(cache, query, false, staleRequestEpoch(cache)).outcome
            == RESPONSE_STALE_EPOCH,
          not(queryDecision(cache, query, false, staleRequestEpoch(cache)).serveWholePage),
        }
      ),
    }

  val protocolWrongLeaderReadsRedirect =
    and {
      SLOT_SET.forall(slot =>
        localServingNode(cache) == cache.actualLeaderNode or and {
          eventualGetDecision(cache, slot, freshRequestEpoch(cache)) == RESPONSE_WRONG_LEADER,
          strongGetDecision(cache, slot, freshRequestEpoch(cache)) == RESPONSE_WRONG_LEADER,
        }
      ),
      SLOT_SET.powerset().forall(requestedKeys =>
        localServingNode(cache) == cache.actualLeaderNode or and {
          batchGetDecision(cache, false, requestedKeys, freshRequestEpoch(cache)).outcome
            == RESPONSE_WRONG_LEADER,
          batchGetDecision(cache, false, requestedKeys, freshRequestEpoch(cache)).servedKeys
            == Set(),
        }
      ),
      ALL_QUERIES.forall(query =>
        localServingNode(cache) == cache.actualLeaderNode or and {
          queryDecision(cache, query, false, freshRequestEpoch(cache)).outcome
            == RESPONSE_WRONG_LEADER,
          not(queryDecision(cache, query, false, freshRequestEpoch(cache)).serveWholePage),
        }
      ),
    }

  val protocolQueriesAvoidPreparedWrites =
    ALL_QUERIES.forall(query =>
      not(cachePlan(cache, query).serveWholePage)
        or not(queryTouchesUnresolvedIntent(cache, query))
    )

  val protocolFullPageCacheReadsMatchSource =
    ALL_QUERIES.forall(query =>
      not(cachePlan(cache, query).serveWholePage)
        or cacheReturnedPage(cache, query)
          == sourceReturnedPage(cache, query)
    )

  def eventualGetServed(slot) =
    canServeEventualGet(cache, slot)

  def wholePageServed(query) =
    cachePlan(cache, query).serveWholePage

  def batchGetServedKeys(strong, requestedKeys) =
    batchGetPlan(cache, strong, requestedKeys).servedKeys

  def abortPermitted(slot) =
    abortStillMatchesSource(cache, slot)

  run acknowledgedPutMakesEventualGetSafe =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(eventualGetServed(1))

  run strongGetSourceAgreement =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(strongGetDecision(cache, 1, freshRequestEpoch(cache)) == RESPONSE_SERVE_CACHE)
      .expect(eventualGetMatchesSource(cache, 1))

  run strongBatchSourceAgreement =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(batchGetDecision(cache, true, Set(0, 1), freshRequestEpoch(cache)).servedKeys == Set(1))

  run staleEpochRequestsReturnStaleEpoch =
    protocolInit
      .expect(eventualGetDecision(cache, 0, staleRequestEpoch(cache)) == RESPONSE_STALE_EPOCH)
      .expect(batchGetServedKeys(false, Set(0, 1)) == Set())
      .expect(queryDecision(cache, firstForwardPage, false, staleRequestEpoch(cache)).outcome == RESPONSE_STALE_EPOCH)

  run wrongLeaderRequestsRedirect =
    protocolInit
      .then(loseLeaderAction)
      .expect(eventualGetDecision(cache, 0, freshRequestEpoch(cache)) == RESPONSE_WRONG_LEADER)
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_WRONG_LEADER)

  run epochRefreshRestoresCacheServing =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(eventualGetServed(1))
      .then(loseEpochAuthorityAction)
      .expect(not(eventualGetServed(1)))
      .expect(eventualGetDecision(cache, 1, freshRequestEpoch(cache)) == RESPONSE_FALLBACK_DB)
      .expect(eventualGetDecision(cache, 1, staleRequestEpoch(cache)) == RESPONSE_STALE_EPOCH)
      .then(gainEpochAuthorityAction)
      .expect(eventualGetServed(1))

  run strongGsiQueryIsRejected =
    protocolInit
      .expect(queryDecision(cache, firstForwardGsiPage, true, freshRequestEpoch(cache)).outcome == RESPONSE_INVALID_GSI_STRONG)

  run gsiSchemaVersionChangeBlocksOnlyGsiQueries =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderRange(0, 1))
      .then(queryFillLeaderGsiRange(0, 1))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .then(advanceActualGsiSchemaVersionAction)
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)
      .then(queryFillLeaderGsiRange(0, 1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)

  run baseSchemaVersionChangeBlocksQueriesButNotPointReads =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(queryFillLeaderRange(0, 1))
      .expect(eventualGetServed(1))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .then(advanceActualBaseSchemaVersionAction)
      .expect(eventualGetServed(1))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)
      .then(queryFillLeaderRange(0, 1))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)

  run sharedDomainPayloadEvictionDropsAuthorityForOverlappingBaseQuery =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(queryFillLeaderRange(0, 1))
      .expect(queryDecision(cache, widerForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .then(evictLeaderPayloadSharedDomainAction)
      .expect(queryDecision(cache, widerForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)

  run followerRangeRevalidationRestoresPromotedQueries =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(queryFillLeaderRange(0, 1))
      .then(syncFollowerAction)
      .then(promoteFollowerNow)
      .then(finishCatchUpNow)
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .then(advanceActualBaseSchemaVersionAction)
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)
      .then(queryFillFollowerRange(0, 1))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)

  run partialLeaderRevalidationRecoversShortestPageFirst =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(followerAcknowledgePutAt(2))
      .then(queryFillLeaderRange(0, 2))
      .then(advanceActualBaseSchemaVersionAction)
      .expect(queryDecision(cache, widerForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)
      .then(queryFillLeaderRange(0, 1))
      .expect(queryDecision(cache, makePartitionQuery(0, 1, -1, 2, 384, false, false, false, GSI_PRIMARY, 0), false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, makePartitionQuery(0, 1, -1, 3, 1024, false, false, false, GSI_PRIMARY, 0), false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)

  run queryCoverageDoesNotLeakAcrossPartitions =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(followerAcknowledgePutAt(2))
      .then(queryFillLeaderRange(0, 1))
      .expect(queryDecision(cache, widerForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, secondPartitionForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_FALLBACK_DB)

  run inPlaceGsiSortRewritePreservesLeaderServability =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(0))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderGsiRange(0, 1))
      .expect(queryDecision(cache, widerForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, widerForwardGsiPage).cacheEvaluatedKeys == List(0, 1))
      .then(leaderRewriteGsiPrimarySortOrderAction)
      .expect(queryDecision(cache, widerForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, widerForwardGsiPage).cacheEvaluatedKeys == List(1, 0))

  run gsiAuthorityCannotReturnWithoutShardAuthority =
    protocolInit
      .then(loseEpochAuthorityAction)
      .expect(not(cache.itemAuthority))
      .expect(not(cache.gsiQueryAuthority))

  run committedLeaderPutCannotBeAborted =
    protocolInit
      .then(preparePutAt(1))
      .expect(abortPermitted(1))
      .then(leaderCommitPutAt(1))
      .expect(not(abortPermitted(1)))

  run gsiSparseMembershipUsesSeparateManifest =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderRange(0, 1))
      .then(queryFillLeaderGsiRange(0, 1))
      .expect(queryDecision(cache, widerForwardPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, widerForwardPage).cacheEvaluatedKeys == List(0, 1))
      .expect(queryDecision(cache, widerForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, widerForwardGsiPage).cacheEvaluatedKeys == List(1))

  run gsiMembershipRemovalDropsOnlyIndexResult =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderGsiRange(0, 1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardGsiPage).cacheEvaluatedKeys == List(1))
      .then(leaderRemoveGsiMembershipAt(1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardGsiPage).cacheEvaluatedKeys == List())

  run gsiMembershipRewriteMovesResultBetweenQuerySpaces =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderGsiRange(0, 1))
      .then(queryFillLeaderGsiAltRange(0, 1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, firstForwardAltGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardGsiPage).cacheEvaluatedKeys == List(1))
      .expect(cachePlan(cache, firstForwardAltGsiPage).cacheEvaluatedKeys == List())
      .then(leaderMoveGsiMembershipToAltAt(1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(queryDecision(cache, firstForwardAltGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardGsiPage).cacheEvaluatedKeys == List())
      .expect(cachePlan(cache, firstForwardAltGsiPage).cacheEvaluatedKeys == List(1))

  run baseDeleteRemovesAlternateGsiMembershipToo =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(leaderMoveGsiMembershipToAltAt(1))
      .then(queryFillLeaderGsiAltRange(0, 1))
      .then(prepareDeleteAt(1))
      .then(leaderCommitDeleteAt(1))
      .expect(not(cache.gsiPresent.contains(1)))
      .expect(not(cache.gsiAltPresent.contains(1)))
      .then(followerAcknowledgeDeleteAt(1))
      .expect(queryDecision(cache, firstForwardAltGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardAltGsiPage).cacheEvaluatedKeys == List())

  run baseDeleteRemovesGsiMembershipToo =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(leaderAddGsiMembershipAt(1))
      .then(queryFillLeaderGsiRange(0, 1))
      .then(prepareDeleteAt(1))
      .then(leaderCommitDeleteAt(1))
      .expect(not(cache.gsiPresent.contains(1)))
      .then(followerAcknowledgeDeleteAt(1))
      .expect(queryDecision(cache, firstForwardGsiPage, false, freshRequestEpoch(cache)).outcome == RESPONSE_SERVE_CACHE)
      .expect(cachePlan(cache, firstForwardGsiPage).cacheEvaluatedKeys == List())

  run gsiProjectionBytesCanServeMoreThanBase =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(followerAcknowledgePutAt(2))
      .then(preparePutAt(3))
      .then(leaderCommitPutAt(3))
      .then(followerAcknowledgePutAt(3))
      .then(leaderAddGsiMembershipAt(1))
      .then(leaderAddGsiMembershipAt(2))
      .then(leaderAddGsiMembershipAt(3))
      .then(queryFillLeaderRange(1, 3))
      .then(queryFillLeaderGsiRange(1, 3))
      .expect(cachePlan(cache, projectionByteBoundaryBasePage).cacheEvaluatedKeys == List(1, 2))
      .expect(cachePlan(cache, projectionByteBoundaryGsiPage).cacheEvaluatedKeys == List(1, 2, 3))

  run promotedFollowerPutStaysFencedUntilRecovery =
    protocolInit
      .then(queryFillLeaderRange(0, 3))
      .then(syncFollowerAction)
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(promoteFollowerNow)
      .expect(not(eventualGetServed(2)))
      .then(recoverPreparedNow)
      .expect(eventualGetServed(2))
      .expect(not(wholePageServed(firstForwardPage)))
      .then(finishCatchUpNow)
      .expect(wholePageServed(firstForwardPage))

  run promotedFollowerDeleteStaysFencedUntilRecovery =
    protocolInit
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(followerAcknowledgePutAt(2))
      .then(queryFillLeaderRange(0, 3))
      .then(syncFollowerAction)
      .then(prepareDeleteAt(2))
      .then(leaderCommitDeleteAt(2))
      .then(promoteFollowerNow)
      .expect(not(eventualGetServed(2)))
      .then(recoverPreparedNow)
      .expect(eventualGetServed(2))

  run promotedFollowerBatchGetUsesPerKeySafety =
    protocolInit
      .then(queryFillLeaderRange(0, 3))
      .then(syncFollowerAction)
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(preparePutAt(2))
      .then(leaderCommitPutAt(2))
      .then(promoteFollowerNow)
      .expect(batchGetServedKeys(false, Set(1, 2, 3)) == Set(1))
      .then(recoverPreparedNow)
      .expect(batchGetServedKeys(false, Set(1, 2, 3)) == Set(1, 2))

  // --- Pre-initialization (unassigned shard) tests ---

  run uninitializedShardBlocksAllReads =
    uninitializedProtocolInit
      .expect(not(eventualGetServed(0)))
      .expect(not(eventualGetServed(1)))
      .expect(not(wholePageServed(firstForwardPage)))
      .expect(batchGetServedKeys(false, Set(0, 1)) == Set())

  run uninitializedShardAssignmentEnablesReads =
    uninitializedProtocolInit
      .then(assignShardAction)
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(eventualGetServed(1))

  // --- Partial sync + promote tests ---

  run partiallySyncedFollowerPromotionStripsUnsyncedQueryAuthority =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(queryFillLeaderRange(0, 1))
      .then(partialSyncFollowerAction)
      .then(promoteFollowerNow)
      // After promotion with partial sync, point reads work for synced slots
      // but query authority is stripped so no full pages
      .expect(not(wholePageServed(widerForwardPage)))
      .then(finishCatchUpNow)
      // After catch-up we still can't serve whole page because follower was only
      // partially synced - coverage is incomplete for unsynced slots
      .then(queryFillFollowerRange(0, 1))
      .expect(wholePageServed(widerForwardPage))

  // --- Drain shard (controlled handoff) tests ---

  run drainedShardBlocksAllReads =
    protocolInit
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .expect(eventualGetServed(1))
      .then(drainShardAction)
      .expect(not(eventualGetServed(1)))
      .expect(not(wholePageServed(firstForwardPage)))
      .expect(batchGetServedKeys(false, Set(0, 1)) == Set())

  // --- Follower divergence tests ---

  run droppedFollowerReplicationCausesStaleAfterPromotion =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(preparePutAt(1))
      .then(leaderCommitPutAt(1))
      .then(followerAcknowledgePutAt(1))
      .then(queryFillLeaderRange(0, 1))
      .then(syncFollowerAction)
      .expect(eventualGetServed(0))
      .expect(eventualGetServed(1))
      // Drop replication for slot 1 on follower
      .then(dropFollowerReplicationAction)
      // Promote follower - it now has stale data for dropped slot
      .then(promoteFollowerNow)
      // Follower can serve point reads for slots it still has
      // but slot 1 was dropped so it cannot serve that anymore
      // (it was removed from follower cache by dropFollowerReplication)
      .expect(eventualGetServed(0))
      .expect(not(eventualGetServed(1)))

  run followerDivergenceDoesNotAffectLeaderServing =
    protocolInit
      .then(preparePutAt(0))
      .then(leaderCommitPutAt(0))
      .then(followerAcknowledgePutAt(0))
      .then(syncFollowerAction)
      .then(dropFollowerReplicationAction)
      // Leader still serves fine
      .expect(eventualGetServed(0))
      .expect(queryDecision(cache, firstForwardPage, false, freshRequestEpoch(cache)).outcome != RESPONSE_WRONG_LEADER)

  // --- Invariants for new transitions ---

  val protocolUninitializedShardsNeverServeCache =
    not(cache.shardAssigned) implies and {
      SLOT_SET.forall(slot => not(canServeEventualGet(cache, slot))),
      ALL_QUERIES.forall(query => not(cachePlan(cache, query).serveWholePage)),
      SLOT_SET.powerset().forall(requestedKeys =>
        batchGetPlan(cache, false, requestedKeys).servedKeys == Set()
      ),
    }

  val protocolDrainedShardsNeverServeCache =
    (not(cache.itemAuthority) and cache.shardAssigned and not(cache.shardBusted)) implies and {
      SLOT_SET.forall(slot => not(canServeEventualGet(cache, slot))),
      ALL_QUERIES.forall(query => not(cachePlan(cache, query).serveWholePage)),
    }
}
