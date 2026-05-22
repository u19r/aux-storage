module distributed_cache_model_tests {
  import distributed_cache_model.* from "distributed_cache_model"

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

  pure def withLeaderCache(state, cache) =
    state
      .with("leaderPayloadKeys", cache.payloadKeys)
      .with("leaderNegativeKeys", cache.negativeKeys)
      .with("leaderKnownManifestKeys", cache.knownManifestKeys)
      .with("leaderCoveredSlots", cache.coveredSlots)

  pure def withFollowerCache(state, cache) =
    state
      .with("followerPayloadKeys", cache.payloadKeys)
      .with("followerNegativeKeys", cache.negativeKeys)
      .with("followerKnownManifestKeys", cache.knownManifestKeys)
      .with("followerCoveredSlots", cache.coveredSlots)

  pure def withLeaderGsiCache(state, cache) =
    state
      .with("leaderKnownGsiManifestKeys", cache.knownManifestKeys)
      .with("leaderCoveredGsiSlots", cache.coveredSlots)

  pure def withLeaderGsiAltCache(state, cache) =
    state
      .with("leaderKnownGsiAltManifestKeys", cache.knownManifestKeys)
      .with("leaderCoveredGsiAltSlots", cache.coveredSlots)

  pure def withFollowerGsiCache(state, cache) =
    state
      .with("followerKnownGsiManifestKeys", cache.knownManifestKeys)
      .with("followerCoveredGsiSlots", cache.coveredSlots)

  pure def withFollowerGsiAltCache(state, cache) =
    state
      .with("followerKnownGsiAltManifestKeys", cache.knownManifestKeys)
      .with("followerCoveredGsiAltSlots", cache.coveredSlots)

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

  pure def queryFillCache(cache, dbPresent, lower, upper) = {
    pure val filledSlots = intervalSlots(lower, upper)
    pure val presentInFill = filledSlots.intersect(dbPresent)
    pure val emptyInFill = filledSlots.exclude(dbPresent)
    cache
      .with("coveredSlots", cache.coveredSlots.union(filledSlots))
      .with("knownManifestKeys", cache.knownManifestKeys.union(presentInFill))
      .with("payloadKeys", cache.payloadKeys.union(presentInFill))
      .with("negativeKeys", cache.negativeKeys.union(emptyInFill))
  }

  pure def queryFillIndexCache(cache, sourceKeys, lower, upper) = {
    pure val filledSlots = intervalSlots(lower, upper)
    pure val presentInFill = filledSlots.intersect(sourceKeys)
    cache
      .with("coveredSlots", cache.coveredSlots.union(filledSlots))
      .with("knownManifestKeys", cache.knownManifestKeys.union(presentInFill))
  }

  pure def visiblePut(cache, slot) =
    cache
      .with("payloadKeys", cache.payloadKeys.union(Set(slot)))
      .with("negativeKeys", cache.negativeKeys.exclude(Set(slot)))
      .with("knownManifestKeys", cache.knownManifestKeys.union(Set(slot)))

  pure def visibleDelete(cache, slot) =
    cache
      .with("payloadKeys", cache.payloadKeys.exclude(Set(slot)))
      .with("negativeKeys", cache.negativeKeys.union(Set(slot)))
      .with("knownManifestKeys", cache.knownManifestKeys.exclude(Set(slot)))

  pure def visibleGsiInsert(cache, slot) =
    cache.with("knownManifestKeys", cache.knownManifestKeys.union(Set(slot)))

  pure def visibleGsiDelete(cache, slot) =
    cache.with("knownManifestKeys", cache.knownManifestKeys.exclude(Set(slot)))

  pure def applyOutcomes(cache, committedPuts, committedDeletes) =
    cache
      .with(
        "payloadKeys",
        cache.payloadKeys.union(committedPuts).exclude(committedDeletes)
      )
      .with(
        "negativeKeys",
        cache.negativeKeys.exclude(committedPuts).union(committedDeletes)
      )
      .with(
        "knownManifestKeys",
        cache.knownManifestKeys.union(committedPuts).exclude(committedDeletes)
      )

  pure def evictPayload(cache, slot) =
    cache.with("payloadKeys", cache.payloadKeys.exclude(Set(slot)))

  pure def evictManifestEntry(cache, slot) =
    cache
      .with("knownManifestKeys", cache.knownManifestKeys.exclude(Set(slot)))
      .with(
        "coveredSlots",
        if (cache.knownManifestKeys.contains(slot))
          cache.coveredSlots.exclude(Set(slot))
        else
          cache.coveredSlots
      )

  pure def evictLeaderManifest(state, slot) =
    withLeaderCurrentBaseSchemaCoveredSlots(
      withLeaderCache(state, evictManifestEntry(leaderCache(state), slot)),
      state.leaderCurrentBaseSchemaCoveredSlots.exclude(Set(slot)),
    )

  pure def evictLeaderPayloadWithSharedProofDomain(state, slot) =
    withLeaderCurrentBaseSchemaCoveredSlots(
      withLeaderCache(
        state,
        evictManifestEntry(evictPayload(leaderCache(state), slot), slot),
      ),
      state.leaderCurrentBaseSchemaCoveredSlots.exclude(Set(slot)),
    )

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

  pure def preparePut(state, slot) =
    state
      .with("preparedPuts", state.preparedPuts.union(Set(slot)))
      .with("preparedDeletes", state.preparedDeletes.exclude(Set(slot)))

  pure def prepareDelete(state, slot) =
    state
      .with("preparedDeletes", state.preparedDeletes.union(Set(slot)))
      .with("preparedPuts", state.preparedPuts.exclude(Set(slot)))

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
    withLeaderGsiAltCache(
      withLeaderGsiCache(
        withLeaderCache(dbCommitted, visibleDelete(leaderCache(dbCommitted), slot)),
        visibleGsiDelete(leaderGsiCache(dbCommitted), slot),
      ),
      visibleGsiDelete(leaderGsiAltCache(dbCommitted), slot),
    )
  }

  pure def followerAcknowledgeDelete(state, slot) =
    withFollowerGsiAltCache(
      withFollowerGsiCache(
        withFollowerCache(
          state.with("preparedDeletes", state.preparedDeletes.exclude(Set(slot))),
          visibleDelete(followerCache(state), slot),
        ),
        visibleGsiDelete(followerGsiCache(state), slot),
      ),
      visibleGsiDelete(followerGsiAltCache(state), slot),
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

  pure def promoteFollowerCatchingUp(state) =
    state
      .with("servingFromFollower", true)
      .with("actualLeaderNode", NODE_FOLLOWER)
      .with("actualEpoch", staleRequestEpoch(state))
      .with("cacheEpoch", staleRequestEpoch(state))
      .with("itemAuthority", true)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure def finishCatchUp(state) =
    state
      .with("queryAuthority", true)
      .with("gsiQueryAuthority", true)

  pure def advanceActualBaseSchemaVersion(state, version) =
    state
      .with("actualBaseSchemaVersion", version)
      .with("leaderCurrentBaseSchemaCoveredSlots", Set())
      .with("followerCurrentBaseSchemaCoveredSlots", Set())

  pure def advanceActualGsiSchemaVersion(state, version) =
    state
      .with("actualGsiSchemaVersion", version)
      .with("leaderCurrentGsiSchemaCoveredSlots", Set())
      .with("followerCurrentGsiSchemaCoveredSlots", Set())
      .with("leaderCurrentGsiAltSchemaCoveredSlots", Set())
      .with("followerCurrentGsiAltSchemaCoveredSlots", Set())

  pure def markShardBusted(state) =
    state
      .with("shardBusted", true)
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure def recoverPreparedOnFollower(state) = {
    pure val committedPuts = state.preparedPuts.intersect(state.dbPresent)
    pure val committedDeletes = state.preparedDeletes.exclude(state.dbPresent)
    pure val recoveredFollower =
      applyOutcomes(followerCache(state), committedPuts, committedDeletes)
    withFollowerCache(
      state
        .with("preparedPuts", Set())
        .with("preparedDeletes", Set()),
      recoveredFollower,
    )
  }

  pure val authoritativeLeaderBaseState = {
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

  pure val uninitializedShardBaseState =
    authoritativeLeaderBaseState
      .with("shardAssigned", false)
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)

  pure val forwardPage = makeQuery(0, 3, -1, 2, 1024, false, false, false, GSI_PRIMARY)
  pure val reversePage = makeQuery(1, 3, 4, 2, 1024, false, true, false, GSI_PRIMARY)

  run pkHashRoutesWholePartition = {
    assert(
      and {
        slotInPkPartition(0, 0),
        slotInPkPartition(1, 0),
        not(slotInPkPartition(2, 0)),
        not(slotInPkPartition(3, 0)),
        slotInPkPartition(2, 1),
        slotInPkPartition(3, 1),
        not(slotInPkPartition(0, 1)),
        not(slotInPkPartition(1, 1)),
      }
    )
  }

  run writesWarmLeaderPointReadsButNotQueries = {
    pure val state =
      followerAcknowledgePut(
        leaderCommitPut(
          preparePut(authoritativeLeaderBaseState, 2),
          2,
        ),
        2,
      )
    assert(
      and {
        validState(state),
        canServeEventualGet(state, 2),
        not(cachePlan(state, forwardPage).serveWholePage),
        cachePlan(state, forwardPage).cacheEvaluatedKeys == List(),
      }
    )
  }

  run manifestWithoutCoverageCannotServeQuery = {
    pure val state =
      authoritativeLeaderBaseState
        .with("dbPresent", Set(1, 2))
        .with("leaderPayloadKeys", Set(1, 2))
        .with("leaderKnownManifestKeys", Set(1, 2))
    assert(
      and {
        validState(state),
        not(cachePlan(state, forwardPage).serveWholePage),
        cachePlan(state, forwardPage).cacheEvaluatedKeys == List(),
      }
    )
  }

  run queryFillCreatesEnoughProofToServeWholePage = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(1, 2)),
        0,
        3,
      )
    assert(
      and {
        validState(warmState),
        cachePlan(warmState, forwardPage).serveWholePage,
        cachePlan(warmState, forwardPage).cacheEvaluatedKeys == List(1, 2),
        cacheReturnedPage(warmState, forwardPage)
          == sourceReturnedPage(warmState, forwardPage),
      }
    )
  }

  run partialCoverageUsesMaximumSafePrefix = {
    pure val partialState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 2, 3)),
        0,
        2,
      )
    pure val longPage = makeQuery(0, 3, -1, 3, 1280, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(partialState),
        not(cachePlan(partialState, longPage).serveWholePage),
        cachePlan(partialState, longPage).cacheEvaluatedKeys == List(0, 2),
        cachePlan(partialState, longPage).dbSuffixNeeded,
      }
    )
  }

  run payloadEvictionStillAllowsWholePageViaMetadataAndKeyFetch = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 2)),
        0,
        3,
      )
    pure val evictedState =
      withLeaderCache(warmState, evictPayload(leaderCache(warmState), 2))
    assert(
      and {
        validState(evictedState),
        cachePlan(evictedState, forwardPage).serveWholePage,
        cachePlan(evictedState, forwardPage).payloadMisses == 1,
        cacheReturnedPage(evictedState, forwardPage) == List(0, 2),
      }
    )
  }

  run sharedDomainPayloadEvictionForcesFallbackForOverlappingQuery = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 2)),
        0,
        3,
      )
    pure val evictedState =
      evictLeaderPayloadWithSharedProofDomain(warmState, 2)
    assert(
      and {
        validState(evictedState),
        not(cachePlan(evictedState, forwardPage).serveWholePage),
        cachePlan(evictedState, forwardPage).cacheEvaluatedKeys == List(0),
      }
    )
  }

  run manifestEvictionShrinksCoverageAndStopsFullPageReads = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(1, 2)),
        0,
        3,
      )
    pure val evictedState =
      evictLeaderManifest(warmState, 1)
    assert(
      and {
        validState(evictedState),
        not(cachePlan(evictedState, forwardPage).serveWholePage),
        cachePlan(evictedState, forwardPage).cacheEvaluatedKeys == List(),
      }
    )
  }

  run unresolvedIntentBlocksLeaderPointRead = {
    pure val state =
      preparePut(authoritativeLeaderBaseState, 1)
    assert(
      and {
        validState(state),
        not(canServeEventualGet(state, 1)),
      }
    )
  }

  run eventualBatchGetServesTheMaximumSafeSubset = {
    pure val state =
      preparePut(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 2))
          .with("leaderPayloadKeys", Set(0))
          .with("leaderNegativeKeys", Set(1)),
        3,
      )
    pure val plan = batchGetPlan(state, false, Set(0, 1, 2, 3))
    assert(
      and {
        validState(state),
        plan.servedKeys == Set(0, 1),
        plan.fallbackKeys == Set(2, 3),
        plan.cachePayloadKeys == Set(0),
        plan.cacheNegativeKeys == Set(1),
      }
    )
  }

  run strongBatchGetAuthoritativeSafeSubset = {
    pure val state =
      authoritativeLeaderBaseState
        .with("dbPresent", Set(0, 2))
        .with("leaderPayloadKeys", Set(0))
        .with("leaderNegativeKeys", Set(1))
    pure val plan = batchGetPlan(state, true, Set(0, 1, 2))
    assert(
      and {
        validState(state),
        plan.servedKeys == Set(0, 1),
        plan.fallbackKeys == Set(2),
      }
    )
  }

  run strongGetAuthoritativePresentMatchesSource = {
    pure val state =
      authoritativeLeaderBaseState
        .with("dbPresent", Set(0))
        .with("leaderPayloadKeys", Set(0))
    assert(
      and {
        validState(state),
        canServeStrongGet(state, 0),
        strongGetDecision(state, 0, freshRequestEpoch(state)) == RESPONSE_SERVE_CACHE,
        eventualGetMatchesSource(state, 0),
      }
    )
  }

  run strongGetAuthoritativeAbsentMatchesSource = {
    pure val state =
      authoritativeLeaderBaseState
        .with("dbPresent", Set())
        .with("leaderNegativeKeys", Set(1))
    assert(
      and {
        validState(state),
        canServeStrongGet(state, 1),
        strongGetDecision(state, 1, freshRequestEpoch(state)) == RESPONSE_SERVE_CACHE,
        eventualGetMatchesSource(state, 1),
      }
    )
  }

  run strongBatchMixedSafeAndUnsafeKeys = {
    pure val state =
      preparePut(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 2))
          .with("leaderPayloadKeys", Set(0))
          .with("leaderNegativeKeys", Set(1)),
        3,
      )
    pure val plan = batchGetPlan(state, true, Set(0, 1, 2, 3))
    assert(
      and {
        validState(state),
        plan.servedKeys == Set(0, 1),
        plan.fallbackKeys == Set(2, 3),
      }
    )
  }

  run staleEpochRequestsNeverServeCache = {
    pure val state =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 1)),
        0,
        3,
      )
    assert(
      and {
        eventualGetDecision(state, 0, staleRequestEpoch(state)) == RESPONSE_STALE_EPOCH,
        strongGetDecision(state, 0, staleRequestEpoch(state)) == RESPONSE_STALE_EPOCH,
        batchGetDecision(state, false, Set(0, 1), staleRequestEpoch(state)).outcome
          == RESPONSE_STALE_EPOCH,
        batchGetDecision(state, false, Set(0, 1), staleRequestEpoch(state)).servedKeys == Set(),
        queryDecision(state, forwardPage, false, staleRequestEpoch(state)).outcome
          == RESPONSE_STALE_EPOCH,
        not(queryDecision(state, forwardPage, false, staleRequestEpoch(state)).serveWholePage),
      }
    )
  }

  run wrongLeaderRequestsRedirectInsteadOfServing = {
    pure val state =
      authoritativeLeaderBaseState
        .with("actualLeaderNode", NODE_FOLLOWER)
        .with("dbPresent", Set(0))
        .with("leaderPayloadKeys", Set(0))
        .with("leaderKnownManifestKeys", Set(0))
        .with("leaderCoveredSlots", Set(0))
    assert(
      and {
        validState(state),
        eventualGetDecision(state, 0, freshRequestEpoch(state)) == RESPONSE_WRONG_LEADER,
        strongGetDecision(state, 0, freshRequestEpoch(state)) == RESPONSE_WRONG_LEADER,
        batchGetDecision(state, false, Set(0), freshRequestEpoch(state)).outcome
          == RESPONSE_WRONG_LEADER,
        batchGetDecision(state, false, Set(0), freshRequestEpoch(state)).servedKeys == Set(),
        queryDecision(state, makeQuery(0, 0, -1, 1, 1024, false, false, false, GSI_PRIMARY), false, freshRequestEpoch(state)).outcome
          == RESPONSE_WRONG_LEADER,
      }
    )
  }

  run byteBudgetCanDeterminePageBoundaryInsideCoveredPrefix = {
    pure val state =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(1, 2, 3)),
        1,
        2,
      )
    pure val byteLimitedPage = makeQuery(1, 3, 0, 3, 640, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        cachePlan(state, byteLimitedPage).serveWholePage,
        cachePlan(state, byteLimitedPage).cacheEvaluatedKeys == List(1, 2),
        cachePlan(state, byteLimitedPage).evaluatedBytes == 640,
        sourceRawPage(
          state.with("leaderCoveredSlots", Set(1, 2, 3)),
          byteLimitedPage,
        ) == List(1, 2),
      }
    )
  }

  run gsiSchemaVersionMismatchBlocksOnlyGsiQueries = {
    pure val state =
      advanceActualGsiSchemaVersion(
        queryFillLeaderGsi(
          queryFillLeader(
            authoritativeLeaderBaseState
              .with("dbPresent", Set(1, 2))
              .with("gsiPresent", Set(2)),
            0,
            3,
          ),
          0,
          3,
        ),
        1,
      )
    pure val baseQuery = makeQuery(0, 3, -1, 2, 1024, false, false, false, GSI_PRIMARY)
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        cachePlan(state, baseQuery).serveWholePage,
        not(cachePlan(state, gsiQuery).serveWholePage),
        queryDecision(state, gsiQuery, false, freshRequestEpoch(state)).outcome
          == RESPONSE_FALLBACK_DB,
      }
    )
  }

  run gsiSchemaRefreshRestoresGsiQueries = {
    pure val staleWitnessState =
      advanceActualGsiSchemaVersion(
        queryFillLeaderGsi(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2))
            .with("gsiPresent", Set(2)),
          0,
          3,
        ),
        1,
      )
    pure val refreshedState =
      queryFillLeaderGsi(staleWitnessState, 0, 3)
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(refreshedState),
        cachePlan(refreshedState, gsiQuery).serveWholePage,
        cachePlan(refreshedState, gsiQuery).cacheEvaluatedKeys == List(2),
      }
    )
  }

  run baseSchemaVersionMismatchBlocksQueriesButNotPointReads = {
    pure val state =
      advanceActualBaseSchemaVersion(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1))
            .with("leaderPayloadKeys", Set(1))
            .with("leaderKnownManifestKeys", Set(1)),
          0,
          3,
        ),
        1,
      )
    pure val baseQuery = makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        canServeEventualGet(state, 1),
        not(cachePlan(state, baseQuery).serveWholePage),
        cachePlan(state, baseQuery).cacheEvaluatedKeys == List(1),
        queryDecision(state, baseQuery, false, freshRequestEpoch(state)).outcome
          == RESPONSE_MIXED,
      }
    )
  }

  run baseSchemaRefreshRestoresBaseQueries = {
    pure val staleWitnessState =
      advanceActualBaseSchemaVersion(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1)),
          0,
          3,
        ),
        1,
      )
    pure val refreshedState =
      queryFillLeader(staleWitnessState, 0, 3)
    pure val baseQuery = makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(refreshedState),
        cachePlan(refreshedState, baseQuery).serveWholePage,
        cachePlan(refreshedState, baseQuery).cacheEvaluatedKeys == List(1),
      }
    )
  }

  run partialBaseSchemaRevalidationServesMaximumSafePrefix = {
    pure val staleWitnessState =
      advanceActualBaseSchemaVersion(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(0, 1, 2)),
          0,
          3,
        ),
        1,
      )
    pure val partiallyRevalidated =
      queryFillLeader(staleWitnessState, 0, 1)
    pure val shortPage = makeQuery(0, 3, -1, 2, 384, false, false, false, GSI_PRIMARY)
    pure val longPage = makeQuery(0, 3, -1, 3, 1024, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(partiallyRevalidated),
        cachePlan(partiallyRevalidated, shortPage).serveWholePage,
        cachePlan(partiallyRevalidated, shortPage).cacheEvaluatedKeys == List(0, 1),
        queryDecision(partiallyRevalidated, shortPage, false, freshRequestEpoch(partiallyRevalidated)).outcome
          == RESPONSE_SERVE_CACHE,
        not(cachePlan(partiallyRevalidated, longPage).serveWholePage),
        cachePlan(partiallyRevalidated, longPage).cacheEvaluatedKeys == List(0, 1),
        cachePlan(partiallyRevalidated, longPage).dbSuffixNeeded,
        queryDecision(partiallyRevalidated, longPage, false, freshRequestEpoch(partiallyRevalidated)).outcome
          == RESPONSE_MIXED,
      }
    )
  }

  run partialGsiSchemaRevalidationServesMaximumSafePrefix = {
    pure val staleWitnessState =
      advanceActualGsiSchemaVersion(
        queryFillLeaderGsi(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(0, 1, 2))
            .with("gsiPresent", Set(0, 1, 2)),
          0,
          3,
        ),
        1,
      )
    pure val partiallyRevalidated =
      queryFillLeaderGsi(staleWitnessState, 0, 1)
    pure val shortPage = makeQuery(0, 3, -1, 2, 256, false, false, true, GSI_PRIMARY)
    pure val longPage = makeQuery(0, 3, -1, 3, 768, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(partiallyRevalidated),
        cachePlan(partiallyRevalidated, shortPage).serveWholePage,
        cachePlan(partiallyRevalidated, shortPage).cacheEvaluatedKeys == List(0, 1),
        queryDecision(partiallyRevalidated, shortPage, false, freshRequestEpoch(partiallyRevalidated)).outcome
          == RESPONSE_SERVE_CACHE,
        not(cachePlan(partiallyRevalidated, longPage).serveWholePage),
        cachePlan(partiallyRevalidated, longPage).cacheEvaluatedKeys == List(0, 1),
        cachePlan(partiallyRevalidated, longPage).dbSuffixNeeded,
        queryDecision(partiallyRevalidated, longPage, false, freshRequestEpoch(partiallyRevalidated)).outcome
          == RESPONSE_MIXED,
      }
    )
  }

  run strongGsiQueriesAreExplicitlyInvalid = {
    pure val state =
      queryFillLeaderGsi(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2))
            .with("gsiPresent", Set(1, 2)),
          0,
          3,
        ),
        0,
        3,
      )
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        queryDecision(state, gsiQuery, true, freshRequestEpoch(state)).outcome
          == RESPONSE_INVALID_GSI_STRONG,
      }
    )
  }

  run gsiQueriesUseSeparateSparseMembership = {
    pure val state =
      queryFillLeaderGsi(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2, 3))
            .with("gsiPresent", Set(2)),
          0,
          3,
        ),
        0,
        3,
      )
    pure val baseQuery = makeQuery(0, 3, -1, 2, 1024, false, false, false, GSI_PRIMARY)
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        cachePlan(state, baseQuery).serveWholePage,
        cachePlan(state, baseQuery).cacheEvaluatedKeys == List(1, 2),
        cachePlan(state, gsiQuery).serveWholePage,
        cachePlan(state, gsiQuery).cacheEvaluatedKeys == List(2),
        cacheReturnedPage(state, gsiQuery) == List(2),
        sourceReturnedPage(state, gsiQuery) == List(2),
      }
    )
  }

  run gsiMembershipRewriteAddsSparseEntryInsideCoveredRange = {
    pure val state =
      leaderAddGsiMembership(
        queryFillLeaderGsi(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2))
            .with("gsiPresent", Set(2))
            .with("leaderPayloadKeys", Set(1, 2))
            .with("leaderKnownManifestKeys", Set(1, 2)),
          0,
          3,
        ),
        1,
      )
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        state.gsiPresent == Set(1, 2),
        cachePlan(state, gsiQuery).serveWholePage,
        cachePlan(state, gsiQuery).cacheEvaluatedKeys == List(1, 2),
      }
    )
  }

  run gsiMembershipRewriteRemovesSparseEntryInsideCoveredRange = {
    pure val state =
      leaderRemoveGsiMembership(
        queryFillLeaderGsi(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2))
            .with("gsiPresent", Set(1, 2))
            .with("leaderPayloadKeys", Set(1, 2))
            .with("leaderKnownManifestKeys", Set(1, 2))
            .with("leaderKnownGsiManifestKeys", Set(1, 2)),
          0,
          3,
        ),
        1,
      )
    pure val gsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        state.gsiPresent == Set(2),
        cachePlan(state, gsiQuery).serveWholePage,
        cachePlan(state, gsiQuery).cacheEvaluatedKeys == List(2),
      }
    )
  }

  run gsiMembershipRewriteMovesResultBetweenQuerySpaces = {
    pure val state =
      leaderMoveGsiMembershipToAlt(
        queryFillLeaderGsiAlt(
          queryFillLeaderGsi(
            authoritativeLeaderBaseState
              .with("dbPresent", Set(1))
              .with("gsiPresent", Set(1)),
            0,
            3,
          ),
          0,
          3,
        ),
        1,
      )
    pure val primaryGsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_PRIMARY)
    pure val alternateGsiQuery = makeQuery(0, 3, -1, 2, 1024, false, false, true, GSI_ALT)
    assert(
      and {
        validState(state),
        state.gsiPresent == Set(),
        state.gsiAltPresent == Set(1),
        cachePlan(state, primaryGsiQuery).serveWholePage,
        cachePlan(state, primaryGsiQuery).cacheEvaluatedKeys == List(),
        cachePlan(state, alternateGsiQuery).serveWholePage,
        cachePlan(state, alternateGsiQuery).cacheEvaluatedKeys == List(1),
      }
    )
  }

  run gsiProjectionBytesCanServeMoreKeysThanBase = {
    pure val state =
      queryFillLeaderGsi(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1, 2, 3))
            .with("gsiPresent", Set(1, 2, 3)),
          1,
          3,
        ),
        1,
        3,
      )
    pure val baseQuery = makeQuery(1, 3, 0, 3, 768, false, false, false, GSI_PRIMARY)
    pure val gsiQuery = makeQuery(1, 3, 0, 3, 768, false, false, true, GSI_PRIMARY)
    assert(
      and {
        validState(state),
        cachePlan(state, baseQuery).serveWholePage,
        cachePlan(state, baseQuery).cacheEvaluatedKeys == List(1, 2),
        cachePlan(state, gsiQuery).serveWholePage,
        cachePlan(state, gsiQuery).cacheEvaluatedKeys == List(1, 2, 3),
        cachePlan(state, gsiQuery).evaluatedBytes == 704,
      }
    )
  }

  run promotedFollowerWithPreparedPutStaysFencedUntilRecovery = {
    pure val replicatedState =
      syncFollowerFromLeader(
        queryFillLeader(authoritativeLeaderBaseState, 0, 3)
      )
    pure val committedButUnacked =
      leaderCommitPut(preparePut(replicatedState, 2), 2)
    pure val promoted =
      promoteFollowerCatchingUp(committedButUnacked)
    pure val recovered =
      recoverPreparedOnFollower(promoted)
    pure val fullyAuthoritative =
      finishCatchUp(recovered)
    pure val singleItemPage = makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(promoted),
        not(canServeEventualGet(promoted, 2)),
        not(cachePlan(promoted, singleItemPage).serveWholePage),
        validState(recovered),
        canServeEventualGet(recovered, 2),
        not(cachePlan(recovered, singleItemPage).serveWholePage),
        cachePlan(fullyAuthoritative, singleItemPage).serveWholePage,
        cachePlan(fullyAuthoritative, singleItemPage).cacheEvaluatedKeys == List(2),
      }
    )
  }

  run promotedFollowerWithCommittedReplicaCanServePointReadsEarlierThanQueries = {
    pure val replicatedState =
      syncFollowerFromLeader(authoritativeLeaderBaseState)
    pure val ackedState =
      followerAcknowledgePut(
        leaderCommitPut(preparePut(replicatedState, 3), 3),
        3,
      )
    pure val promoted =
      promoteFollowerCatchingUp(ackedState)
    assert(
      and {
        validState(promoted),
        canServeEventualGet(promoted, 3),
        not(cachePlan(promoted, makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)).serveWholePage),
      }
    )
  }

  run promotedFollowerWithPreparedDeleteStaysFencedUntilRecovery = {
    pure val replicatedState =
      syncFollowerFromLeader(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(2))
            .with("leaderPayloadKeys", Set(2))
            .with("leaderKnownManifestKeys", Set(2)),
          0,
          3,
        )
      )
    pure val committedButUnacked =
      leaderCommitDelete(prepareDelete(replicatedState, 2), 2)
    pure val promoted =
      promoteFollowerCatchingUp(committedButUnacked)
    pure val recovered =
      recoverPreparedOnFollower(promoted)
    pure val fullyAuthoritative =
      finishCatchUp(recovered)
    pure val singleItemPage = makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(promoted),
        not(canServeEventualGet(promoted, 2)),
        validState(recovered),
        canServeEventualGet(recovered, 2),
        cachePlan(fullyAuthoritative, singleItemPage).serveWholePage,
        cachePlan(fullyAuthoritative, singleItemPage).cacheEvaluatedKeys == List(),
      }
    )
  }

  run reverseQueriesUseTheSameCoverageRules = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(1, 3)),
        1,
        3,
      )
    assert(
      and {
        validState(warmState),
        cachePlan(warmState, reversePage).serveWholePage,
        cachePlan(warmState, reversePage).cacheEvaluatedKeys == List(3, 1),
        cacheReturnedPage(warmState, reversePage)
          == sourceReturnedPage(warmState, reversePage),
      }
    )
  }

  run filteringStillUsesRawEvaluationBoundary = {
    pure val warmState =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 1, 2)),
        0,
        3,
      )
    pure val filteredPage = makeQuery(0, 3, -1, 2, 1024, true, false, false, GSI_PRIMARY)
    assert(
      and {
        validState(warmState),
        cachePlan(warmState, filteredPage).serveWholePage,
        cachePlan(warmState, filteredPage).cacheEvaluatedKeys == List(0, 1),
        cacheReturnedPage(warmState, filteredPage) == List(0),
        cacheReturnedPage(warmState, filteredPage)
          == sourceReturnedPage(warmState, filteredPage),
      }
    )
  }

  run bustedShardForcesFallback = {
    pure val state =
      markShardBusted(
        queryFillLeader(
          authoritativeLeaderBaseState
            .with("dbPresent", Set(1))
            .with("leaderPayloadKeys", Set(1))
            .with("leaderKnownManifestKeys", Set(1)),
          0,
          3,
        )
      )
    assert(
      and {
        validState(state),
        not(canServeEventualGet(state, 1)),
        not(cachePlan(state, makeQuery(0, 3, -1, 1, 1024, false, false, false, GSI_PRIMARY)).serveWholePage),
      }
    )
  }

  run coverageDoesNotLeakAcrossPartitions = {
    pure val leftPartitionPage =
      makePartitionQuery(0, 1, -1, 2, 1024, false, false, false, GSI_PRIMARY, 0)
    pure val rightPartitionPage =
      makePartitionQuery(2, 3, 1, 2, 1024, false, false, false, GSI_PRIMARY, 1)
    pure val warmLeftOnly =
      queryFillLeader(
        authoritativeLeaderBaseState
          .with("dbPresent", Set(0, 1, 2, 3)),
        0,
        1,
      )
    assert(
      and {
        cachePlan(warmLeftOnly, leftPartitionPage).serveWholePage,
        cachePlan(warmLeftOnly, leftPartitionPage).cacheEvaluatedKeys == List(0, 1),
        not(cachePlan(warmLeftOnly, rightPartitionPage).serveWholePage),
        cachePlan(warmLeftOnly, rightPartitionPage).cacheEvaluatedKeys == List(),
      }
    )
  }

  run gsiSortRewriteRequiresCurrentManifestOrder = {
    pure val gsiPage =
      makePartitionQuery(0, 1, -1, 2, 1024, false, false, true, GSI_PRIMARY, 0)
    pure val warmState =
      queryFillLeaderGsi(
        leaderAddGsiMembership(
          leaderAddGsiMembership(
            followerAcknowledgePut(
              leaderCommitPut(
                preparePut(
                  followerAcknowledgePut(
                    leaderCommitPut(
                      preparePut(authoritativeLeaderBaseState, 0),
                      0,
                    ),
                    0,
                  ),
                  1,
                ),
                1,
              ),
              1,
            ),
            0,
          ),
          1,
        ),
        0,
        1,
      )
    pure val staleOrderState =
      warmState.with("actualGsiPrimaryOrderVersion", 1)
    pure val refreshedOrderState =
      staleOrderState.with("leaderGsiPrimaryOrderVersion", 1)
    assert(
      and {
        cachePlan(warmState, gsiPage).serveWholePage,
        cachePlan(warmState, gsiPage).cacheEvaluatedKeys == List(0, 1),
        not(cachePlan(staleOrderState, gsiPage).serveWholePage),
        cachePlan(staleOrderState, gsiPage).cacheEvaluatedKeys == List(),
        cachePlan(refreshedOrderState, gsiPage).serveWholePage,
        cachePlan(refreshedOrderState, gsiPage).cacheEvaluatedKeys == List(1, 0),
      }
    )
  }

  // --- Uninitialized shard tests ---

  run uninitializedShardBlocksPointReads = {
    pure val state = uninitializedShardBaseState
    assert(
      and {
        validState(state),
        not(canServeEventualGet(state, 0)),
        not(canServeEventualGet(state, 1)),
        not(canServeEventualGet(state, 2)),
        not(canServeEventualGet(state, 3)),
      }
    )
  }

  run uninitializedShardBlocksQueries = {
    pure val state = uninitializedShardBaseState
    assert(
      and {
        validState(state),
        not(cachePlan(state, forwardPage).serveWholePage),
        not(cachePlan(state, reversePage).serveWholePage),
      }
    )
  }

  run uninitializedShardBlocksBatchGet = {
    pure val state = uninitializedShardBaseState
    assert(
      and {
        validState(state),
        batchGetPlan(state, false, Set(0, 1, 2)).servedKeys == Set(),
      }
    )
  }

  run assignedShardEnablesNormalOperation = {
    pure val state = uninitializedShardBaseState.with("shardAssigned", true)
      .with("itemAuthority", true)
      .with("queryAuthority", true)
      .with("gsiQueryAuthority", true)
    pure val withWrite =
      followerAcknowledgePut(
        leaderCommitPut(
          preparePut(state, 1),
          1,
        ),
        1,
      )
    assert(
      and {
        validState(withWrite),
        canServeEventualGet(withWrite, 1),
      }
    )
  }

  run drainedShardBlocksReads = {
    pure val state = authoritativeLeaderBaseState
      .with("itemAuthority", false)
      .with("queryAuthority", false)
      .with("gsiQueryAuthority", false)
    assert(
      and {
        validState(state),
        not(canServeEventualGet(state, 0)),
        not(cachePlan(state, forwardPage).serveWholePage),
      }
    )
  }
}
