module distributed_cache_model {
  pure val ORDERED_SLOTS = List(0, 1, 2, 3)
  pure val ORDERED_SLOTS_DESC = List(3, 2, 1, 0)
  pure val ORDERED_SLOTS_LEFT = List(0, 1)
  pure val ORDERED_SLOTS_LEFT_DESC = List(1, 0)
  pure val ORDERED_SLOTS_RIGHT = List(2, 3)
  pure val ORDERED_SLOTS_RIGHT_DESC = List(3, 2)
  pure val PRIMARY_GSI_ORDER_V0 = List(0, 1, 2, 3)
  pure val PRIMARY_GSI_ORDER_V1 = List(1, 0, 2, 3)
  pure val PRIMARY_GSI_ORDER_V0_DESC = List(3, 2, 1, 0)
  pure val PRIMARY_GSI_ORDER_V1_DESC = List(3, 2, 0, 1)
  pure val ALT_GSI_ORDER_V0 = List(0, 1, 2, 3)
  pure val ALT_GSI_ORDER_V1 = List(0, 1, 3, 2)
  pure val ALT_GSI_ORDER_V0_DESC = List(3, 2, 1, 0)
  pure val ALT_GSI_ORDER_V1_DESC = List(2, 3, 1, 0)
  pure val SLOT_SET = Set(0, 1, 2, 3)
  pure val PARTITIONS = Set(0, 1)
  // Model abstraction: a partition is the cache-node routing unit selected by
  // hashing the DynamoDB PK value. Slots model SK positions inside that PK's
  // range and must never influence cache-node selection.
  pure val NODE_LEADER = 0
  pure val NODE_FOLLOWER = 1
  pure val NODE_IDS = Set(NODE_LEADER, NODE_FOLLOWER)
  pure val GSI_PRIMARY = 0
  pure val GSI_ALT = 1
  pure val GSI_VARIANTS = Set(GSI_PRIMARY, GSI_ALT)
  pure val EPOCHS = Set(0, 1)
  pure val SCHEMA_VERSIONS = Set(0, 1, 2)
  pure val ORDER_VERSIONS = Set(0, 1)
  pure val LOWER_BOUNDS = Set(0, 1, 2, 3)
  pure val UPPER_BOUNDS = Set(0, 1, 2, 3)
  pure val START_MARKERS = Set(-1, 0, 1, 2, 3, 4)
  pure val PAGE_LIMITS = Set(1, 2, 3)
  pure val BYTE_BUDGETS = Set(384, 640, 768, 1024, 1280)
  pure val BOOLS = Set(false, true)

  var dbPresent: Set[int]
  var gsiPresent: Set[int]
  var gsiAltPresent: Set[int]

  var leaderPayloadKeys: Set[int]
  var leaderNegativeKeys: Set[int]
  var leaderKnownManifestKeys: Set[int]
  var leaderCoveredSlots: Set[int]
  var leaderKnownGsiManifestKeys: Set[int]
  var leaderCoveredGsiSlots: Set[int]
  var leaderKnownGsiAltManifestKeys: Set[int]
  var leaderCoveredGsiAltSlots: Set[int]

  var followerPayloadKeys: Set[int]
  var followerNegativeKeys: Set[int]
  var followerKnownManifestKeys: Set[int]
  var followerCoveredSlots: Set[int]
  var followerKnownGsiManifestKeys: Set[int]
  var followerCoveredGsiSlots: Set[int]
  var followerKnownGsiAltManifestKeys: Set[int]
  var followerCoveredGsiAltSlots: Set[int]

  var preparedPuts: Set[int]
  var preparedDeletes: Set[int]

  var servingFromFollower: bool
  var actualLeaderNode: int
  var actualEpoch: int
  var cacheEpoch: int
  var actualBaseSchemaVersion: int
  var actualGsiSchemaVersion: int
  var actualGsiPrimaryOrderVersion: int
  var actualGsiAltOrderVersion: int
  var leaderCurrentBaseSchemaCoveredSlots: Set[int]
  var followerCurrentBaseSchemaCoveredSlots: Set[int]
  var leaderCurrentGsiSchemaCoveredSlots: Set[int]
  var followerCurrentGsiSchemaCoveredSlots: Set[int]
  var leaderCurrentGsiAltSchemaCoveredSlots: Set[int]
  var followerCurrentGsiAltSchemaCoveredSlots: Set[int]
  var leaderGsiPrimaryOrderVersion: int
  var followerGsiPrimaryOrderVersion: int
  var leaderGsiAltOrderVersion: int
  var followerGsiAltOrderVersion: int
  var cachedWritesOnly: bool
  var itemAuthority: bool
  var queryAuthority: bool
  var gsiQueryAuthority: bool
  var manifestRebuilding: bool
  var continuityBroken: bool
  var shardBusted: bool
  var shardAssigned: bool

  pure def makePartitionQuery(lower, upper, startExclusive, limit, byteBudget, onlyEven, reverse, usesGsi, gsiVariant, partition) = {
    lower: lower,
    upper: upper,
    startExclusive: startExclusive,
    limit: limit,
    byteBudget: byteBudget,
    onlyEven: onlyEven,
    reverse: reverse,
    usesGsi: usesGsi,
    gsiVariant: gsiVariant,
    partition: partition,
  }

  pure def makeQuery(lower, upper, startExclusive, limit, byteBudget, onlyEven, reverse, usesGsi, gsiVariant) =
    makePartitionQuery(
      lower,
      upper,
      startExclusive,
      limit,
      byteBudget,
      onlyEven,
      reverse,
      usesGsi,
      gsiVariant,
      if (lower >= 2) 1 else 0,
    )

  pure def partitionLower(partition) =
    if (partition == 0) 0 else 2

  pure def partitionUpper(partition) =
    if (partition == 0) 1 else 3

  pure def partitionForPkHash(pkHash) =
    pkHash % 2

  pure def slotInPkPartition(slot, pkHash) =
    partitionLower(partitionForPkHash(pkHash)) <= slot
      and slot <= partitionUpper(partitionForPkHash(pkHash))

  pure def validQuery(query) =
    and {
      PARTITIONS.contains(query.partition),
      query.lower <= query.upper,
      partitionLower(query.partition) <= query.lower,
      query.upper <= partitionUpper(query.partition),
      if (query.reverse)
        and {
          query.startExclusive >= query.lower,
          query.startExclusive <= partitionUpper(query.partition) + 1,
        }
      else
        and {
          query.startExclusive >= partitionLower(query.partition) - 1,
          query.startExclusive <= query.upper,
        },
    }

  pure val ALL_QUERIES =
    tuples(
      LOWER_BOUNDS,
      UPPER_BOUNDS,
      START_MARKERS,
      PAGE_LIMITS,
      BYTE_BUDGETS,
      BOOLS,
      BOOLS,
      BOOLS,
      GSI_VARIANTS,
      PARTITIONS,
    )
      .map(tuple =>
        makePartitionQuery(
          tuple._1,
          tuple._2,
          tuple._3,
          tuple._4,
          tuple._5,
          tuple._6,
          tuple._7,
          tuple._8,
          tuple._9,
          tuple._10,
        )
      )
      .filter(query => validQuery(query))

  pure val BASE_QUERIES =
    ALL_QUERIES.filter(query => not(query.usesGsi))

  pure val GSI_QUERIES =
    ALL_QUERIES.filter(query => query.usesGsi)

  pure def liveState = {
    dbPresent: dbPresent,
    gsiPresent: gsiPresent,
    gsiAltPresent: gsiAltPresent,

    leaderPayloadKeys: leaderPayloadKeys,
    leaderNegativeKeys: leaderNegativeKeys,
    leaderKnownManifestKeys: leaderKnownManifestKeys,
    leaderCoveredSlots: leaderCoveredSlots,
    leaderKnownGsiManifestKeys: leaderKnownGsiManifestKeys,
    leaderCoveredGsiSlots: leaderCoveredGsiSlots,
    leaderKnownGsiAltManifestKeys: leaderKnownGsiAltManifestKeys,
    leaderCoveredGsiAltSlots: leaderCoveredGsiAltSlots,

    followerPayloadKeys: followerPayloadKeys,
    followerNegativeKeys: followerNegativeKeys,
    followerKnownManifestKeys: followerKnownManifestKeys,
    followerCoveredSlots: followerCoveredSlots,
    followerKnownGsiManifestKeys: followerKnownGsiManifestKeys,
    followerCoveredGsiSlots: followerCoveredGsiSlots,
    followerKnownGsiAltManifestKeys: followerKnownGsiAltManifestKeys,
    followerCoveredGsiAltSlots: followerCoveredGsiAltSlots,

    preparedPuts: preparedPuts,
    preparedDeletes: preparedDeletes,

    servingFromFollower: servingFromFollower,
    actualLeaderNode: actualLeaderNode,
    actualEpoch: actualEpoch,
    cacheEpoch: cacheEpoch,
    actualBaseSchemaVersion: actualBaseSchemaVersion,
    actualGsiSchemaVersion: actualGsiSchemaVersion,
    actualGsiPrimaryOrderVersion: actualGsiPrimaryOrderVersion,
    actualGsiAltOrderVersion: actualGsiAltOrderVersion,
    leaderCurrentBaseSchemaCoveredSlots: leaderCurrentBaseSchemaCoveredSlots,
    followerCurrentBaseSchemaCoveredSlots: followerCurrentBaseSchemaCoveredSlots,
    leaderCurrentGsiSchemaCoveredSlots: leaderCurrentGsiSchemaCoveredSlots,
    followerCurrentGsiSchemaCoveredSlots: followerCurrentGsiSchemaCoveredSlots,
    leaderCurrentGsiAltSchemaCoveredSlots: leaderCurrentGsiAltSchemaCoveredSlots,
    followerCurrentGsiAltSchemaCoveredSlots: followerCurrentGsiAltSchemaCoveredSlots,
    leaderGsiPrimaryOrderVersion: leaderGsiPrimaryOrderVersion,
    followerGsiPrimaryOrderVersion: followerGsiPrimaryOrderVersion,
    leaderGsiAltOrderVersion: leaderGsiAltOrderVersion,
    followerGsiAltOrderVersion: followerGsiAltOrderVersion,
    cachedWritesOnly: cachedWritesOnly,
    itemAuthority: itemAuthority,
    queryAuthority: queryAuthority,
    gsiQueryAuthority: gsiQueryAuthority,
    manifestRebuilding: manifestRebuilding,
    continuityBroken: continuityBroken,
    shardBusted: shardBusted,
    shardAssigned: shardAssigned,
  }

  pure def absentSlots(dbPresent) = SLOT_SET.exclude(dbPresent)

  pure def unresolvedIntents(state) =
    state.preparedPuts.union(state.preparedDeletes)

  pure def localServingNode(state) =
    if (state.servingFromFollower) NODE_FOLLOWER else NODE_LEADER

  pure def otherNode(node) =
    if (node == NODE_LEADER) NODE_FOLLOWER else NODE_LEADER

  pure def freshRequestEpoch(state) =
    state.actualEpoch

  pure def staleRequestEpoch(state) =
    if (state.actualEpoch == 0) 1 else 0

  pure def servingPayloadKeys(state) =
    if (state.servingFromFollower)
      state.followerPayloadKeys
    else
      state.leaderPayloadKeys

  pure def servingNegativeKeys(state) =
    if (state.servingFromFollower)
      state.followerNegativeKeys
    else
      state.leaderNegativeKeys

  pure def servingBaseManifestKeys(state) =
    if (state.servingFromFollower)
      state.followerKnownManifestKeys
    else
      state.leaderKnownManifestKeys

  pure def servingBaseCoveredSlots(state) =
    if (state.servingFromFollower)
      state.followerCoveredSlots
    else
      state.leaderCoveredSlots

  pure def servingGsiManifestKeys(state, query) =
    if (state.servingFromFollower)
      if (query.gsiVariant == GSI_ALT)
        state.followerKnownGsiAltManifestKeys
      else
        state.followerKnownGsiManifestKeys
    else
      if (query.gsiVariant == GSI_ALT)
        state.leaderKnownGsiAltManifestKeys
      else
        state.leaderKnownGsiManifestKeys

  pure def servingGsiCoveredSlots(state, query) =
    if (state.servingFromFollower)
      if (query.gsiVariant == GSI_ALT)
        state.followerCoveredGsiAltSlots
      else
        state.followerCoveredGsiSlots
    else
      if (query.gsiVariant == GSI_ALT)
        state.leaderCoveredGsiAltSlots
      else
        state.leaderCoveredGsiSlots

  pure def servingManifestKeys(state, query) =
    if (query.usesGsi)
      servingGsiManifestKeys(state, query)
    else
      servingBaseManifestKeys(state)

  pure def servingCoveredSlots(state, query) =
    if (query.usesGsi)
      servingGsiCoveredSlots(state, query)
    else
      servingBaseCoveredSlots(state)

  pure def servingCurrentBaseSchemaCoveredSlots(state) =
    if (state.servingFromFollower)
      state.followerCurrentBaseSchemaCoveredSlots
    else
      state.leaderCurrentBaseSchemaCoveredSlots

  pure def servingCurrentGsiSchemaCoveredSlots(state, query) =
    if (state.servingFromFollower)
      if (query.gsiVariant == GSI_ALT)
        state.followerCurrentGsiAltSchemaCoveredSlots
      else
        state.followerCurrentGsiSchemaCoveredSlots
    else
      if (query.gsiVariant == GSI_ALT)
        state.leaderCurrentGsiAltSchemaCoveredSlots
      else
        state.leaderCurrentGsiSchemaCoveredSlots

  pure def servingCurrentSchemaCoveredSlots(state, query) =
    if (query.usesGsi)
      servingCurrentGsiSchemaCoveredSlots(state, query)
    else
      servingCurrentBaseSchemaCoveredSlots(state)

  pure def actualGsiOrderVersion(state, query) =
    if (query.gsiVariant == GSI_ALT)
      state.actualGsiAltOrderVersion
    else
      state.actualGsiPrimaryOrderVersion

  pure def servingGsiOrderVersion(state, query) =
    if (state.servingFromFollower)
      if (query.gsiVariant == GSI_ALT)
        state.followerGsiAltOrderVersion
      else
        state.followerGsiPrimaryOrderVersion
    else
      if (query.gsiVariant == GSI_ALT)
        state.leaderGsiAltOrderVersion
      else
        state.leaderGsiPrimaryOrderVersion

  pure def manifestOrderCurrent(state, query) =
    not(query.usesGsi)
      or servingGsiOrderVersion(state, query) == actualGsiOrderVersion(state, query)

  pure def validLocalCacheFields(payloadKeys, negativeKeys, knownManifestKeys, coveredSlots, dbPresent, staleSlots) =
    and {
      payloadKeys.exclude(staleSlots).subseteq(dbPresent),
      negativeKeys.exclude(staleSlots).subseteq(absentSlots(dbPresent)),
      knownManifestKeys.exclude(staleSlots).subseteq(dbPresent),
      dbPresent
        .intersect(coveredSlots)
        .exclude(staleSlots)
        .subseteq(knownManifestKeys),
    }

  pure def validIndexCacheFields(knownManifestKeys, coveredSlots, sourceKeys, staleSlots) =
    and {
      knownManifestKeys.exclude(staleSlots).subseteq(sourceKeys),
      sourceKeys
        .intersect(coveredSlots)
        .exclude(staleSlots)
        .subseteq(knownManifestKeys),
    }

  pure def validState(state) = {
    pure val staleSlots = unresolvedIntents(state)
    and {
      state.gsiPresent.subseteq(state.dbPresent),
      state.gsiAltPresent.subseteq(state.dbPresent),
      state.gsiPresent.intersect(state.gsiAltPresent).size() == 0,
      validLocalCacheFields(
        state.leaderPayloadKeys,
        state.leaderNegativeKeys,
        state.leaderKnownManifestKeys,
        state.leaderCoveredSlots,
        state.dbPresent,
        staleSlots,
      ),
      validIndexCacheFields(
        state.leaderKnownGsiManifestKeys,
        state.leaderCoveredGsiSlots,
        state.gsiPresent,
        staleSlots,
      ),
      validIndexCacheFields(
        state.leaderKnownGsiAltManifestKeys,
        state.leaderCoveredGsiAltSlots,
        state.gsiAltPresent,
        staleSlots,
      ),
      validLocalCacheFields(
        state.followerPayloadKeys,
        state.followerNegativeKeys,
        state.followerKnownManifestKeys,
        state.followerCoveredSlots,
        state.dbPresent,
        staleSlots,
      ),
      validIndexCacheFields(
        state.followerKnownGsiManifestKeys,
        state.followerCoveredGsiSlots,
        state.gsiPresent,
        staleSlots,
      ),
      validIndexCacheFields(
        state.followerKnownGsiAltManifestKeys,
        state.followerCoveredGsiAltSlots,
        state.gsiAltPresent,
        staleSlots,
      ),
      NODE_IDS.contains(state.actualLeaderNode),
      EPOCHS.contains(state.actualEpoch),
      EPOCHS.contains(state.cacheEpoch),
      SCHEMA_VERSIONS.contains(state.actualBaseSchemaVersion),
      SCHEMA_VERSIONS.contains(state.actualGsiSchemaVersion),
      ORDER_VERSIONS.contains(state.actualGsiPrimaryOrderVersion),
      ORDER_VERSIONS.contains(state.actualGsiAltOrderVersion),
      ORDER_VERSIONS.contains(state.leaderGsiPrimaryOrderVersion),
      ORDER_VERSIONS.contains(state.followerGsiPrimaryOrderVersion),
      ORDER_VERSIONS.contains(state.leaderGsiAltOrderVersion),
      ORDER_VERSIONS.contains(state.followerGsiAltOrderVersion),
      state.leaderCurrentBaseSchemaCoveredSlots.subseteq(state.leaderCoveredSlots),
      state.followerCurrentBaseSchemaCoveredSlots.subseteq(state.followerCoveredSlots),
      state.leaderCurrentGsiSchemaCoveredSlots.subseteq(state.leaderCoveredGsiSlots),
      state.followerCurrentGsiSchemaCoveredSlots.subseteq(state.followerCoveredGsiSlots),
      state.leaderCurrentGsiAltSchemaCoveredSlots.subseteq(state.leaderCoveredGsiAltSlots),
      state.followerCurrentGsiAltSchemaCoveredSlots.subseteq(state.followerCoveredGsiAltSlots),
      state.preparedPuts.intersect(state.preparedDeletes).size() == 0,
      not(state.itemAuthority) or state.cacheEpoch == state.actualEpoch,
      not(state.queryAuthority) or state.itemAuthority,
      not(state.gsiQueryAuthority) or state.itemAuthority,
      if (state.shardBusted)
        and {
          not(state.itemAuthority),
          not(state.queryAuthority),
          not(state.gsiQueryAuthority),
        }
      else
        true,
      if (not(state.shardAssigned))
        and {
          not(state.itemAuthority),
          not(state.queryAuthority),
          not(state.gsiQueryAuthority),
        }
      else
        true,
    }
  }

  pure def baseOrderedSlots(query) =
    if (query.partition == 0)
      if (query.reverse) ORDERED_SLOTS_LEFT_DESC else ORDERED_SLOTS_LEFT
    else
      if (query.reverse) ORDERED_SLOTS_RIGHT_DESC else ORDERED_SLOTS_RIGHT

  pure def gsiOrderedSlots(query, orderVersion) =
    if (query.gsiVariant == GSI_ALT)
      if (orderVersion == 1)
        if (query.reverse) ALT_GSI_ORDER_V1_DESC else ALT_GSI_ORDER_V1
      else if (query.reverse)
        ALT_GSI_ORDER_V0_DESC
      else
        ALT_GSI_ORDER_V0
    else if (orderVersion == 1)
      if (query.reverse) PRIMARY_GSI_ORDER_V1_DESC else PRIMARY_GSI_ORDER_V1
    else if (query.reverse)
      PRIMARY_GSI_ORDER_V0_DESC
    else
      PRIMARY_GSI_ORDER_V0

  pure def orderedSlots(state, query) =
    if (query.usesGsi)
      gsiOrderedSlots(query, actualGsiOrderVersion(state, query))
        .select(slot => partitionLower(query.partition) <= slot and slot <= partitionUpper(query.partition))
    else
      baseOrderedSlots(query)

  pure def afterExclusiveStart(query, slot) =
    if (query.reverse)
      slot < query.startExclusive
    else
      slot > query.startExclusive

  pure def slotInRange(query, slot) =
    and {
      query.lower <= slot,
      slot <= query.upper,
      afterExclusiveStart(query, slot),
    }

  pure def candidateSlots(state, query) =
    orderedSlots(state, query).select(slot => slotInRange(query, slot))

  pure def filterMatches(query, slot) =
    if (query.onlyEven) slot % 2 == 0 else true

  pure def baseRawItemBytes(slot) =
    if (slot == 0)
      128
    else if (slot == 1)
      256
    else if (slot == 2)
      384
    else
      640

  pure def gsiRawItemBytes(slot) =
    if (slot == 0)
      96
    else if (slot == 1)
      160
    else if (slot == 2)
      224
    else
      320

  pure def rawItemBytes(query, slot) =
    if (query.usesGsi)
      gsiRawItemBytes(slot)
    else
      baseRawItemBytes(slot)

  pure def rawPageBytes(rawKeys, query) =
    rawKeys.foldl(0, (acc, slot) => acc + rawItemBytes(query, slot))

  pure def takeWithinRawBoundary(rawKeys, query) =
    rawKeys
      .foldl((List(), 0, false), (acc, slot) =>
        if (acc._3)
          acc
        else if (
          acc._1.length() < query.limit
            and acc._2 + rawItemBytes(query, slot) <= query.byteBudget
        )
          (acc._1.append(slot), acc._2 + rawItemBytes(query, slot), false)
        else
          (acc._1, acc._2, true)
      )
      ._1

  pure def sourceRawKeys(state, query) =
    candidateSlots(state, query).select(slot =>
      if (query.usesGsi)
        if (query.gsiVariant == GSI_ALT)
          state.gsiAltPresent.contains(slot)
        else
          state.gsiPresent.contains(slot)
      else
        state.dbPresent.contains(slot)
    )

  pure def sourceRawPage(state, query) =
    takeWithinRawBoundary(sourceRawKeys(state, query), query)

  pure def sourceReturnedPage(state, query) =
    sourceRawPage(state, query).select(slot => filterMatches(query, slot))

  pure def slotHasUnresolvedIntent(state, slot) =
    unresolvedIntents(state).contains(slot)

  pure def queryTouchesUnresolvedIntent(state, query) =
    candidateSlots(state, query)
      .select(slot => slotHasUnresolvedIntent(state, slot))
      .length() > 0

  pure def itemProofActive(state) =
    and {
      state.shardAssigned,
      localServingNode(state) == state.actualLeaderNode,
      state.cacheEpoch == state.actualEpoch,
      state.cachedWritesOnly,
      state.itemAuthority,
      not(state.shardBusted),
    }

  pure def queryProofActive(state, query) =
    and {
      itemProofActive(state),
      if (query.usesGsi)
        state.gsiQueryAuthority
      else
        state.queryAuthority,
      not(state.manifestRebuilding),
      not(state.continuityBroken),
      not(queryTouchesUnresolvedIntent(state, query)),
      manifestOrderCurrent(state, query),
    }

  pure def effectiveCoveredSlots(state, query) =
    servingCoveredSlots(state, query).intersect(servingCurrentSchemaCoveredSlots(state, query))

  pure def coveredPrefixSlots(state, query) =
    if (queryProofActive(state, query))
      candidateSlots(state, query)
        .foldl((List(), true), (acc, slot) =>
          if (acc._2 and effectiveCoveredSlots(state, query).contains(slot))
            (acc._1.append(slot), true)
          else
            (acc._1, false)
        )
        ._1
    else
      List()

  pure def cachePrefixRawKeys(state, query) =
    coveredPrefixSlots(state, query)
      .select(slot => servingManifestKeys(state, query).contains(slot))

  pure def cacheRawPage(state, query) =
    takeWithinRawBoundary(cachePrefixRawKeys(state, query), query)

  pure def cacheReturnedPage(state, query) =
    cacheRawPage(state, query).select(slot => filterMatches(query, slot))

  pure def payloadMissesOnCachePage(state, query) =
    cacheRawPage(state, query)
      .select(slot => not(servingPayloadKeys(state).contains(slot)))
      .length()

  pure def rawStopSatisfiedWithinPrefix(prefixRaw, query) = {
    pure val page = takeWithinRawBoundary(prefixRaw, query)
    or {
      page.length() == query.limit,
      rawPageBytes(page, query) == query.byteBudget,
      page.length() < prefixRaw.length(),
    }
  }

  pure def cachePlan(state, query) = {
    pure val prefixSlots = coveredPrefixSlots(state, query)
    pure val prefixRaw = cachePrefixRawKeys(state, query)
    pure val cacheRaw = cacheRawPage(state, query)
    pure val sourceRaw = sourceRawPage(state, query)
    pure val exhaustiveToBoundary =
      prefixSlots.length() == candidateSlots(state, query).length()
    {
      proofActive: queryProofActive(state, query),
      cachePrefixSlots: prefixSlots,
      cacheEvaluatedKeys: cacheRaw,
      cacheReturnedKeys: cacheReturnedPage(state, query),
      evaluatedBytes: rawPageBytes(cacheRaw, query),
      payloadMisses: payloadMissesOnCachePage(state, query),
      serveWholePage: queryProofActive(state, query) and or {
        rawStopSatisfiedWithinPrefix(prefixRaw, query),
        exhaustiveToBoundary,
      },
      dbSuffixNeeded: cacheRaw.length() < sourceRaw.length(),
    }
  }

  pure def canServeEventualGet(state, slot) =
    and {
      itemProofActive(state),
      not(slotHasUnresolvedIntent(state, slot)),
      or {
        servingPayloadKeys(state).contains(slot),
        servingNegativeKeys(state).contains(slot),
      },
    }

  pure def canServeStrongGet(state, slot) =
    canServeEventualGet(state, slot)

  pure def eventualGetMatchesSource(state, slot) =
    or {
      and {
        servingPayloadKeys(state).contains(slot),
        state.dbPresent.contains(slot),
      },
      and {
        servingNegativeKeys(state).contains(slot),
        not(state.dbPresent.contains(slot)),
      },
    }

  pure def batchGetPlan(state, strong, requestedKeys) = {
    pure val servedKeys =
      if (strong)
        requestedKeys.filter(slot => canServeStrongGet(state, slot))
      else
        requestedKeys.filter(slot => canServeEventualGet(state, slot))
    {
      servedKeys: servedKeys,
      fallbackKeys: requestedKeys.exclude(servedKeys),
      cachePayloadKeys: servedKeys.intersect(servingPayloadKeys(state)),
      cacheNegativeKeys: servedKeys.intersect(servingNegativeKeys(state)),
    }
  }

  pure val ROUTE_OK = "ok"
  pure val RESPONSE_STALE_EPOCH = "stale_epoch"
  pure val RESPONSE_WRONG_LEADER = "wrong_leader"
  pure val RESPONSE_SERVE_CACHE = "serve_cache"
  pure val RESPONSE_FALLBACK_DB = "fallback_db"
  pure val RESPONSE_MIXED = "mixed"
  pure val RESPONSE_INVALID_GSI_STRONG = "invalid_gsi_consistent_read"

  pure def requestRoute(state, requestEpoch) =
    if (requestEpoch != state.actualEpoch)
      RESPONSE_STALE_EPOCH
    else if (localServingNode(state) != state.actualLeaderNode)
      RESPONSE_WRONG_LEADER
    else
      ROUTE_OK

  pure def eventualGetDecision(state, slot, requestEpoch) = {
    pure val route = requestRoute(state, requestEpoch)
    if (route != ROUTE_OK)
      route
    else if (canServeEventualGet(state, slot))
      RESPONSE_SERVE_CACHE
    else
      RESPONSE_FALLBACK_DB
  }

  pure def strongGetDecision(state, slot, requestEpoch) = {
    pure val route = requestRoute(state, requestEpoch)
    if (route != ROUTE_OK)
      route
    else if (canServeStrongGet(state, slot))
      RESPONSE_SERVE_CACHE
    else
      RESPONSE_FALLBACK_DB
  }

  pure def emptyBatchPlan(requestedKeys) = {
    servedKeys: Set(),
    fallbackKeys: requestedKeys,
    cachePayloadKeys: Set(),
    cacheNegativeKeys: Set(),
  }

  pure def batchGetDecision(state, strong, requestedKeys, requestEpoch) = {
    pure val route = requestRoute(state, requestEpoch)
    pure val plan =
      if (route == ROUTE_OK)
        batchGetPlan(state, strong, requestedKeys)
      else
        emptyBatchPlan(requestedKeys)
    {
      route: route,
      outcome:
        if (route != ROUTE_OK)
          route
        else if (plan.servedKeys.size() == 0)
          RESPONSE_FALLBACK_DB
        else if (plan.fallbackKeys.size() == 0)
          RESPONSE_SERVE_CACHE
        else
          RESPONSE_MIXED,
      servedKeys: plan.servedKeys,
      fallbackKeys: plan.fallbackKeys,
    }
  }

  pure def queryDecision(state, query, strong, requestEpoch) = {
    pure val route = requestRoute(state, requestEpoch)
    pure val plan = cachePlan(state, query)
    {
      route: route,
      outcome:
        if (route != ROUTE_OK)
          route
        else if (strong and query.usesGsi)
          RESPONSE_INVALID_GSI_STRONG
        else if (strong)
          RESPONSE_FALLBACK_DB
        else if (plan.serveWholePage)
          RESPONSE_SERVE_CACHE
        else if (plan.cacheEvaluatedKeys.length() > 0)
          RESPONSE_MIXED
        else
          RESPONSE_FALLBACK_DB,
      serveWholePage:
        route == ROUTE_OK and not(strong) and plan.serveWholePage,
      cacheEvaluatedKeys:
        if (route == ROUTE_OK and not(strong))
          plan.cacheEvaluatedKeys
        else
          List(),
    }
  }

  action init =
    all {
      dbPresent' = Set(),
      gsiPresent' = Set(),
      gsiAltPresent' = Set(),

      leaderPayloadKeys' = Set(),
      leaderNegativeKeys' = Set(),
      leaderKnownManifestKeys' = Set(),
      leaderCoveredSlots' = Set(),
      leaderKnownGsiManifestKeys' = Set(),
      leaderCoveredGsiSlots' = Set(),
      leaderKnownGsiAltManifestKeys' = Set(),
      leaderCoveredGsiAltSlots' = Set(),

      followerPayloadKeys' = Set(),
      followerNegativeKeys' = Set(),
      followerKnownManifestKeys' = Set(),
      followerCoveredSlots' = Set(),
      followerKnownGsiManifestKeys' = Set(),
      followerCoveredGsiSlots' = Set(),
      followerKnownGsiAltManifestKeys' = Set(),
      followerCoveredGsiAltSlots' = Set(),

      preparedPuts' = Set(),
      preparedDeletes' = Set(),

      servingFromFollower' = false,
      actualLeaderNode' = NODE_LEADER,
      actualEpoch' = 0,
      cacheEpoch' = 0,
      actualBaseSchemaVersion' = 0,
      actualGsiSchemaVersion' = 0,
      actualGsiPrimaryOrderVersion' = 0,
      actualGsiAltOrderVersion' = 0,
      leaderCurrentBaseSchemaCoveredSlots' = Set(),
      followerCurrentBaseSchemaCoveredSlots' = Set(),
      leaderCurrentGsiSchemaCoveredSlots' = Set(),
      followerCurrentGsiSchemaCoveredSlots' = Set(),
      leaderCurrentGsiAltSchemaCoveredSlots' = Set(),
      followerCurrentGsiAltSchemaCoveredSlots' = Set(),
      leaderGsiPrimaryOrderVersion' = 0,
      followerGsiPrimaryOrderVersion' = 0,
      leaderGsiAltOrderVersion' = 0,
      followerGsiAltOrderVersion' = 0,
      cachedWritesOnly' = true,
      itemAuthority' = true,
      queryAuthority' = true,
      gsiQueryAuthority' = true,
      manifestRebuilding' = false,
      continuityBroken' = false,
      shardBusted' = false,
      shardAssigned' = true,
    }

  action step = {
    nondet nextDb = SLOT_SET.powerset().oneOf()
    nondet nextGsiPresent = nextDb.powerset().oneOf()
    nondet nextGsiAltPresent = nextDb.exclude(nextGsiPresent).powerset().oneOf()

    nondet nextLeaderPayload = SLOT_SET.powerset().oneOf()
    nondet nextLeaderNegative = SLOT_SET.powerset().oneOf()
    nondet nextLeaderManifest = SLOT_SET.powerset().oneOf()
    nondet nextLeaderCovered = SLOT_SET.powerset().oneOf()
    nondet nextLeaderGsiManifest = SLOT_SET.powerset().oneOf()
    nondet nextLeaderGsiCovered = SLOT_SET.powerset().oneOf()
    nondet nextLeaderGsiAltManifest = SLOT_SET.powerset().oneOf()
    nondet nextLeaderGsiAltCovered = SLOT_SET.powerset().oneOf()

    nondet nextFollowerPayload = SLOT_SET.powerset().oneOf()
    nondet nextFollowerNegative = SLOT_SET.powerset().oneOf()
    nondet nextFollowerManifest = SLOT_SET.powerset().oneOf()
    nondet nextFollowerCovered = SLOT_SET.powerset().oneOf()
    nondet nextFollowerGsiManifest = SLOT_SET.powerset().oneOf()
    nondet nextFollowerGsiCovered = SLOT_SET.powerset().oneOf()
    nondet nextFollowerGsiAltManifest = SLOT_SET.powerset().oneOf()
    nondet nextFollowerGsiAltCovered = SLOT_SET.powerset().oneOf()

    nondet nextPreparedPuts = SLOT_SET.powerset().oneOf()
    nondet nextPreparedDeletes = SLOT_SET.powerset().oneOf()

    nondet nextServingFromFollower = BOOLS.oneOf()
    nondet nextActualLeaderNode = NODE_IDS.oneOf()
    nondet nextActualEpoch = EPOCHS.oneOf()
    nondet nextCacheEpoch = EPOCHS.oneOf()
    nondet nextActualBaseSchemaVersion = SCHEMA_VERSIONS.oneOf()
    nondet nextActualGsiSchemaVersion = SCHEMA_VERSIONS.oneOf()
    nondet nextActualGsiPrimaryOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextActualGsiAltOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextLeaderCurrentBaseSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextFollowerCurrentBaseSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextLeaderCurrentGsiSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextFollowerCurrentGsiSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextLeaderCurrentGsiAltSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextFollowerCurrentGsiAltSchemaCovered = SLOT_SET.powerset().oneOf()
    nondet nextLeaderGsiPrimaryOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextFollowerGsiPrimaryOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextLeaderGsiAltOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextFollowerGsiAltOrderVersion = ORDER_VERSIONS.oneOf()
    nondet nextCachedWritesOnly = BOOLS.oneOf()
    nondet nextItemAuthority = BOOLS.oneOf()
    nondet nextQueryAuthority = BOOLS.oneOf()
    nondet nextGsiQueryAuthority = BOOLS.oneOf()
    nondet nextManifestRebuilding = BOOLS.oneOf()
    nondet nextContinuityBroken = BOOLS.oneOf()
    nondet nextShardBusted = BOOLS.oneOf()
    nondet nextShardAssigned = BOOLS.oneOf()

    pure val candidateState = {
      dbPresent: nextDb,
      gsiPresent: nextGsiPresent,
      gsiAltPresent: nextGsiAltPresent,

      leaderPayloadKeys: nextLeaderPayload,
      leaderNegativeKeys: nextLeaderNegative,
      leaderKnownManifestKeys: nextLeaderManifest,
      leaderCoveredSlots: nextLeaderCovered,
      leaderKnownGsiManifestKeys: nextLeaderGsiManifest,
      leaderCoveredGsiSlots: nextLeaderGsiCovered,
      leaderKnownGsiAltManifestKeys: nextLeaderGsiAltManifest,
      leaderCoveredGsiAltSlots: nextLeaderGsiAltCovered,

      followerPayloadKeys: nextFollowerPayload,
      followerNegativeKeys: nextFollowerNegative,
      followerKnownManifestKeys: nextFollowerManifest,
      followerCoveredSlots: nextFollowerCovered,
      followerKnownGsiManifestKeys: nextFollowerGsiManifest,
      followerCoveredGsiSlots: nextFollowerGsiCovered,
      followerKnownGsiAltManifestKeys: nextFollowerGsiAltManifest,
      followerCoveredGsiAltSlots: nextFollowerGsiAltCovered,

      preparedPuts: nextPreparedPuts,
      preparedDeletes: nextPreparedDeletes,

      servingFromFollower: nextServingFromFollower,
      actualLeaderNode: nextActualLeaderNode,
      actualEpoch: nextActualEpoch,
      cacheEpoch: nextCacheEpoch,
      actualBaseSchemaVersion: nextActualBaseSchemaVersion,
      actualGsiSchemaVersion: nextActualGsiSchemaVersion,
      actualGsiPrimaryOrderVersion: nextActualGsiPrimaryOrderVersion,
      actualGsiAltOrderVersion: nextActualGsiAltOrderVersion,
      leaderCurrentBaseSchemaCoveredSlots: nextLeaderCurrentBaseSchemaCovered,
      followerCurrentBaseSchemaCoveredSlots: nextFollowerCurrentBaseSchemaCovered,
      leaderCurrentGsiSchemaCoveredSlots: nextLeaderCurrentGsiSchemaCovered,
      followerCurrentGsiSchemaCoveredSlots: nextFollowerCurrentGsiSchemaCovered,
      leaderCurrentGsiAltSchemaCoveredSlots: nextLeaderCurrentGsiAltSchemaCovered,
      followerCurrentGsiAltSchemaCoveredSlots: nextFollowerCurrentGsiAltSchemaCovered,
      leaderGsiPrimaryOrderVersion: nextLeaderGsiPrimaryOrderVersion,
      followerGsiPrimaryOrderVersion: nextFollowerGsiPrimaryOrderVersion,
      leaderGsiAltOrderVersion: nextLeaderGsiAltOrderVersion,
      followerGsiAltOrderVersion: nextFollowerGsiAltOrderVersion,
      cachedWritesOnly: nextCachedWritesOnly,
      itemAuthority: nextItemAuthority,
      queryAuthority: nextQueryAuthority,
      gsiQueryAuthority: nextGsiQueryAuthority,
      manifestRebuilding: nextManifestRebuilding,
      continuityBroken: nextContinuityBroken,
      shardBusted: nextShardBusted,
      shardAssigned: nextShardAssigned,
    }

    all {
      validState(candidateState),

      dbPresent' = nextDb,
      gsiPresent' = nextGsiPresent,
      gsiAltPresent' = nextGsiAltPresent,

      leaderPayloadKeys' = nextLeaderPayload,
      leaderNegativeKeys' = nextLeaderNegative,
      leaderKnownManifestKeys' = nextLeaderManifest,
      leaderCoveredSlots' = nextLeaderCovered,
      leaderKnownGsiManifestKeys' = nextLeaderGsiManifest,
      leaderCoveredGsiSlots' = nextLeaderGsiCovered,
      leaderKnownGsiAltManifestKeys' = nextLeaderGsiAltManifest,
      leaderCoveredGsiAltSlots' = nextLeaderGsiAltCovered,

      followerPayloadKeys' = nextFollowerPayload,
      followerNegativeKeys' = nextFollowerNegative,
      followerKnownManifestKeys' = nextFollowerManifest,
      followerCoveredSlots' = nextFollowerCovered,
      followerKnownGsiManifestKeys' = nextFollowerGsiManifest,
      followerCoveredGsiSlots' = nextFollowerGsiCovered,
      followerKnownGsiAltManifestKeys' = nextFollowerGsiAltManifest,
      followerCoveredGsiAltSlots' = nextFollowerGsiAltCovered,

      preparedPuts' = nextPreparedPuts,
      preparedDeletes' = nextPreparedDeletes,

      servingFromFollower' = nextServingFromFollower,
      actualLeaderNode' = nextActualLeaderNode,
      actualEpoch' = nextActualEpoch,
      cacheEpoch' = nextCacheEpoch,
      actualBaseSchemaVersion' = nextActualBaseSchemaVersion,
      actualGsiSchemaVersion' = nextActualGsiSchemaVersion,
      actualGsiPrimaryOrderVersion' = nextActualGsiPrimaryOrderVersion,
      actualGsiAltOrderVersion' = nextActualGsiAltOrderVersion,
      leaderCurrentBaseSchemaCoveredSlots' = nextLeaderCurrentBaseSchemaCovered,
      followerCurrentBaseSchemaCoveredSlots' = nextFollowerCurrentBaseSchemaCovered,
      leaderCurrentGsiSchemaCoveredSlots' = nextLeaderCurrentGsiSchemaCovered,
      followerCurrentGsiSchemaCoveredSlots' = nextFollowerCurrentGsiSchemaCovered,
      leaderCurrentGsiAltSchemaCoveredSlots' = nextLeaderCurrentGsiAltSchemaCovered,
      followerCurrentGsiAltSchemaCoveredSlots' = nextFollowerCurrentGsiAltSchemaCovered,
      leaderGsiPrimaryOrderVersion' = nextLeaderGsiPrimaryOrderVersion,
      followerGsiPrimaryOrderVersion' = nextFollowerGsiPrimaryOrderVersion,
      leaderGsiAltOrderVersion' = nextLeaderGsiAltOrderVersion,
      followerGsiAltOrderVersion' = nextFollowerGsiAltOrderVersion,
      cachedWritesOnly' = nextCachedWritesOnly,
      itemAuthority' = nextItemAuthority,
      queryAuthority' = nextQueryAuthority,
      gsiQueryAuthority' = nextGsiQueryAuthority,
      manifestRebuilding' = nextManifestRebuilding,
      continuityBroken' = nextContinuityBroken,
      shardBusted' = nextShardBusted,
      shardAssigned' = nextShardAssigned,
    }
  }

  val validLiveState = validState(liveState)

  val strongReadsUseItemAuthority =
    SLOT_SET.forall(slot =>
      canServeStrongGet(liveState, slot) == canServeEventualGet(liveState, slot)
    )

  val preparedIntentGetsBypass =
    SLOT_SET.forall(slot =>
      not(slotHasUnresolvedIntent(liveState, slot))
        or not(canServeEventualGet(liveState, slot))
    )

  val eventualGetsAreSound =
    SLOT_SET.forall(slot =>
      not(canServeEventualGet(liveState, slot))
        or eventualGetMatchesSource(liveState, slot)
    )

  val strongBatchGetsUseAllSafeKeys =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetPlan(liveState, true, requestedKeys).servedKeys
        == requestedKeys.filter(slot => canServeStrongGet(liveState, slot))
    )

  val eventualBatchGetsAreSound =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetPlan(liveState, false, requestedKeys).servedKeys.forall(slot =>
        eventualGetMatchesSource(liveState, slot)
      )
    )

  val eventualBatchGetsUseAllSafeKeys =
    SLOT_SET.powerset().forall(requestedKeys =>
      batchGetPlan(liveState, false, requestedKeys).servedKeys
        == requestedKeys.filter(slot => canServeEventualGet(liveState, slot))
    )

  val staleEpochReadsNeverServeCache =
    and {
      SLOT_SET.forall(slot =>
        and {
          eventualGetDecision(liveState, slot, staleRequestEpoch(liveState)) == RESPONSE_STALE_EPOCH,
          strongGetDecision(liveState, slot, staleRequestEpoch(liveState)) == RESPONSE_STALE_EPOCH,
        }
      ),
      SLOT_SET.powerset().forall(requestedKeys =>
        and {
          batchGetDecision(liveState, false, requestedKeys, staleRequestEpoch(liveState)).outcome
            == RESPONSE_STALE_EPOCH,
          batchGetDecision(liveState, false, requestedKeys, staleRequestEpoch(liveState)).servedKeys
            == Set(),
          batchGetDecision(liveState, true, requestedKeys, staleRequestEpoch(liveState)).outcome
            == RESPONSE_STALE_EPOCH,
        }
      ),
      ALL_QUERIES.forall(query =>
        and {
          queryDecision(liveState, query, false, staleRequestEpoch(liveState)).outcome
            == RESPONSE_STALE_EPOCH,
          not(queryDecision(liveState, query, false, staleRequestEpoch(liveState)).serveWholePage),
          queryDecision(liveState, query, true, staleRequestEpoch(liveState)).outcome
            == RESPONSE_STALE_EPOCH,
        }
      ),
    }

  val wrongLeaderReadsRedirect =
    and {
      SLOT_SET.forall(slot =>
        localServingNode(liveState) == liveState.actualLeaderNode or and {
          eventualGetDecision(liveState, slot, freshRequestEpoch(liveState)) == RESPONSE_WRONG_LEADER,
          strongGetDecision(liveState, slot, freshRequestEpoch(liveState)) == RESPONSE_WRONG_LEADER,
        }
      ),
      SLOT_SET.powerset().forall(requestedKeys =>
        localServingNode(liveState) == liveState.actualLeaderNode or and {
          batchGetDecision(liveState, false, requestedKeys, freshRequestEpoch(liveState)).outcome
            == RESPONSE_WRONG_LEADER,
          batchGetDecision(liveState, false, requestedKeys, freshRequestEpoch(liveState)).servedKeys
            == Set(),
          batchGetDecision(liveState, true, requestedKeys, freshRequestEpoch(liveState)).outcome
            == RESPONSE_WRONG_LEADER,
        }
      ),
      ALL_QUERIES.forall(query =>
        localServingNode(liveState) == liveState.actualLeaderNode or and {
          queryDecision(liveState, query, false, freshRequestEpoch(liveState)).outcome
            == RESPONSE_WRONG_LEADER,
          not(queryDecision(liveState, query, false, freshRequestEpoch(liveState)).serveWholePage),
          queryDecision(liveState, query, true, freshRequestEpoch(liveState)).outcome
            == RESPONSE_WRONG_LEADER,
        }
      ),
    }

  val freshRoutedReadsMatchLocalRules =
    and {
      SLOT_SET.forall(slot =>
        localServingNode(liveState) != liveState.actualLeaderNode or and {
          eventualGetDecision(liveState, slot, freshRequestEpoch(liveState))
            == if (canServeEventualGet(liveState, slot))
              RESPONSE_SERVE_CACHE
            else
              RESPONSE_FALLBACK_DB,
          strongGetDecision(liveState, slot, freshRequestEpoch(liveState))
            == if (canServeStrongGet(liveState, slot))
              RESPONSE_SERVE_CACHE
            else
              RESPONSE_FALLBACK_DB,
        }
      ),
      SLOT_SET.powerset().forall(requestedKeys =>
        localServingNode(liveState) != liveState.actualLeaderNode or and {
          batchGetDecision(liveState, false, requestedKeys, freshRequestEpoch(liveState)).servedKeys
            == batchGetPlan(liveState, false, requestedKeys).servedKeys,
          batchGetDecision(liveState, true, requestedKeys, freshRequestEpoch(liveState)).servedKeys
            == batchGetPlan(liveState, true, requestedKeys).servedKeys,
        }
      ),
      ALL_QUERIES.forall(query =>
        localServingNode(liveState) != liveState.actualLeaderNode or and {
          queryDecision(liveState, query, false, freshRequestEpoch(liveState)).serveWholePage
            == cachePlan(liveState, query).serveWholePage,
          queryDecision(liveState, query, true, freshRequestEpoch(liveState)).outcome
            == if (query.usesGsi)
              RESPONSE_INVALID_GSI_STRONG
            else
              RESPONSE_FALLBACK_DB,
        }
      ),
    }

  val strongGsiQueriesRemainInvalid =
    ALL_QUERIES.forall(query =>
      requestRoute(liveState, freshRequestEpoch(liveState)) != ROUTE_OK
        or not(query.usesGsi)
        or queryDecision(liveState, query, true, freshRequestEpoch(liveState)).outcome
          == RESPONSE_INVALID_GSI_STRONG
    )

  val partialQueryPrefixesReturnMixed =
    ALL_QUERIES.forall(query =>
      requestRoute(liveState, freshRequestEpoch(liveState)) != ROUTE_OK
        or and {
          cachePlan(liveState, query).serveWholePage
            or cachePlan(liveState, query).cacheEvaluatedKeys.length() == 0
            or queryDecision(liveState, query, false, freshRequestEpoch(liveState)).outcome
              == RESPONSE_MIXED,
          cachePlan(liveState, query).serveWholePage
            or cachePlan(liveState, query).cacheEvaluatedKeys.length() > 0
            or queryDecision(liveState, query, false, freshRequestEpoch(liveState)).outcome
              == RESPONSE_FALLBACK_DB,
        }
    )

  val baseQueriesOnlyUseCurrentSchemaCoverage =
    BASE_QUERIES.forall(query =>
      cachePlan(liveState, query).cacheEvaluatedKeys
        .select(slot => not(servingCurrentBaseSchemaCoveredSlots(liveState).contains(slot)))
        .length() == 0
    )

  val gsiQueriesOnlyUseCurrentSchemaCoverage =
    GSI_QUERIES.forall(query =>
      cachePlan(liveState, query).cacheEvaluatedKeys
        .select(slot => not(servingCurrentGsiSchemaCoveredSlots(liveState, query).contains(slot)))
        .length() == 0
    )

  val gsiQueriesOnlyServeWithCurrentManifestOrder =
    GSI_QUERIES.forall(query =>
      not(cachePlan(liveState, query).serveWholePage)
        or manifestOrderCurrent(liveState, query)
    )

  val servedQueriesAvoidPreparedWrites =
    ALL_QUERIES.forall(query =>
      not(cachePlan(liveState, query).serveWholePage)
        or not(queryTouchesUnresolvedIntent(liveState, query))
    )

  val cachePrefixMatchesSourcePrefix =
    ALL_QUERIES.forall(query =>
      cachePrefixRawKeys(liveState, query)
        == sourceRawKeys(liveState, query)
          .slice(0, cachePrefixRawKeys(liveState, query).length())
    )

  val fullPageCacheReadsMatchSource =
    ALL_QUERIES.forall(query =>
      not(cachePlan(liveState, query).serveWholePage)
        or cacheReturnedPage(liveState, query)
          == sourceReturnedPage(liveState, query)
    )
}
