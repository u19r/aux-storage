/// Compact MBT contract for the storage-cache CacheState/Transition boundary.
///
/// The existing distributed-cache scenario models cover broad protocol and
/// cluster behavior. This model drives the owning Rust transition reducer and
/// compares the state that matters for cached writes, query coverage, and
/// authority fencing after every generated transition.
module distributed_cache_transition_mbt {
  pure val SLOTS = Set(0, 1, 2, 3)
  pure val OPERATIONS = Set(
    "prepare_put",
    "commit_put",
    "ack_put",
    "prepare_delete",
    "commit_delete",
    "ack_delete",
    "abort_prepared",
    "fill_base",
    "sync_follower",
    "lose_epoch",
    "gain_epoch",
    "lose_leader",
    "regain_leader",
    "promote_follower",
    "finish_catch_up",
    "bust_shard",
    "assign_shard",
    "drain_shard",
  )

  type CacheState = {
    dbPresent: Set[int],
    preparedPuts: Set[int],
    preparedDeletes: Set[int],
    leaderPayloadKeys: Set[int],
    leaderNegativeKeys: Set[int],
    leaderManifestKeys: Set[int],
    leaderCoveredSlots: Set[int],
    followerPayloadKeys: Set[int],
    followerNegativeKeys: Set[int],
    followerManifestKeys: Set[int],
    followerCoveredSlots: Set[int],
    servingNode: int,
    actualLeaderNode: int,
    actualEpoch: int,
    cacheEpoch: int,
    itemAuthority: bool,
    queryAuthority: bool,
    gsiQueryAuthority: bool,
    shardBusted: bool,
    shardAssigned: bool,
    lastOperation: str,
    lastSlot: int,
    lastLower: int,
    lastUpper: int,
    lastMask: int,
    lastOutcome: str,
  }

  var cache: CacheState

  pure def initialState = {
    dbPresent: Set(),
    preparedPuts: Set(),
    preparedDeletes: Set(),
    leaderPayloadKeys: Set(),
    leaderNegativeKeys: Set(),
    leaderManifestKeys: Set(),
    leaderCoveredSlots: Set(),
    followerPayloadKeys: Set(),
    followerNegativeKeys: Set(),
    followerManifestKeys: Set(),
    followerCoveredSlots: Set(),
    servingNode: 0,
    actualLeaderNode: 0,
    actualEpoch: 0,
    cacheEpoch: 0,
    itemAuthority: true,
    queryAuthority: true,
    gsiQueryAuthority: true,
    shardBusted: false,
    shardAssigned: true,
    lastOperation: "init",
    lastSlot: 0,
    lastLower: 0,
    lastUpper: 0,
    lastMask: 0,
    lastOutcome: "not_checked",
  }

  pure def requested(state, operation, slot, lower, upper, mask) =
    state
      .with("lastOperation", operation)
      .with("lastSlot", slot)
      .with("lastLower", lower)
      .with("lastUpper", upper)
      .with("lastMask", mask)
      .with("lastOutcome", "rejected")

  pure def applied(state) = state.with("lastOutcome", "applied")

  pure def validWritePreparation(state, slot) =
    state.actualLeaderNode == 0
      and state.servingNode == 0
      and not(state.shardBusted)
      and not(state.preparedPuts.contains(slot))
      and not(state.preparedDeletes.contains(slot))

  pure def visiblePut(state, replica, slot) =
    if (replica == 0)
      state
        .with("leaderPayloadKeys", state.leaderPayloadKeys.union(Set(slot)))
        .with("leaderNegativeKeys", state.leaderNegativeKeys.exclude(Set(slot)))
        .with("leaderManifestKeys", state.leaderManifestKeys.union(Set(slot)))
    else
      state
        .with("followerPayloadKeys", state.followerPayloadKeys.union(Set(slot)))
        .with("followerNegativeKeys", state.followerNegativeKeys.exclude(Set(slot)))
        .with("followerManifestKeys", state.followerManifestKeys.union(Set(slot)))

  pure def visibleDelete(state, replica, slot) =
    if (replica == 0)
      state
        .with("leaderPayloadKeys", state.leaderPayloadKeys.exclude(Set(slot)))
        .with("leaderNegativeKeys", state.leaderNegativeKeys.union(Set(slot)))
        .with("leaderManifestKeys", state.leaderManifestKeys.exclude(Set(slot)))
    else
      state
        .with("followerPayloadKeys", state.followerPayloadKeys.exclude(Set(slot)))
        .with("followerNegativeKeys", state.followerNegativeKeys.union(Set(slot)))
        .with("followerManifestKeys", state.followerManifestKeys.exclude(Set(slot)))

  pure def fillBase(state, lower, upper) = {
    pure val filledSlots = SLOTS.filter(slot => lower <= slot and slot <= upper)
    pure val presentInFill = filledSlots.intersect(state.dbPresent)
    pure val emptyInFill = filledSlots.exclude(state.dbPresent)
    if (state.servingNode == 0)
      state
        .with("leaderCoveredSlots", state.leaderCoveredSlots.union(filledSlots))
        .with("leaderManifestKeys", state.leaderManifestKeys.union(presentInFill))
        .with("leaderPayloadKeys", state.leaderPayloadKeys.union(presentInFill))
        .with("leaderNegativeKeys", state.leaderNegativeKeys.union(emptyInFill))
    else
      state
        .with("followerCoveredSlots", state.followerCoveredSlots.union(filledSlots))
        .with("followerManifestKeys", state.followerManifestKeys.union(presentInFill))
        .with("followerPayloadKeys", state.followerPayloadKeys.union(presentInFill))
        .with("followerNegativeKeys", state.followerNegativeKeys.union(emptyInFill))
  }

  pure def applyOperation(state, operation, slot, lower, upper, mask) = {
    pure val base = requested(state, operation, slot, lower, upper, mask)
    if (operation == "prepare_put" and validWritePreparation(state, slot))
      applied(base.with("preparedPuts", state.preparedPuts.union(Set(slot))))
    else if (operation == "prepare_delete" and validWritePreparation(state, slot))
      applied(base.with("preparedDeletes", state.preparedDeletes.union(Set(slot))))
    else if (
      operation == "commit_put"
        and state.actualLeaderNode == 0
        and state.servingNode == 0
        and state.preparedPuts.contains(slot)
    )
      applied(
        base
          .with("dbPresent", state.dbPresent.union(Set(slot)))
          .with("leaderPayloadKeys", state.leaderPayloadKeys.union(Set(slot)))
          .with("leaderNegativeKeys", state.leaderNegativeKeys.exclude(Set(slot)))
          .with("leaderManifestKeys", state.leaderManifestKeys.union(Set(slot)))
      )
    else if (operation == "ack_put" and state.preparedPuts.contains(slot) and state.dbPresent.contains(slot))
      applied(
        visiblePut(
          base.with("preparedPuts", state.preparedPuts.exclude(Set(slot))),
          1,
          slot,
        )
      )
    else if (
      operation == "commit_delete"
        and state.actualLeaderNode == 0
        and state.servingNode == 0
        and state.preparedDeletes.contains(slot)
    )
      applied(
        visibleDelete(
          base.with("dbPresent", state.dbPresent.exclude(Set(slot))),
          0,
          slot,
        )
          .with("preparedDeletes", state.preparedDeletes)
      )
    else if (
      operation == "ack_delete"
        and state.preparedDeletes.contains(slot)
        and not(state.dbPresent.contains(slot))
    )
      applied(
        visibleDelete(
          base.with("preparedDeletes", state.preparedDeletes.exclude(Set(slot))),
          1,
          slot,
        )
      )
    else if (
      operation == "abort_prepared"
        and (
          (state.preparedPuts.contains(slot) and not(state.dbPresent.contains(slot)))
            or (state.preparedDeletes.contains(slot) and state.dbPresent.contains(slot))
        )
    )
      applied(
        base
          .with("preparedPuts", state.preparedPuts.exclude(Set(slot)))
          .with("preparedDeletes", state.preparedDeletes.exclude(Set(slot)))
      )
    else if (
      operation == "fill_base"
        and lower <= upper
        and state.actualLeaderNode == state.servingNode
        and state.itemAuthority
    )
      applied(fillBase(base, lower, upper))
    else if (operation == "sync_follower")
      applied(
        base
          .with("followerPayloadKeys", state.leaderPayloadKeys)
          .with("followerNegativeKeys", state.leaderNegativeKeys)
          .with("followerManifestKeys", state.leaderManifestKeys)
          .with("followerCoveredSlots", state.leaderCoveredSlots)
      )
    else if (operation == "lose_epoch")
      applied(
        base
          .with("actualEpoch", if (state.actualEpoch == 0) 1 else 0)
          .with("itemAuthority", false)
          .with("queryAuthority", false)
          .with("gsiQueryAuthority", false)
      )
    else if (
      operation == "gain_epoch"
        and not(state.shardBusted)
        and state.shardAssigned
    )
      applied(
        base
          .with("cacheEpoch", state.actualEpoch)
          .with("itemAuthority", true)
          .with("queryAuthority", true)
          .with("gsiQueryAuthority", true)
      )
    else if (operation == "lose_leader")
      applied(base.with("actualLeaderNode", 1 - state.servingNode))
    else if (operation == "regain_leader")
      applied(base.with("actualLeaderNode", state.servingNode))
    else if (
      operation == "promote_follower"
        and state.servingNode == 0
        and not(state.shardBusted)
        and state.shardAssigned
    )
      applied(
        base
          .with("servingNode", 1)
          .with("actualLeaderNode", 1)
          .with("cacheEpoch", state.actualEpoch)
          .with("itemAuthority", true)
          .with("queryAuthority", false)
          .with("gsiQueryAuthority", false)
      )
    else if (
      operation == "finish_catch_up"
        and state.servingNode == 1
        and state.itemAuthority
        and not(state.shardBusted)
    )
      applied(base.with("queryAuthority", true).with("gsiQueryAuthority", true))
    else if (operation == "bust_shard")
      applied(
        base
          .with("shardBusted", true)
          .with("itemAuthority", false)
          .with("queryAuthority", false)
          .with("gsiQueryAuthority", false)
      )
    else if (
      operation == "assign_shard"
        and not(state.shardAssigned)
        and not(state.shardBusted)
        and state.cacheEpoch == state.actualEpoch
    )
      applied(
        base
          .with("shardAssigned", true)
          .with("itemAuthority", true)
          .with("queryAuthority", true)
          .with("gsiQueryAuthority", true)
      )
    else if (operation == "drain_shard" and state.shardAssigned and not(state.shardBusted))
      applied(
        base
          .with("shardAssigned", false)
          .with("itemAuthority", false)
          .with("queryAuthority", false)
          .with("gsiQueryAuthority", false)
      )
    else
      base
  }

  action init = all {
    cache' = initialState,
  }

  action Step = {
    nondet operation = OPERATIONS.oneOf()
    nondet slot = SLOTS.oneOf()
    nondet lower = SLOTS.oneOf()
    nondet upper = SLOTS.oneOf()
    nondet mask = Set(0, 1, 3, 5, 15).oneOf()
    cache' = applyOperation(cache, operation, slot, lower, upper, mask)
  }

  action step = Step

  val inv =
    cache.preparedPuts.intersect(cache.preparedDeletes) == Set()
      and cache.leaderPayloadKeys.intersect(cache.dbPresent) == cache.leaderPayloadKeys
      and cache.followerPayloadKeys.intersect(cache.dbPresent) == cache.followerPayloadKeys
      and (not(cache.itemAuthority) or cache.cacheEpoch == cache.actualEpoch)
      and (not(cache.queryAuthority) or cache.itemAuthority)
      and (not(cache.gsiQueryAuthority) or cache.itemAuthority)
      and (not(cache.shardBusted) or (
        not(cache.itemAuthority)
          and not(cache.queryAuthority)
          and not(cache.gsiQueryAuthority)
      ))

}
