/// Model for key-ordered stream partition splitting.
///
/// The model intentionally keeps the hash space tiny. The real implementation
/// uses u64 hash ranges, but the safety questions are the same:
///   - open partitions cover every hash exactly once
///   - a split replaces one writable parent with two writable children
///   - read fan-in keeps the sealed parent available across the split boundary
///   - autosplit only applies when pressure is sustained and routing keys are diverse
module stream_partition_split {
  pure val HASH_SPACE_END = 16
  pure val HASHES = Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
  pure val PARTITION_IDS = Set(0, 1, 2, 3, 4, 5, 6, 7)
  pure val NONE = -1
  pure val SPLIT_THRESHOLD = 13
  pure val HIGH_STREAK_TARGET = 3
  pure val MIN_ROUTING_BUCKETS_TO_SPLIT = 2
  pure val INITIAL_OPEN_COUNT = 2
  pure val PRESSURES = Set(0, SPLIT_THRESHOLD, 20)
  pure val ROUTING_BUCKET_COUNTS = Set(0, 1, 2, 3)

  var family: Family
  var pressure: int
  var routingBucketCount: int
  var cooldownTicks: int
  var splitCount: int

  type Partition = {
    id: int,
    slot: int,
    state: str,
    openedAfter: int,
    sealedAfter: int,
    start: int,
    end: int,
  }

  type Controller = {
    pressure: int,
    highStreak: int,
    lowStreak: int,
  }

  type Config = {
    autoscaleEnabled: bool,
    freeze: bool,
    maxOpen: int,
    cooldownActive: bool,
    epoch: int,
    controller: Controller,
  }

  type Family = {
    partitions: int -> Partition,
    liveIds: Set[int],
    nextId: int,
    nextSlot: int,
    config: Config,
  }

  pure def closedPartition(id) = {
    id: id,
    slot: id,
    state: "retired",
    openedAfter: NONE,
    sealedAfter: NONE,
    start: 0,
    end: 0,
  }

  pure def openPartition(id, slot, start, end, openedAfter) = {
    id: id,
    slot: slot,
    state: "open",
    openedAfter: openedAfter,
    sealedAfter: NONE,
    start: start,
    end: end,
  }

  pure def baseConfig = {
    autoscaleEnabled: true,
    freeze: false,
    maxOpen: 8,
    cooldownActive: false,
    epoch: 0,
    controller: {
      pressure: 0,
      highStreak: 0,
      lowStreak: 0,
    },
  }

  pure def baseFamily = {
    partitions: Map(
      0 -> openPartition(0, 0, 0, 8, NONE),
      1 -> openPartition(1, 1, 8, HASH_SPACE_END, NONE),
      2 -> closedPartition(2),
      3 -> closedPartition(3),
      4 -> closedPartition(4),
      5 -> closedPartition(5),
      6 -> closedPartition(6),
      7 -> closedPartition(7),
    ),
    liveIds: Set(0, 1),
    nextId: 2,
    nextSlot: 2,
    config: baseConfig,
  }

  pure def isOpen(partition) =
    partition.state == "open"

  pure def isReadable(partition) =
    partition.state != "retired"

  pure def containsHash(partition, hash) =
    partition.start <= hash and hash < partition.end

  pure def openIds(family) =
    family.liveIds.filter(id => isOpen(family.partitions.get(id)))

  pure def readableIds(family) =
    family.liveIds.filter(id => isReadable(family.partitions.get(id)))

  pure def openCount(family) =
    openIds(family).size()

  pure def openOwners(family, hash) =
    openIds(family).filter(id => containsHash(family.partitions.get(id), hash))

  pure def routeKey(family, hash) =
    openOwners(family, hash)

  pure def hashCoveredExactlyOnce(family) =
    HASHES.forall(hash => openOwners(family, hash).size() == 1)

  pure def partitionRangeValid(partition) =
    if (isReadable(partition))
      partition.start < partition.end
        and 0 <= partition.start
        and partition.end <= HASH_SPACE_END
    else
      partition.start == partition.end

  pure def partitionFamilyRangesValid(family) =
    family.liveIds.forall(id => partitionRangeValid(family.partitions.get(id)))

  pure def readableOwners(family, hash) =
    readableIds(family).filter(id => containsHash(family.partitions.get(id), hash))

  pure def partitionItemIdRangesDoNotOverlap(left, right) =
    left.id == right.id
      or (
        left.sealedAfter != NONE
        and right.openedAfter != NONE
        and left.sealedAfter <= right.openedAfter
      )
      or (
        right.sealedAfter != NONE
        and left.openedAfter != NONE
        and right.sealedAfter <= left.openedAfter
      )

  pure def itemOrderingPreservedForHash(family, hash) =
    openOwners(family, hash).size() == 1
      and readableOwners(family, hash).forall(leftId =>
        readableOwners(family, hash).forall(rightId =>
          partitionItemIdRangesDoNotOverlap(
            family.partitions.get(leftId),
            family.partitions.get(rightId),
          )
        )
      )

  /// Model constraint: an item key's ordered log is never fanned out across
  /// multiple writable partitions, and readable parent/child intervals are
  /// separated by the split boundary.
  pure def totalOrderingPreservedAtItemLevel(family) =
    HASHES.forall(hash => itemOrderingPreservedForHash(family, hash))

  pure def splitChildren(parent, leftId, rightId, leftSlot, rightSlot, boundary) = {
    pure val midpoint = parent.start + ((parent.end - parent.start) / 2)
    {
      left: openPartition(leftId, leftSlot, parent.start, midpoint, boundary),
      right: openPartition(rightId, rightSlot, midpoint, parent.end, boundary),
    }
  }

  pure def partitionSplittable(partition) =
    isOpen(partition) and partition.end > partition.start + 1

  pure def splitPartition(family, parentId, boundary) = {
    pure val parent = family.partitions.get(parentId)
    pure val children = splitChildren(
      parent,
      family.nextId,
      family.nextId + 1,
      family.nextSlot,
      family.nextSlot + 1,
      boundary,
    )
    pure val closedParent = parent
      .with("state", "write_closed")
      .with("sealedAfter", boundary)
    family
      .with(
        "partitions",
        family.partitions
          .set(parentId, closedParent)
          .set(family.nextId, children.left)
          .set(family.nextId + 1, children.right)
      )
      .with("liveIds", family.liveIds.union(Set(family.nextId, family.nextId + 1)))
      .with("nextId", family.nextId + 2)
      .with("nextSlot", family.nextSlot + 2)
      .with(
        "config",
        family.config
          .with("epoch", family.config.epoch + 1)
          .with("cooldownActive", true)
          .with(
            "controller",
            family.config.controller.with("highStreak", 0).with("lowStreak", 0)
          )
      )
  }

  pure def forwardReadCandidate(partition, token) =
    isReadable(partition)
      and (
        token == NONE
        or partition.sealedAfter == NONE
        or partition.sealedAfter > token
      )

  pure def reverseReadCandidate(partition, token) =
    isReadable(partition)
      and (
        token == NONE
        or partition.openedAfter == NONE
        or partition.openedAfter < token
      )

  pure def forwardReadIds(family, token) =
    readableIds(family).filter(id => forwardReadCandidate(family.partitions.get(id), token))

  pure def reverseReadIds(family, token) =
    readableIds(family).filter(id => reverseReadCandidate(family.partitions.get(id), token))

  pure def controllerHot(config, pressure) =
    config.controller.highStreak >= HIGH_STREAK_TARGET
      and config.controller.pressure >= SPLIT_THRESHOLD
      and pressure >= SPLIT_THRESHOLD

  pure def canAutosplit(config, pressure, openPartitions, routingBucketCount) =
    not(config.freeze)
      and config.autoscaleEnabled
      and not(config.cooldownActive)
      and openPartitions < config.maxOpen
      and controllerHot(config, pressure)
      and routingBucketCount >= MIN_ROUTING_BUCKETS_TO_SPLIT

  pure def hotController = {
    pressure: SPLIT_THRESHOLD,
    highStreak: HIGH_STREAK_TARGET,
    lowStreak: 0,
  }

  pure def hotFamily = baseFamily.with(
    "config",
    baseConfig.with("controller", hotController)
  )

  pure def withControllerAndCooldown(family, controller, cooldownActive) =
    family.with(
      "config",
      family.config
        .with("controller", controller)
        .with("cooldownActive", cooldownActive)
    )

  pure def coldController = {
    pressure: 0,
    highStreak: 0,
    lowStreak: 1,
  }

  def observeLoadState(nextPressure, nextRoutingBucketCount) = {
    pure val nextController =
      if (nextPressure >= SPLIT_THRESHOLD)
        hotController
      else
        coldController
    withControllerAndCooldown(
      family,
      nextController,
      family.config.cooldownActive,
    )
  }

  action init =
    all {
      family' = baseFamily,
      pressure' = 0,
      routingBucketCount' = 0,
      cooldownTicks' = 0,
      splitCount' = 0,
    }

  action observeLoad = {
    nondet nextPressure = PRESSURES.oneOf()
    nondet nextRoutingBucketCount = ROUTING_BUCKET_COUNTS.oneOf()
    all {
      family' = observeLoadState(nextPressure, nextRoutingBucketCount),
      pressure' = nextPressure,
      routingBucketCount' = nextRoutingBucketCount,
      cooldownTicks' = cooldownTicks,
      splitCount' = splitCount,
    }
  }

  action expireCooldown =
    all {
      cooldownTicks > 0,
      family' = withControllerAndCooldown(family, family.config.controller, false),
      pressure' = pressure,
      routingBucketCount' = routingBucketCount,
      cooldownTicks' = 0,
      splitCount' = splitCount,
    }

  action autosplit = {
    nondet parentId = openIds(family)
      .filter(id => partitionSplittable(family.partitions.get(id)))
      .oneOf()
    all {
      canAutosplit(family.config, pressure, openCount(family), routingBucketCount),
      family' = splitPartition(family, parentId, 10 + splitCount),
      pressure' = pressure,
      routingBucketCount' = routingBucketCount,
      cooldownTicks' = 1,
      splitCount' = splitCount + 1,
    }
  }

  action step =
    any {
      observeLoad,
      expireCooldown,
      autosplit,
    }

  val splitCountMatchesOpenCount =
    openCount(family) == INITIAL_OPEN_COUNT + splitCount

  val openCountWithinBounds =
    INITIAL_OPEN_COUNT <= openCount(family)
      and openCount(family) <= family.config.maxOpen

  val cooldownStateConsistent =
    family.config.cooldownActive == (cooldownTicks > 0)

  val cooldownPreventsAutosplit =
    family.config.cooldownActive
      implies not(canAutosplit(family.config, pressure, openCount(family), routingBucketCount))

  val singleRoutingBucketPreventsAutosplit =
    routingBucketCount < MIN_ROUTING_BUCKETS_TO_SPLIT
      implies not(canAutosplit(family.config, pressure, openCount(family), routingBucketCount))

  val streamSplitSafetyInvariant =
    partitionFamilyRangesValid(family)
      and hashCoveredExactlyOnce(family)
      and totalOrderingPreservedAtItemLevel(family)
      and splitCountMatchesOpenCount
      and openCountWithinBounds
      and cooldownStateConsistent
      and cooldownPreventsAutosplit
      and singleRoutingBucketPreventsAutosplit
}
