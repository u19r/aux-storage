/// FoundationDB GRV cache selection and modeled read-version age contract.
module fdb_grv_staleness {
  pure val DEFAULT_MAX_GRV_LAG_MS = 50

  var latestVersion: int
  var cachedVersion: int
  var cachedAtMs: int
  var nowMs: int
  var maxGrvLagMs: int
  var consistentRead: bool
  var cacheEnabled: bool
  var usedCachedVersion: bool
  var observedVersion: int
  var observedAtMs: int

  action init = all {
    latestVersion' = 2,
    cachedVersion' = 1,
    cachedAtMs' = 980,
    nowMs' = 1000,
    maxGrvLagMs' = DEFAULT_MAX_GRV_LAG_MS,
    consistentRead' = false,
    cacheEnabled' = true,
    usedCachedVersion' = true,
    observedVersion' = 1,
    observedAtMs' = 1000,
  }

  action EventualCachedRead = all {
    latestVersion' = 2,
    cachedVersion' = 1,
    cachedAtMs' = 980,
    nowMs' = 1000,
    maxGrvLagMs' = DEFAULT_MAX_GRV_LAG_MS,
    consistentRead' = false,
    cacheEnabled' = true,
    usedCachedVersion' = true,
    observedVersion' = 1,
    observedAtMs' = 1000,
  }

  action EventualUncachedRead = all {
    latestVersion' = 3,
    cachedVersion' = 1,
    cachedAtMs' = 940,
    nowMs' = 1000,
    maxGrvLagMs' = 0,
    consistentRead' = false,
    cacheEnabled' = false,
    usedCachedVersion' = false,
    observedVersion' = 3,
    observedAtMs' = 1000,
  }

  action ConsistentRead = all {
    latestVersion' = 4,
    cachedVersion' = 2,
    cachedAtMs' = 950,
    nowMs' = 1000,
    maxGrvLagMs' = DEFAULT_MAX_GRV_LAG_MS,
    consistentRead' = true,
    cacheEnabled' = true,
    usedCachedVersion' = false,
    observedVersion' = 4,
    observedAtMs' = 1000,
  }

  action step = any {
    EventualCachedRead,
    EventualUncachedRead,
    ConsistentRead,
  }

  val consistentReadsUseLatest = not(consistentRead) or observedVersion == latestVersion

  val consistentReadsBypassCache = not(consistentRead) or not(usedCachedVersion)

  val disabledCacheUsesLatest = cacheEnabled or observedVersion == latestVersion

  val cacheUseIsAllowed =
    not(usedCachedVersion) or cacheEnabled and not(consistentRead)

  val cachedAgeWithinBudget =
    not(cacheEnabled and not(consistentRead))
      or observedAtMs - cachedAtMs <= maxGrvLagMs

  val observedVersionIsPermitted = observedVersion <= latestVersion

  val inv = all {
    consistentReadsUseLatest,
    consistentReadsBypassCache,
    disabledCacheUsesLatest,
    cacheUseIsAllowed,
    cachedAgeWithinBudget,
    observedVersionIsPermitted,
  }

  run grvExamples = all {
    assert(DEFAULT_MAX_GRV_LAG_MS == 50),
    assert(1000 - 980 <= DEFAULT_MAX_GRV_LAG_MS),
    assert(1000 - 940 > 0),
  }

  val eventual_cached_within_lag_witness =
    not(consistentRead) and cacheEnabled and observedVersion < latestVersion
  val consistent_latest_witness = consistentRead and observedVersion == latestVersion
}
