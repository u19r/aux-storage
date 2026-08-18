/// FoundationDB owned-key parallel point-read contract.
///
/// The model deliberately abstracts away the C API. Rust ownership and the FDB
/// simulator prove key lifetimes; this model proves the observable attempt and
/// publication rules around that boundary.
module fdb_parallel_reads {
  pure val MISSING = "MISSING"

  type ReadCase = {
    first: str,
    second: str,
    third: str,
  }

  pure def valueFor(key: str): str =
    if (key == "a") "A"
    else if (key == "b") "B"
    else MISSING

  pure def expectedFor(readCase: ReadCase): ReadCase = {
    first: valueFor(readCase.first),
    second: valueFor(readCase.second),
    third: valueFor(readCase.third),
  }

  pure def versionsMatch(versions: List[int], expected: int): bool =
    versions.foldl(true, (matches, version) => matches and version == expected)

  var request: ReadCase
  var publishedResult: ReadCase
  var completionOrder: List[int]
  var attemptReadVersions: List[int]
  var attemptVersion: int
  var failedAttemptVersion: int
  var publishedVersion: int
  var publishedCount: int
  var published: bool
  var attemptFailed: bool
  var retryFailedVersion: int
  var retryReadVersions: List[int]
  var retryPublishedVersion: int

  pure val initialCase = {
    first: "a",
    second: "b",
    third: "c",
  }

  pure val emptyResult = {
    first: MISSING,
    second: MISSING,
    third: MISSING,
  }

  action init = all {
    request' = initialCase,
    publishedResult' = emptyResult,
    completionOrder' = List(),
    attemptReadVersions' = List(),
    attemptVersion' = 0,
    failedAttemptVersion' = 0,
    publishedVersion' = 0,
    publishedCount' = 0,
    published' = false,
    attemptFailed' = false,
    retryFailedVersion' = 0,
    retryReadVersions' = List(),
    retryPublishedVersion' = 0,
  }

  action OutOfOrderCompletion = all {
    request' = { first: "a", second: "b", third: "c" },
    publishedResult' = expectedFor({ first: "a", second: "b", third: "c" }),
    completionOrder' = List(2, 0, 1),
    attemptReadVersions' = List(10, 10, 10),
    attemptVersion' = 10,
    failedAttemptVersion' = failedAttemptVersion,
    publishedVersion' = 10,
    publishedCount' = 3,
    published' = true,
    attemptFailed' = false,
    retryFailedVersion' = retryFailedVersion,
    retryReadVersions' = retryReadVersions,
    retryPublishedVersion' = retryPublishedVersion,
  }

  action DuplicateAndMissing = all {
    request' = { first: "a", second: "a", third: "missing" },
    publishedResult' = expectedFor({ first: "a", second: "a", third: "missing" }),
    completionOrder' = List(1, 2, 0),
    attemptReadVersions' = List(20, 20, 20),
    attemptVersion' = 20,
    failedAttemptVersion' = failedAttemptVersion,
    publishedVersion' = 20,
    publishedCount' = 3,
    published' = true,
    attemptFailed' = false,
    retryFailedVersion' = retryFailedVersion,
    retryReadVersions' = retryReadVersions,
    retryPublishedVersion' = retryPublishedVersion,
  }

  action ReadError = all {
    request' = { first: "a", second: "b", third: "c" },
    publishedResult' = emptyResult,
    completionOrder' = List(1, 0, 2),
    attemptReadVersions' = List(30, 30),
    attemptVersion' = 30,
    failedAttemptVersion' = 30,
    publishedVersion' = 0,
    publishedCount' = 0,
    published' = false,
    attemptFailed' = true,
    retryFailedVersion' = retryFailedVersion,
    retryReadVersions' = retryReadVersions,
    retryPublishedVersion' = retryPublishedVersion,
  }

  action RetryAfterError = all {
    attemptFailed,
    request' = { first: "a", second: "b", third: "c" },
    publishedResult' = expectedFor({ first: "a", second: "b", third: "c" }),
    completionOrder' = List(2, 1, 0),
    attemptReadVersions' = List(31, 31, 31),
    attemptVersion' = 31,
    failedAttemptVersion' = failedAttemptVersion,
    publishedVersion' = 31,
    publishedCount' = 3,
    published' = true,
    attemptFailed' = false,
    retryFailedVersion' = failedAttemptVersion,
    retryReadVersions' = List(31, 31, 31),
    retryPublishedVersion' = 31,
  }

  action step = any {
    OutOfOrderCompletion,
    DuplicateAndMissing,
    ReadError,
    RetryAfterError,
  }

  val responseOrderPreserved =
    not(published) or publishedResult == expectedFor(request)

  val noPartialResponse =
    not(published) or publishedCount == 3

  val oneVersionPerPublication =
    not(published)
      or publishedVersion == attemptVersion
        and versionsMatch(attemptReadVersions, attemptVersion)

  val retryPublishesOnlyNewAttempt =
    retryPublishedVersion == 0
      or retryPublishedVersion > retryFailedVersion
        and versionsMatch(retryReadVersions, retryPublishedVersion)

  val inv = all {
    responseOrderPreserved,
    noPartialResponse,
    oneVersionPerPublication,
    retryPublishesOnlyNewAttempt,
  }

  run parallelReadExamples = all {
    assert(expectedFor({ first: "a", second: "b", third: "c" }) == {
      first: "A",
      second: "B",
      third: MISSING,
    }),
    assert(expectedFor({ first: "a", second: "a", third: "missing" }).first == "A"),
    assert(expectedFor({ first: "a", second: "a", third: "missing" }).second == "A"),
    assert(expectedFor({ first: "a", second: "a", third: "missing" }).third == MISSING),
  }

  val out_of_order_witness = published and completionOrder == List(2, 0, 1)
  val duplicate_key_witness = request.first == "a" and request.second == "a"
  val missing_value_witness = publishedResult.third == MISSING
  val retry_after_error_witness = published and publishedVersion == 31
}
