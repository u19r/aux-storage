/// Design-level protocol model. This checks abstract transitions and assumptions,
/// not production implementation, provider atomicity, or crash durability.
/// Destination authority and atomic maintenance progress.
///
/// The bg-jobs scheduler supplies a claim for one stable job identity. This
/// model begins at the destination transaction: scope, target incarnation,
/// job binding, fence, revision, domain effect and progress are durable. A
/// pending observation is deliberately ephemeral so takeover and replacement
/// races can occur between observation and commit.
module maintenance_commit {
  var targetIncarnation: int
  var taskIncarnation: int
  var boundJob: str
  var acceptedFence: int
  var revision: int
  var effectCount: int
  var progressEffectCount: int
  var sweepCursor: int
  var pending: bool
  var pendingIncarnation: int
  var pendingJob: str
  var pendingFence: int
  var pendingRevision: int
  var responseLost: bool
  var staleEffects: int
  var lastOutcome: str

  pure def admissionDecision(
    currentTarget: int,
    currentTask: int,
    currentJob: str,
    currentFence: int,
    currentRevision: int,
    candidateTarget: int,
    candidateJob: str,
    candidateFence: int,
    expectedRevision: int
  ): str =
    if (candidateTarget != currentTarget or currentTask != currentTarget) "superseded_target"
    else if (candidateJob != currentJob) "binding_mismatch"
    else if (candidateFence < currentFence) "superseded_fence"
    else if (expectedRevision != currentRevision) "native_conflict"
    else "evaluate"

  action init = all {
    targetIncarnation' = 1,
    taskIncarnation' = 1,
    boundJob' = "job-a",
    acceptedFence' = 1,
    revision' = 0,
    effectCount' = 0,
    progressEffectCount' = 0,
    sweepCursor' = 0,
    pending' = false,
    pendingIncarnation' = 0,
    pendingJob' = "none",
    pendingFence' = 0,
    pendingRevision' = 0,
    responseLost' = false,
    staleEffects' = 0,
    lastOutcome' = "initial",
  }

  action ObserveCurrent = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence,
    revision' = revision,
    effectCount' = effectCount,
    progressEffectCount' = progressEffectCount,
    sweepCursor' = sweepCursor,
    pending' = true,
    pendingIncarnation' = targetIncarnation,
    pendingJob' = boundJob,
    pendingFence' = acceptedFence,
    pendingRevision' = revision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "observed",
  }

  action ObserveDifferentJob = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence,
    revision' = revision,
    effectCount' = effectCount,
    progressEffectCount' = progressEffectCount,
    sweepCursor' = sweepCursor,
    pending' = true,
    pendingIncarnation' = targetIncarnation,
    pendingJob' = "job-b",
    pendingFence' = acceptedFence + 10,
    pendingRevision' = revision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "observed_foreign_job",
  }

  action CommitObserved = if (pending) {
    val decision = admissionDecision(
      targetIncarnation,
      taskIncarnation,
      boundJob,
      acceptedFence,
      revision,
      pendingIncarnation,
      pendingJob,
      pendingFence,
      pendingRevision
    )
    if (decision == "evaluate") {
      all {
        targetIncarnation' = targetIncarnation,
        taskIncarnation' = taskIncarnation,
        boundJob' = boundJob,
        acceptedFence' = if (pendingFence > acceptedFence) pendingFence else acceptedFence,
        revision' = revision + 1,
        effectCount' = effectCount + 1,
        progressEffectCount' = progressEffectCount + 1,
        sweepCursor' = sweepCursor + 1,
        pending' = false,
        pendingIncarnation' = pendingIncarnation,
        pendingJob' = pendingJob,
        pendingFence' = pendingFence,
        pendingRevision' = pendingRevision,
        responseLost' = false,
        staleEffects' = staleEffects,
        lastOutcome' = "applied",
      }
    } else {
      all {
        targetIncarnation' = targetIncarnation,
        taskIncarnation' = taskIncarnation,
        boundJob' = boundJob,
        acceptedFence' = acceptedFence,
        revision' = revision,
        effectCount' = effectCount,
        progressEffectCount' = progressEffectCount,
        sweepCursor' = sweepCursor,
        pending' = false,
        pendingIncarnation' = pendingIncarnation,
        pendingJob' = pendingJob,
        pendingFence' = pendingFence,
        pendingRevision' = pendingRevision,
        responseLost' = false,
        staleEffects' = staleEffects,
        lastOutcome' = decision,
      }
    }
  } else {
    all {
      targetIncarnation' = targetIncarnation,
      taskIncarnation' = taskIncarnation,
      boundJob' = boundJob,
      acceptedFence' = acceptedFence,
      revision' = revision,
      effectCount' = effectCount,
      progressEffectCount' = progressEffectCount,
      sweepCursor' = sweepCursor,
      pending' = pending,
      pendingIncarnation' = pendingIncarnation,
      pendingJob' = pendingJob,
      pendingFence' = pendingFence,
      pendingRevision' = pendingRevision,
      responseLost' = responseLost,
      staleEffects' = staleEffects,
      lastOutcome' = lastOutcome,
    }
  }

  /// A newer claim commits a complete destination step while an older
  /// observation may still be in flight.
  action Takeover = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence + 1,
    revision' = revision + 1,
    effectCount' = effectCount + 1,
    progressEffectCount' = progressEffectCount + 1,
    sweepCursor' = sweepCursor + 1,
    pending' = pending,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "takeover_applied",
  }

  /// Recreating a named resource installs a different destination identity.
  /// Any pending observation remains bound to the old incarnation.
  action Recreate = all {
    targetIncarnation' = targetIncarnation + 1,
    taskIncarnation' = targetIncarnation + 1,
    boundJob' = boundJob,
    acceptedFence' = 1,
    revision' = 0,
    effectCount' = 0,
    progressEffectCount' = 0,
    sweepCursor' = 0,
    pending' = pending,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "recreated",
  }

  /// An authority-only commit accepts a higher fence on an idle task without
  /// inventing a domain effect.
  action AcceptHigherFenceIdle = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence + 1,
    revision' = revision + 1,
    effectCount' = effectCount,
    progressEffectCount' = progressEffectCount,
    sweepCursor' = sweepCursor,
    pending' = pending,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "idle_fence_accepted",
  }

  /// Periodic completion resets only domain cursor state. Binding and the
  /// highest destination fence remain durable.
  action PeriodicEnd = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence,
    revision' = revision + 1,
    effectCount' = effectCount,
    progressEffectCount' = progressEffectCount,
    sweepCursor' = 0,
    pending' = pending,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = false,
    staleEffects' = staleEffects,
    lastOutcome' = "periodic_end",
  }

  /// The transaction commits, but its response is lost. Durable state is not
  /// rolled back and retry must reconcile from the advanced revision.
  action CommitAndLoseAck = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence,
    revision' = revision + 1,
    effectCount' = effectCount + 1,
    progressEffectCount' = progressEffectCount + 1,
    sweepCursor' = sweepCursor + 1,
    pending' = false,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = true,
    staleEffects' = staleEffects,
    lastOutcome' = "ack_lost",
  }

  action ReconcileLostAck = if (responseLost) {
    all {
      targetIncarnation' = targetIncarnation,
      taskIncarnation' = taskIncarnation,
      boundJob' = boundJob,
      acceptedFence' = acceptedFence,
      revision' = revision,
      effectCount' = effectCount,
      progressEffectCount' = progressEffectCount,
      sweepCursor' = sweepCursor,
      pending' = false,
      pendingIncarnation' = pendingIncarnation,
      pendingJob' = pendingJob,
      pendingFence' = pendingFence,
      pendingRevision' = pendingRevision,
      responseLost' = false,
      staleEffects' = staleEffects,
      lastOutcome' = "ack_reconciled",
    }
  } else {
    all {
      targetIncarnation' = targetIncarnation,
      taskIncarnation' = taskIncarnation,
      boundJob' = boundJob,
      acceptedFence' = acceptedFence,
      revision' = revision,
      effectCount' = effectCount,
      progressEffectCount' = progressEffectCount,
      sweepCursor' = sweepCursor,
      pending' = pending,
      pendingIncarnation' = pendingIncarnation,
      pendingJob' = pendingJob,
      pendingFence' = pendingFence,
      pendingRevision' = pendingRevision,
      responseLost' = responseLost,
      staleEffects' = staleEffects,
      lastOutcome' = lastOutcome,
    }
  }

  action step = any {
    ObserveCurrent,
    ObserveDifferentJob,
    CommitObserved,
    Takeover,
    Recreate,
    AcceptHigherFenceIdle,
    PeriodicEnd,
    CommitAndLoseAck,
    ReconcileLostAck,
  }

  val atomic_progress = effectCount == progressEffectCount
  val current_target_only = taskIncarnation == targetIncarnation and staleEffects == 0
  val authority_is_durable = acceptedFence > 0 and revision >= 0
  val inv = all { atomic_progress, current_target_only, authority_is_durable }

  val successful_commit_witness = lastOutcome == "applied" and effectCount > 0
  val takeover_witness = lastOutcome == "superseded_fence" and acceptedFence > pendingFence
  val replacement_witness = lastOutcome == "superseded_target" and effectCount == 0
  val lost_ack_witness = lastOutcome == "ack_reconciled" and effectCount > 0
  val periodic_end_witness = lastOutcome == "periodic_end" and acceptedFence > 1

  /// Mutation control: applying a stale page outside the destination
  /// transaction tears effect/progress and records a stale effect.
  action UnsafeStaleEffectCommit = all {
    targetIncarnation' = targetIncarnation,
    taskIncarnation' = taskIncarnation,
    boundJob' = boundJob,
    acceptedFence' = acceptedFence,
    revision' = revision + 1,
    effectCount' = effectCount + 1,
    progressEffectCount' = progressEffectCount,
    sweepCursor' = sweepCursor + 1,
    pending' = false,
    pendingIncarnation' = pendingIncarnation,
    pendingJob' = pendingJob,
    pendingFence' = pendingFence,
    pendingRevision' = pendingRevision,
    responseLost' = false,
    staleEffects' = staleEffects + 1,
    lastOutcome' = "unsafe_stale_commit",
  }

  action unsafeStaleEffectStep = UnsafeStaleEffectCommit

  run admissionExamples = all {
    assert(admissionDecision(1, 1, "job-a", 2, 5, 1, "job-a", 2, 5) == "evaluate"),
    assert(admissionDecision(1, 1, "job-a", 2, 5, 1, "job-a", 3, 5) == "evaluate"),
    assert(admissionDecision(1, 1, "job-a", 2, 5, 1, "job-a", 1, 5) == "superseded_fence"),
    assert(admissionDecision(1, 1, "job-a", 2, 5, 1, "job-b", 99, 5) == "binding_mismatch"),
    assert(admissionDecision(1, 1, "job-a", 2, 5, 2, "job-a", 3, 5) == "superseded_target"),
    assert(admissionDecision(1, 1, "job-a", 2, 5, 1, "job-a", 3, 4) == "native_conflict"),
  }
}
