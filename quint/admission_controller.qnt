/// Bounded adaptive admission-controller model.
///
/// The model intentionally abstracts provider latency into a finite signal:
/// `gradientHealthy` means the observed latency gradient is at least one and
/// `goodputImproved` means a probe beat the previous saturated goodput.  The
/// Rust controller keeps the real floating-point values; this model checks the
/// boundary decisions, state transitions, and concurrency bounds that must not
/// depend on that representation.
module admission_controller {
  type Config = {
    enabled: bool,
    minimum: int,
    maximum: int,
  }

  type ControllerState = {
    desiredLimit: int,
    state: str,
    healthyWindows: int,
    congestedWindows: int,
    nonCongestedWindows: int,
    emergencyClearWindows: int,
    saturatedHealthyWindows: int,
    hasPreviousGoodput: bool,
    // Zero means that no probe is active. A separate field lets a failed
    // probe restore the limit which existed before the probe started.
    probePreviousLimit: int,
  }

  type WindowSignals = {
    saturated: bool,
    pressure: bool,
    enoughSamples: bool,
    gradientHealthy: bool,
    goodputImproved: bool,
    goodputPositive: bool,
    probeDue: bool,
  }

  pure def maxInt(left, right) = if (left > right) left else right

  pure def minInt(left, right) = if (left < right) left else right

  pure def congested(signals) =
    signals.pressure or (signals.saturated and not(signals.gradientHealthy))

  pure def backoffLimit(config, desiredLimit) =
    maxInt(config.minimum, (desiredLimit * 8) / 10)

  pure def emergencyLimit(config, desiredLimit) =
    maxInt(config.minimum, desiredLimit / 2)

  pure def probeLimit(config, desiredLimit) =
    minInt(config.maximum, desiredLimit + maxInt(1, ((desiredLimit * 5) + 99) / 100))

  pure def normalBackoff(config, state) =
    state
      .with("desiredLimit", backoffLimit(config, state.desiredLimit))
      .with("state", "backoff")
      .with("nonCongestedWindows", 0)
      .with("congestedWindows", 0)
      .with("probePreviousLimit", 0)

  pure def transition(config, current, signals) =
    if (not(config.enabled)) {
      current
        .with("desiredLimit", config.maximum)
        .with("state", "stable")
        .with("probePreviousLimit", 0)
    } else if (signals.pressure) {
      current
        .with("desiredLimit", emergencyLimit(config, current.desiredLimit))
        .with("state", "emergency")
        .with("emergencyClearWindows", 0)
        .with("congestedWindows", 0)
        .with("probePreviousLimit", 0)
    } else {
      val isCongested = congested(signals)
      val nextCongestedWindows = if (isCongested) current.congestedWindows + 1 else 0
      val nextState = current.with("congestedWindows", nextCongestedWindows)
      val sustainedCongestion = nextCongestedWindows >= 2
      if (nextState.state == "warmup") {
        if (isCongested) {
          if (sustainedCongestion) normalBackoff(config, nextState).with("healthyWindows", 0)
          else nextState.with("healthyWindows", 0)
        } else if (not(signals.enoughSamples)) {
          nextState.with("healthyWindows", 0)
        } else {
          val healthyWindows = nextState.healthyWindows + 1
          nextState
            .with("healthyWindows", healthyWindows)
            .with("state", if (healthyWindows >= 2) "stable" else "warmup")
        }
      } else if (nextState.state == "stable") {
        if (sustainedCongestion) {
          normalBackoff(config, nextState).with("saturatedHealthyWindows", 0)
        } else if (isCongested) {
          nextState.with("saturatedHealthyWindows", 0)
        } else if (signals.saturated and signals.enoughSamples and signals.gradientHealthy) {
          val saturatedHealthyWindows = nextState.saturatedHealthyWindows + 1
          if (saturatedHealthyWindows >= 3 and signals.probeDue) {
            nextState
              .with("probePreviousLimit", nextState.desiredLimit)
              .with("desiredLimit", probeLimit(config, nextState.desiredLimit))
              .with("state", "probe")
              .with("saturatedHealthyWindows", saturatedHealthyWindows)
              .with("hasPreviousGoodput", true)
          } else {
            nextState.with("saturatedHealthyWindows", saturatedHealthyWindows)
          }
        } else if (not(signals.saturated)) {
          nextState.with("saturatedHealthyWindows", 0)
        } else {
          nextState
        }
      } else if (nextState.state == "probe") {
        val previousLimit = if (nextState.probePreviousLimit > 0)
          nextState.probePreviousLimit
        else nextState.desiredLimit
        val improved =
          signals.goodputPositive
            and (not(nextState.hasPreviousGoodput) or signals.goodputImproved)
            and signals.enoughSamples
            and signals.gradientHealthy
        if (not(signals.saturated)) {
          nextState.with("state", "stable").with("probePreviousLimit", 0)
        } else if (improved) {
          nextState
            .with("state", "stable")
            .with("hasPreviousGoodput", true)
            .with("probePreviousLimit", 0)
        } else {
          nextState
            .with("desiredLimit", maxInt(previousLimit, config.minimum))
            .with("state", "backoff")
            .with("probePreviousLimit", 0)
        }
      } else if (nextState.state == "backoff") {
        if (sustainedCongestion) {
          normalBackoff(config, nextState)
        } else if (isCongested) {
          nextState.with("nonCongestedWindows", 0)
        } else {
          val nonCongestedWindows = nextState.nonCongestedWindows + 1
          if (nonCongestedWindows >= 2) {
            nextState
              .with("nonCongestedWindows", 0)
              .with("state", "recovering")
          } else {
            nextState.with("nonCongestedWindows", nonCongestedWindows)
          }
        }
      } else if (nextState.state == "emergency") {
        val emergencyClearWindows = nextState.emergencyClearWindows + 1
        nextState
          .with("emergencyClearWindows", emergencyClearWindows)
          .with("state", if (emergencyClearWindows >= 3) "recovering" else "emergency")
      } else {
        // Recovering follows the same safe probe rules as Stable.
        if (sustainedCongestion) {
          normalBackoff(config, nextState).with("saturatedHealthyWindows", 0)
        } else if (isCongested) {
          nextState.with("saturatedHealthyWindows", 0)
        } else if (signals.saturated and signals.enoughSamples and signals.gradientHealthy) {
          val saturatedHealthyWindows = nextState.saturatedHealthyWindows + 1
          if (saturatedHealthyWindows >= 3 and signals.probeDue) {
            nextState
              .with("probePreviousLimit", nextState.desiredLimit)
              .with("desiredLimit", probeLimit(config, nextState.desiredLimit))
              .with("state", "probe")
              .with("saturatedHealthyWindows", saturatedHealthyWindows)
              .with("hasPreviousGoodput", true)
          } else {
            nextState.with("saturatedHealthyWindows", saturatedHealthyWindows)
          }
        } else if (not(signals.saturated)) {
          nextState.with("saturatedHealthyWindows", 0)
        } else {
          nextState
        }
      }
    }

  pure def initialConfig = {
    enabled: true,
    minimum: 2,
    maximum: 8,
  }

  pure def initialState = {
    desiredLimit: 8,
    state: "warmup",
    healthyWindows: 0,
    congestedWindows: 0,
    nonCongestedWindows: 0,
    emergencyClearWindows: 0,
    saturatedHealthyWindows: 0,
    hasPreviousGoodput: false,
    probePreviousLimit: 0,
  }

  pure def quietSignals = {
    saturated: false,
    pressure: false,
    enoughSamples: true,
    gradientHealthy: true,
    goodputImproved: true,
    goodputPositive: true,
    probeDue: true,
  }

  var controller: ControllerState
  var lastSignals: WindowSignals

  action init = all {
    controller' = initialState,
    lastSignals' = quietSignals,
  }

  action Apply = {
    nondet saturated = Set(true, false).oneOf()
    nondet pressure = Set(true, false).oneOf()
    nondet enoughSamples = Set(true, false).oneOf()
    nondet gradientHealthy = Set(true, false).oneOf()
    nondet goodputImproved = Set(true, false).oneOf()
    nondet goodputPositive = Set(true, false).oneOf()
    nondet probeDue = Set(true, false).oneOf()
    val signals = {
      saturated: saturated,
      pressure: pressure,
      enoughSamples: enoughSamples,
      gradientHealthy: gradientHealthy,
      goodputImproved: goodputImproved,
      goodputPositive: goodputPositive,
      probeDue: probeDue,
    }
    all {
      controller' = transition(initialConfig, controller, signals),
      lastSignals' = signals,
    }
  }

  action step = Apply

  val inv = all {
    controller.desiredLimit >= initialConfig.minimum,
    controller.desiredLimit <= initialConfig.maximum,
    controller.healthyWindows >= 0,
    controller.congestedWindows >= 0,
    controller.nonCongestedWindows >= 0,
    controller.emergencyClearWindows >= 0,
    controller.saturatedHealthyWindows >= 0,
    controller.probePreviousLimit >= 0,
    controller.probePreviousLimit <= initialConfig.maximum,
    controller.state == "warmup"
      or controller.state == "stable"
      or controller.state == "probe"
      or controller.state == "backoff"
      or controller.state == "recovering"
      or controller.state == "emergency",
  }

  val emergency_witness = controller.state == "emergency"
  val backoff_witness = controller.state == "backoff"
  val probe_witness = controller.state == "probe"
  val recovering_witness = controller.state == "recovering"

  run pressureAlwaysCutsToEmergency =
    true.expect(
      transition(initialConfig, initialState, quietSignals.with("pressure", true)).state
        == "emergency"
    )

  run pressureNeverExceedsMinimum =
    true.expect(
      transition(initialConfig, initialState.with("desiredLimit", 2), quietSignals.with("pressure", true)).desiredLimit
        == 2
    )

  run sustainedCongestionBacksOff =
    true.expect(
      transition(
        initialConfig,
        initialState.with("state", "stable").with("congestedWindows", 1),
        quietSignals.with("saturated", true).with("gradientHealthy", false)
      ).state == "backoff"
    )

  run probeRequiresThreeHealthySaturatedWindows =
    true.expect(
      transition(
        initialConfig,
        initialState.with("state", "stable").with("saturatedHealthyWindows", 2),
        quietSignals.with("saturated", true)
      ).state == "probe"
    )

  run probeNeverExceedsConfiguredMaximum =
    true.expect(
      transition(
        initialConfig,
        initialState
          .with("state", "stable")
          .with("desiredLimit", initialConfig.maximum)
          .with("saturatedHealthyWindows", 2),
        quietSignals.with("saturated", true)
      ).desiredLimit == initialConfig.maximum
    )

  val failedProbeResult = transition(
    initialConfig,
    initialState
      .with("state", "probe")
      .with("desiredLimit", 5)
      .with("probePreviousLimit", 4)
      .with("hasPreviousGoodput", true),
    quietSignals
      .with("saturated", true)
      .with("goodputImproved", false)
      .with("goodputPositive", false)
  )

  run failedProbeRestoresPreviousLimit =
    true
      .expect(failedProbeResult.desiredLimit == 4)
      .expect(failedProbeResult.state == "backoff")
      .expect(failedProbeResult.probePreviousLimit == 0)

  run disabledControllerUsesFixedMaximum =
    true.expect(
      transition(initialConfig.with("enabled", false), initialState, quietSignals).desiredLimit
        == initialConfig.maximum
    )

  run zeroGoodputProbeBacksOff =
    true.expect(
      transition(
        initialConfig,
        initialState
          .with("state", "probe")
          .with("desiredLimit", 5)
          .with("probePreviousLimit", 4)
          .with("hasPreviousGoodput", true),
        quietSignals.with("saturated", true).with("goodputPositive", false)
      ).state == "backoff"
    )
}
