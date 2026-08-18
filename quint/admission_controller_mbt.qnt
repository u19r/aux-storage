/// Compact model-based-test boundary for the adaptive admission transition.
///
/// Each Check action supplies one valid controller state and one finite window
/// signal set. The Rust driver applies the real transition implementation and
/// compares this complete observable state record with the model result.
module admission_controller_mbt {
  import admission_controller.* from "admission_controller"

  var mbtLastConfig: Config
  var mbtLastState: ControllerState
  var mbtLastSignals: WindowSignals

  action mbtInit = all {
    mbtLastConfig' = initialConfig,
    mbtLastState' = initialState,
    mbtLastSignals' = quietSignals,
  }

  action MbtCheck = {
    nondet mbtEnabled = Set(true, false).oneOf()
    nondet desiredLimit = Set(2, 3, 4, 6, 8).oneOf()
    nondet state = Set("warmup", "stable", "probe", "backoff", "recovering", "emergency").oneOf()
    nondet healthyWindows = Set(0, 1, 2, 3).oneOf()
    nondet congestedWindows = Set(0, 1, 2, 3).oneOf()
    nondet nonCongestedWindows = Set(0, 1, 2, 3).oneOf()
    nondet emergencyClearWindows = Set(0, 1, 2, 3).oneOf()
    nondet saturatedHealthyWindows = Set(0, 1, 2, 3).oneOf()
    nondet hasPreviousGoodput = Set(true, false).oneOf()
    nondet probePreviousLimit = Set(0, 1, 2, 3, 4, 6, 8).oneOf()
    nondet saturated = Set(true, false).oneOf()
    nondet pressure = Set(true, false).oneOf()
    nondet enoughSamples = Set(true, false).oneOf()
    nondet gradientHealthy = Set(true, false).oneOf()
    nondet goodputImproved = Set(true, false).oneOf()
    nondet goodputPositive = Set(true, false).oneOf()
    nondet probeDue = Set(true, false).oneOf()
    val config = {
      enabled: mbtEnabled,
      minimum: 2,
      maximum: 8,
    }
    val current = {
      desiredLimit: desiredLimit,
      state: state,
      healthyWindows: healthyWindows,
      congestedWindows: congestedWindows,
      nonCongestedWindows: nonCongestedWindows,
      emergencyClearWindows: emergencyClearWindows,
      saturatedHealthyWindows: saturatedHealthyWindows,
      hasPreviousGoodput: hasPreviousGoodput,
      probePreviousLimit: probePreviousLimit,
    }
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
      mbtLastConfig' = config,
      mbtLastState' = transition(config, current, signals),
      mbtLastSignals' = signals,
    }
  }

  action mbtStep = MbtCheck

  val mbtInv = all {
    mbtLastState.desiredLimit >= mbtLastConfig.minimum,
    mbtLastState.desiredLimit <= mbtLastConfig.maximum,
    mbtLastState.healthyWindows >= 0,
    mbtLastState.congestedWindows >= 0,
    mbtLastState.nonCongestedWindows >= 0,
    mbtLastState.emergencyClearWindows >= 0,
    mbtLastState.saturatedHealthyWindows >= 0,
    mbtLastState.probePreviousLimit >= 0,
    mbtLastState.probePreviousLimit <= mbtLastConfig.maximum,
  }
}
