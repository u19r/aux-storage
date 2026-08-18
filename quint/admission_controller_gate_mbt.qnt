/// Model-based boundary for the admission gate's permit accounting.
///
/// The adaptive transition is modelled separately. This model keeps the
/// concurrency contract small and explicit: foreground admission is bounded
/// by the configured foreground limit, control work is bounded by its reserve,
/// and completing or dropping a permit returns exactly one slot.
module admission_controller_gate_mbt {
  type Config = {
    foregroundLimit: int,
    controlLimit: int,
  }

  type Gate = {
    inFlight: int,
    controlInFlight: int,
    activePermits: int,
    activeControls: int,
    lastOperation: str,
    lastClass: str,
  }

  var config: Config
  var gate: Gate

  action init = all {
    config' = {
      foregroundLimit: 2,
      controlLimit: 1,
    },
    gate' = {
      inFlight: 0,
      controlInFlight: 0,
      activePermits: 0,
      activeControls: 0,
      lastOperation: "init",
      lastClass: "none",
    },
  }

  pure def transition(current, cfg, operation, class) = if (operation == "acquire") {
    val admitted = current.inFlight < cfg.foregroundLimit
    if (admitted) {
        {
          inFlight: current.inFlight + 1,
          controlInFlight: current.controlInFlight,
          activePermits: current.activePermits + 1,
          activeControls: current.activeControls,
          lastOperation: "admit",
          lastClass: class,
        }
    } else {
        {
          inFlight: current.inFlight,
          controlInFlight: current.controlInFlight,
          activePermits: current.activePermits,
          activeControls: current.activeControls,
          lastOperation: "reject",
          lastClass: class,
        }
    }
  } else if (operation == "complete" or operation == "drop") {
    val hasPermit = current.activePermits > 0
    if (hasPermit) {
      {
        inFlight: current.inFlight - 1,
        controlInFlight: current.controlInFlight,
        activePermits: current.activePermits - 1,
        activeControls: current.activeControls,
        lastOperation: operation,
        lastClass: "none",
      }
    } else {
      current
    }
  } else if (operation == "control_acquire") {
    val admitted = current.controlInFlight < cfg.controlLimit
    if (admitted) {
      {
        inFlight: current.inFlight,
        controlInFlight: current.controlInFlight + 1,
        activePermits: current.activePermits,
        activeControls: current.activeControls + 1,
        lastOperation: "control_admit",
        lastClass: "none",
      }
    } else {
      {
        inFlight: current.inFlight,
        controlInFlight: current.controlInFlight,
        activePermits: current.activePermits,
        activeControls: current.activeControls,
        lastOperation: "control_reject",
        lastClass: "none",
      }
    }
  } else if (operation == "control_drop") {
    val hasPermit = current.activeControls > 0
    if (hasPermit) {
      {
        inFlight: current.inFlight,
        controlInFlight: current.controlInFlight - 1,
        activePermits: current.activePermits,
        activeControls: current.activeControls - 1,
        lastOperation: "control_drop",
        lastClass: "none",
      }
    } else {
      current
    }
  } else {
    current
  }

  action step = {
    nondet operation = Set("acquire", "complete", "drop", "control_acquire", "control_drop").oneOf()
    nondet class = Set("point", "range", "write").oneOf()
    all {
      config' = config,
      gate' = transition(gate, config, operation, class),
    }
  }

  val inv = all {
    gate.inFlight == gate.activePermits,
    gate.controlInFlight == gate.activeControls,
    gate.inFlight >= 0,
    gate.inFlight <= config.foregroundLimit,
    gate.controlInFlight >= 0,
    gate.controlInFlight <= config.controlLimit,
    gate.activePermits >= 0,
    gate.activeControls >= 0,
  }
}
