/// Nondeterministic bug-hunting harness for admission recovery, probing, and
/// pressure transitions. The imported design model composes arbitrary finite
/// windows so boundary combinations are explored rather than only replaying
/// the named examples.
module admission_controller_bughunt {
  import admission_controller.* from "admission_controller"
}
