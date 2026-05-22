/// Bug-hunting wrapper for shared logical backfill.
///
/// Run with:
/// quint run quint/logical_backfill_bughunt.qnt --invariant=inv --max-samples=5000 --max-steps=16
module logical_backfill_bughunt {
  import logical_backfill.* from "./logical_backfill"

  action bughuntStep = step
}
