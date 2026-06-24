# fdb-chaos-runner

`fdb-chaos-runner` is the command wrapper for aux-storage FoundationDB simulation runs.
It materializes the simulation TOML, records rerun metadata, preserves simulator output,
copies trace files into the run artifact, runs aggregate artifact checkers, and removes
scratch files created by the current run.

## Failure Triage

1. Start from the artifact printed by the failing command.
2. Reproduce with the recorded command:

   ```sh
   just fdb-chaos-rerun run-artifacts/fdb-chaos/smoke/<workload>-seed-<seed>-<run-id>
   ```

3. Inspect `fdbserver-output.log`, `metrics.log`, `run-metadata.json`, and any `*-check.json`
   aggregate reports before changing code.
4. If the failure has supported aggregate-checker artifacts, minimize the client prefixes:

   ```sh
   just fdb-chaos-minimize-history run-artifacts/fdb-chaos/smoke/<workload>-seed-<seed>-<run-id>
   ```

   The command currently supports shared-key histories and background-lease event streams. It writes
   checker-specific minimization reports and minimized artifacts while preserving the original
   anomaly kind/key signatures.

5. Classify the issue in `docs/plans/fdb-chaos.md` under "Bugs Found During Implementation" as a
   product bug, workload-oracle bug, runner bug, command-surface bug, or open simulator validation
   blocker.
6. When fixed, add the seed/profile and minimized artifact notes to
   `crates/fdb-chaos-workload/regressions/fixed-seeds.toml`.

Do not weaken checkers to make a seed pass. Preserve the artifact, make the failure explainable,
and only then decide whether the fix belongs in production code, workload modeling, runner
post-processing, or the command surface.
