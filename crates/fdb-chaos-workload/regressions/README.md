# FDB Chaos Regressions

This directory records fixed FoundationDB simulation seeds that should stay reproducible.
Keep entries small and command-oriented: workload, profile, seed, buggify mode, bug area,
the artifact that exposed the issue, and the command to rerun it.

When a failure has a useful minimized history, keep the minimization report path in the
entry. Large raw artifacts stay under `run-artifacts/fdb-chaos/`; this directory is the
stable index for future regression replay.
