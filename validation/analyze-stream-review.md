# Streaming Analyze and branch closeout validation

Base: `2c2162cc303429d07e723e9afc94f3e3c457717f`.

## Implemented contract

Analyze now drains one native ExecuteBatches stream instead of collecting a
DataFrame. OutputRows and OutputBatches count successfully observed nonempty
outputs. FirstBatchDuration begins when opening execution and ends at the first
nonempty batch; zero means none observed. ExecuteDuration includes cleanup.
MaterializedRows is removed. ExplainAnalyze returns partial text with an error.

Reports retain reached plans, completed operator snapshots, termination and
original execution/preparation and cleanup errors. Error classification follows
public Next/Close boundaries: nested operators can return cleanup errors from
Next. Returned errors preserve their causes with errors.Is/As. Diagnostic
attributes remain source/operator-provided; they are not automatically additive
or exclusive measurements. Native peak_memory_bytes is marked
peak_memory_scope=query; allocation deltas are process-wide Go runtime counters.

Preparation has one closer owner across partially built source graphs. Join
build and Top-K replay propagate reader Close failures, and budget fallback does
not swallow a cleanup error. Shared DataFrame/batch execution adopts an external
QueryBudget without closing it; otherwise it owns the created budget. Bounded
policies reject unlimited external budgets. Session and frame overrides agree.

## Verification

- Go 1.27.3 linux/amd64, official archive checksum verified.
- Dependencies downloaded from their configured upstream sources; go mod verify
  passed, go.mod and go.sum unchanged.
- Full scripts/validate.sh: PASS, including ordinary and SIMD tests, disabled
  SIMD fallback, supported forced widths, Linux build, broad race checks, vet,
  GoDoc, generated kernel/structure checks, and lazy-demo.
- Independent focused race and overlay checks: PASS. They cover large output
  without materialization, empty and grouped output, first-batch/deadline behavior,
  partial read failure, Next plus Close errors, preparation cleanup, Top-K replay
  Close failure, external budget ownership/limits, bounded overrides, sink encoder
  finalization, and query-scope peak rendering.
- Two independently reproduced lifecycle/policy findings were fixed before the
  final checks: Top-K replay Close loss and unlimited external-budget admission
  under Bounded execution. Final reviewed source hashes matched tested files.

Raw full-check output: [analyze-stream-full-validation.txt](analyze-stream-full-validation.txt).

## Limits and next stage

The local Parquet fixture is the disclosed synthetic substitute, SHA256
`115f46c063da52e2b3b8c883522e8d29a4559b410772bc08681a87235d783876`.
This is not evidence that the original fixture or GitHub Actions passed.
Workflows and the repository's original fixture are unchanged.
No old W4 acceptance result is rewritten: that performance target is deferred.
No whole-process RSS guarantee or merge approval is implied.

Unified expressions, conditional evaluation and associated optimizer changes
are being implemented separately and are not part of this validated checkpoint.
