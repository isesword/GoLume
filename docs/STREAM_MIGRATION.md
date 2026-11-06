# Native-stream-only execution migration

This is a breaking API change. There is one source and physical execution
protocol, not a legacy whole-partition engine behind a compatibility wrapper.

For current scope, resource limits and staged follow-up work, see
[CURRENT_STATUS.md](CURRENT_STATUS.md). Historical phase gates and benchmark
reports describe their own checkpoints, not automatic acceptance of this tree.
The old prepared W4 target is accepted as deferred so development can continue;
its unmet measurements are not relabeled as passing, and merge readiness is a
separate decision.

## Result consumers

- `Collect(ctx)` returns an owned `DataFrame`; call `Close` on it.
- `Prepare(ctx)` returns `*PreparedDataFrame`. Each `Execute` owns its result;
  each `ExecuteBatches` owns a separate borrowed stream. Close results/readers
  and the prepared plan according to their lifetimes.
- `CollectBatches(ctx, BatchOptions)` and `SinkBatches` consume borrowed
  `BatchView` values. A view expires on the next `Next` or reader `Close`;
  retaining one does not copy its data. Close a reader when stopping early.
- `SinkParquet` and `SinkIPC` avoid collecting the whole result first. They
  share the native graph and resource policy; choosing a sink alone does not
  make an unsupported intermediate operator bounded.
- `PrepareResult` and `CollectResult` are terminal-Aggregate-only APIs, as
  detailed below. Aggregate followed by row operators uses the other consumers.

`Collect` always retains its complete output. `ExecutionBounded` is not an
out-of-core DataFrame representation or a promise that any plan can finish.

Owned DataFrame execution, borrowed iterators, Analyze and file sinks use the
same query-budget acquisition policy. A `QueryBudget` supplied through
`memory.WithQueryBudget` is honored, including its accounting root and limits,
and remains caller-owned. Execution releases its own resources, not the caller's
reservations or budget; the caller must close the budget after dependent results
and consumers are finished. Without an external budget, execution creates and
closes its own. Closed engines/budgets reject new work. `ExecutionBounded` also
rejects an unlimited external budget rather than silently replacing or ignoring
it; stateful spill requirements still apply to the actual execution budget.

## Source implementers

`source.Source` now includes `NewBatchStream(context.Context, source.ReadOptions)`
in addition to Schema, Stats and Partitions. `source.Reader`,
`source.LegacyReaderSource`, `NewReader`, `ReadPartition`, and their fallback
adapter are removed. Parquet, Arrow record and in-memory sources implement the
native protocol directly. Parquet's typed decoder is private scratch conversion,
not a second partition execution interface.

A stream fills a reusable destination through Next. A successful batch is borrowed
until the next Next or Close. EOF and errors clear output; errors are terminal;
Close is idempotent. Preserve query cancellation, memory reservations and errors
from closing child resources. Copy results explicitly when retaining them.

Only implement PreparedBatchStreamProvider when the source genuinely honors the
exact supplied partition sequence, including order, duplicates and an empty
sequence. The explicit subset-opening API rejects unsupported sources. Global
Sort, TopK, Window, Limit and aggregate output do not advertise this capability.
Pipelines open their complete native streams instead. Eligible resident joins
forward exact probe subsets; global join strategies reject subset execution.

## Join callers

Removed: PreparedJoin, PrepareJoin, CollectJoin, JoinResult, JoinedRow, RowRef,
and ErrPreparedJoinClosed. These exposed partition-relative references from a
separate executor that cannot represent arbitrary native batch boundaries.

Use `left.Join(right, options).Collect(ctx)` for an owned DataFrame; inspect
Height/Column and call Close. For repeat execution, use `.Prepare(ctx)`, call
Execute, close each returned frame, and close the prepared plan. The closed-plan
error is ErrPreparedFrameClosed. All six join semantics now use the same physical
join implementation; no row-reference compatibility API remains.

Removed convenience aliases `Frame`, `Prepared`, `PreparedFrame`, `PrepareFrame`
and `CollectFrame` are replaced by the canonical `DataFrame`, `PreparedResult`,
`PreparedDataFrame`, `Prepare` and `Collect` names.

## Aggregation and prepared execution

engine.Result is a terminal representation of the shared native aggregation
execution, not a separate Reader executor. The former direct/radix/workspace
execution tree and input-rowgroup-sized retained scratch are removed. Prepared
plans retain compilation and bound concurrent executions, not source-sized
workspace arrays. PrepareOptions.MaxConcurrency remains unchanged; the diagnostic
PlanInfo.WorkspacePool is renamed MaxConcurrency. Engine and PreparedQuery public
Prepare/Execute/Close signatures and Result remain unchanged.

`LazyFrame.PrepareResult` and `CollectResult` require a terminal Aggregate.
A row-producing plan or an operation after aggregation, such as Sort, Filter,
Select or Limit, must use Prepare/Collect instead. The result terminal reuses the
exact aggregate query compiled by the same lazy optimizer and physical graph;
it does not collect a DataFrame and infer statistics from diagnostics.

`PreparedResult.Execute(ctx, engine.ExecuteOptions{Materialize: false})` returns
counters, checksum, strategy and elapsed time without retaining output rows.
`Materialize: true`, and the one-shot CollectResult helper, return the actual
group keys and aggregate values in Result.Rows, including SQL NULL validity.
An externally supplied QueryBudget remains owned by the caller.

Concurrent Close calls on a prepared DataFrame or its result terminal all wait
for active executions and the single cleanup pass to finish. Repeated Close
calls return the same cleanup error without closing resources again.

Materialized Result.Rows are deep-copied and charged while being collected. On
successful return they become caller-owned Go values and are no longer charged
to the completed query. DataFrame ownership remains explicit through Close.
Prepared strategies do not silently change when an external QueryBudget is
supplied; an incompatible budget is rejected rather than misreported.

The old input-wide aggregate radix backend is removed. ForceStrategy="radix" is
rejected rather than silently mapped to another strategy. Hints.ForceRadixBits and ForceWorkerCount,
planner.Strategy.RadixBits, planner.Input.ForceRadixBits and RadixCapable are
removed, as are the matching CompiledQuery.ForceRadixBits and RadixCapable
fields. Configure native scan parallelism through engine.Options.Pipeline.Workers,
not the removed aggregate-backend worker hint. Native partitioned hash aggregation
uses engine.Options.Workers (bounded by the scheduler); its ownership workers do
not depend on how many source partitions exist. Actual diagnostics identify direct-fused, bitmap-direct, partitioned-hash, sorted or
budget-driven spill-hash. The standalone radix package and physical join radix
implementation are separate algorithms and remain available.

The direct-fused strategy requires an explicitly supported numeric comparison.
No-filter and constant predicates use bitmap-direct when a serial direct
strategy is selected; forcing direct-fused for an unsupported predicate is an
error rather than a diagnostic
label for a different execution path.

## Continuously charged prepared-state reuse

Repeated engine.PreparedQuery.Execute calls can retain owned fixed-width resident
state and bounded partition/wave scratch in an engine-wide reclaimable cache.
DefaultOptions.PreparedCacheBytes is 64 MiB; zero disables retention, and a finite
engine hard limit also caps idle bytes. Source batches, predicates, output data,
string/dictionary group state, sorted/spill strategies, public BatchStreams,
one-shot Engine.Execute and externally supplied QueryBudgets do not use it.

The root remains charged while buffers are idle. Each checkout admits the whole
capacity into the new query through stable ReusableAccounts; no prior query or
context owns idle memory. Failed attachment destroys that entry before cold
fallback. Admission and root-pressure eviction share one hard cache cap, including
entries in transit. Engine.Memory().Used() may therefore remain positive after a
successful prepared execution, while the completed query's Used() is zero.
PreparedCacheStats exposes idle/credited bytes and hit/miss/eviction counts.

PreparedQuery.Close and Engine.Close wait for accepted work and in-flight eviction
cleanup. Engine.Close rejects new execution with ErrEngineClosed. Result rows
remain caller-owned and survive subsequent resets. See
[prepared memory ownership](PREPARED_MEMORY_REUSE.md).

The shared I32 dense SUM/COUNT kernel uses the same state transitions, preserves
NULL/fallback paths and existing bounded cancellation checks, and does not
restore a separate executor. The historical diagnostic combined W4 median
13.338 ms remains above the old 10.04 ms reference and its 3% limit (10.341 ms).
That target is now deferred, not passed; the controlled improvement is not full
performance acceptance. See
[the measured evidence](../validation/charged-reuse/README.md).

## Streaming Analyze and partial failure reports

Implemented in the current worktree: `Analyze` prepares once and drains one
native execution through the same stream opener as
`PreparedDataFrame.ExecuteBatches`. It does not collect a DataFrame or retain
output batches. Blocking operators still require their ordinary state and obey
the same resource/spill policy; diagnostic draining does not make them stateless.

The report API intentionally changes:

- `AnalyzeReport.MaterializedRows` is removed without a compatibility shim.
  `OutputRows` counts successfully observed output rows, not all input work.
- `OutputBatches` counts nonempty output batches, not zero-row progress calls.
- `FirstBatchDuration` starts immediately before opening the execution stream
  and ends at the first nonempty output batch. It is zero when none was observed,
  including empty results or failure before the first nonempty batch.
- `ExecuteDuration` starts at that same boundary and includes stream opening,
  draining, and top-level stream/budget/prepared-plan cleanup. Preparation is
  measured separately in `PrepareDuration`.
- `SelectedRows`, `GroupCount` and `Strategy` summarize only a terminal Aggregate.
  Nested aggregate diagnostics remain on their own `ExecuteOperators` entries.

`Analyze` returns the available report alongside any error. `ExplainAnalyze`
likewise returns complete or partial text even on failure. Plans and operator
snapshots describe stages reached; counts on a failed execution are partial,
not a completed-query result. Final snapshots are captured after cleanup and
scale with plan size, not output row count. Callers must check the returned error.

Failure fields follow the public operation boundaries:

- `Error` is the error from preparation, stream opening or `Next`.
- `FailurePhase` is `prepare` or `execute` when `Error` is non-nil, and empty on
  success or a cleanup-only failure.
- `CleanupError` contains errors from top-level reader/budget/plan `Close`.
  It does not replace `Error`; both remain discoverable with `errors.Is/As` in
  the joined error returned by Analyze.
- `Termination` is `completed`, `failed`, `canceled` (including deadline expiry),
  `budget-exceeded` (memory or spill quota), or `cleanup-failed`. The last value
  applies only when there is no primary `Error` but top-level cleanup fails.

These fields are not global causal attribution. An operator may close a nested
stream during preparation or `Next`, so a nested Close error can appear in
`Error` with `FailurePhase=prepare` or `execute`, not `CleanupError`. Profiled
operator `close_error` attributes identify that origin where available. A
primary failure still determines termination when top-level cleanup also fails.

Allocation deltas are process-wide Go runtime measurements, not exclusive query
or operator allocations. Native `peak_memory_bytes` attributes carry
`peak_memory_scope=query`: they are shared query-budget high-water marks and
must not be summed across operators. A caller-owned budget can include earlier
or concurrent caller usage, so its peak is not necessarily exclusive to this
Analyze invocation either. It is not process RSS.

Unified predicate/projection expression execution remains subsequent work;
`expr.Program` and `expr.ScalarProgram` are still separate.

Validation status: this stage's implementation, independent final review and
full local `scripts/validate.sh` passed. Coverage includes ordinary/SIMD tests,
host-supported forced widths, SIMD-disabled fallback, Linux build, race, vet,
GoDoc, structure/generated-code checks and demo, plus independent focused tests.
See [the review record](../validation/analyze-stream-review.md) and
[full validation log](../validation/analyze-stream-full-validation.txt).
The fixture remains the disclosed synthetic substitute; original-fixture and
GitHub Actions success are not claimed. The old W4 target remains deferred and
unified expressions remain an unfinished separate stage. This is not a release
or merge-readiness claim.

## Native vector representation

Int8, Int16, Uint8, Uint16 and Float32 values use their corresponding native
Batch maps throughout cloning, range views, Arrow export, Struct children and
JSON-decoded Struct output. Callers that read these logical fields from widened
Int32s/Uint32s/Float64s maps must use the native maps instead; no widened fallback
is retained. Internal scalar registers may still use a wider computation type.

## Hash table ownership

The hashagg WithOptions constructors now return a table and an error.
TableOptions.Reserve is now `func(int64) (memory.Reservation, error)`, not a
retained-byte delta callback. Tables own their slot/state/null-state/string-arena reservations and
must be closed. Allocation failures during construction release earlier tokens;
growth reserves the complete replacement while the old component remains live,
then releases the old component after replacement. Do not also retain/release
the same tokens in an external workspace list. This replaces the previous
under-counted new-minus-old growth accounting.

## Predicate workspace ownership

Program.WorkspaceBytes(rows) returns a checked fresh-workspace capacity estimate
and an error on invalid or overflowing sizes. The callback supplied to
EvalWorkspace.SetReserveHook now returns `(memory.Reservation, error)` for each
dictionary-cache allocation. The cache owns
that token, reserves a complete replacement before allocating, and releases the
old token after replacement. Call EvalWorkspace.Release when replacing or closing
a workspace. Engine streams own their static workspace reservation separately;
cache tokens must not also be released by an external reservation list.

## Shared filter and predicate ownership

Engine resident, sorted and spill streams delegate predicate reservations to
`expr.PredicateWorkspace`, also used by canonical `physical.FilterSource`.
Static workspace, dynamic dictionary cache, selection indices and retained
compaction capacities are charged before allocation, including old/new overlap.
The opening query owns the stream: a plain context preserves that owner, the
same explicit owner is accepted, and explicit rebinding is rejected before
child work. Output remains borrowed until the next Next or Close.

Bounded filtering supports contiguous Arrow slices with explicit reference and
validity ownership. Discontiguous opaque gathering without a truthful sizing
contract returns ResourceError before allocation. All-selected borrowing and
explicitly unbounded direct-stream gathering retain their documented semantics.
This is a deliberate bounded-support restriction, not a claim that arbitrary
opaque allocations are budgeted. See [Filter workspace details](FILTER_WORKSPACE.md).

The experimental producer-side Filter acceleration is not enabled in this WIP
checkpoint; shared ownership fixes are independent of that rejected optimization.

## Resource policy

`engine.DefaultOptions()` has no finite memory hard cap (`MemoryLimitBytes=0`)
and disables Spill. Configure a finite memory limit for `ExecutionBounded`,
and a positive `Spill.MaxBytes` when the selected stateful strategy requires
Spill. Unsupported bounded plans fail explicitly. `ExecutionMemoryOnly` disables
Spill; `ExecutionAuto` selects strategies within the same native graph.

Managed reservation is not a process RSS guarantee. `Collect`, caller-retained
Result rows, immutable source data and decoder/runtime overhead have different
ownership and accounting lifetimes; none may be treated as free memory merely
because a completed query reports zero usage.

ReadOptions.BatchRows and BatchBytes govern managed row-scaled decoded buffers
and scratch. Arrow/in-memory input buffers are immutable borrowed source data;
new offset/validity slice scratch is reserved, not the caller's pre-existing
values charged again.

ReadOptions.BatchPayloadTargetBytes is a soft output-payload target and
BatchPayloadBytes is a separate hard output-payload limit. Zero disables that
additional limit. A single row may exceed the soft target if it still fits the
hard limit. These controls do not change BatchBytes, which governs managed
decoding/range scratch. Native immutable sources size real variable-length
ranges, not just average schema widths. Projection evaluates bounded input
ranges and reduces a candidate before retrying a payload/budget failure; a
single oversized row fails explicitly.

Parallel scan now publishes a borrowed whole-batch lease and waits for consumer
acknowledgment before advancing or closing its source. The consumer may divide
that lease into ByteSizer morsels. In-flight accounting includes the complete
published batch plus its largest live range metadata, while new range metadata
receives its own allocation reservation. Cancellation cannot revoke a batch
still being read. Fan-out retains owned copies because independent branches
have different lifetimes. External source implementations must honor supplied
payload limits; oversized handoff batches fail rather than silently exceeding
per-worker capacity.

ReadOptions.MaxCompressedPageBytes and MaxUncompressedPageBytes separately cap
compressed/uncompressed Parquet page-body sizes declared by page headers,
before page-body allocation. Both default to 8 MiB;
negative limits are invalid. A valid file with larger pages now fails unless the
caller explicitly supplies larger limits appropriate for trusted data. Projection
avoids opening excluded oversized columns. These are per-page limits, not a
shared query/RSS quota across columns and codecs.

Parquet string cursors read bounded batches from one Arrow page, reserve cursor
headers and copied payload before allocation, then admit common row spans across
columns. Page-backed slices are consumed before advancing their readers. For a
large but individually admissible row crowded out by lookahead headers, string
columns can restart from the already-emitted row with minimal inline lookahead.
That exceptional reread is bounded by cancellation checkpoints and keeps no
whole-rowgroup buffer. Subsequent normal rows can recover batch capacity.

Arrow's allocator methods return byte slices without an error channel. Query
quota failures therefore cannot safely be injected as recoverable allocation
errors through that interface. No panic-based quota adapter or false RSS bound
is introduced. Decoder pages/dictionaries, codecs, fixed metadata, runtime and
GC retention remain distinct from managed reservations. Page guards improve
admission but do not prove an aggregate process-memory cap.
