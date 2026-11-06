# Native-stream-only execution migration

This is a breaking API change. There is one source and physical execution
protocol, not a legacy whole-partition engine behind a compatibility wrapper.

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
