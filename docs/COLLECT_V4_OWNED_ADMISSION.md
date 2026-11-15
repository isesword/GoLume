# Collect v5: native owned-result admission (unpublished candidate)

This is a new implementation on released ab96126, not the missing earlier
Collect candidate. Historical 3.8–7.1% timings and prior budget claims do not
apply to it.

## Behavior

Collect used to reserve `BatchBytes` and then `CloneBatch`: logical native
payload was charged, but Batch/map/vector headers, capacity rounding and
retained-result bookkeeping were not. The new path estimates native allocation
before cloning, uses logical rows rather than copying unused source tails, and
holds each output reservation until DataFrame.Close. Source streams close on
success and failure; failed partial outputs release their prior reservations.
Copied DataFrame values still share one idempotently closed state.

Native admission reuses the owned fan-out allocation model: 1280 bytes per
native Batch container, 896 per native runtime binding (including list child),
5/4 bulk payload rounding allowance, Fields storage and conservative Struct
append growth. Add 256 bytes per retained result entry to cover the result
slices, their simultaneous old/new growth buffers and reservation bookkeeping.
These deliberately conservative constants follow the existing clone strategy.
They are not measured RSS, a GC heap limit, or a guarantee for every Go runtime.
Source/caller buffers, execution/setup memory outside these clones, GC-delayed
garbage, allocator arenas and hidden substring backing remain outside scope.

## Visible hard-budget change

A single batch with four Int64 values named `x` previously reserved 32 bytes.
It now admits 2473 bytes: native container 1280, binding 896, name 1,
bulk charge 40, result entry 256. Thus a 1024-byte hard limit now rejects that
Collect with the existing typed MemoryOutput error and CollectBatches/sink hint.
This is a compatibility change in resource admission, not a performance win.
A four-value batch named `id` costs one more byte. A 128-batch result costs each
batch's admission separately; no implicit coalescing is claimed.

Budgets in existing success/lifetime tests have been increased only where this
new contract requires it; low-budget rejection and cleanup assertions remain.
The bounded aggregate test retains its 96 KiB spill soft threshold while using
a 512 KiB hard ceiling instead of 256 KiB to also retain admitted result graphs.

The bound can be reduced in a future compact-copy implementation that allocates
only populated maps, or by coalescing tiny batches. Merely reducing these
constants without changing the allocation strategy would weaken the guarantee.
Such work needs separate allocation evidence; this candidate does not claim it.
Publication requires explicit acceptance of this stronger budget contract.

## Opaque compatibility and limits

Opaque keeps the historical Collect validity-only logical charge and full-value
Retain/Release behavior, including Arrow wrappers and non-Arrow reference-counted
values. No Data(), private graph, extension whitelist or source-native size
contract is consulted. A mixed batch still has its native part admitted.
Opaque backing and its wrapper allocations remain outside the native guarantee;
this is not a complete memory bound for mixed or Opaque results. Nil Opaque
entries remain ignored.

Empty Struct children preserve declared-field selection. Repeated dictionary
fields reuse the same empty runtime binding, preventing duplicate large copies.
Nonempty Struct behavior and existing unsupported shapes are unchanged. Valid
acyclic runtime batch graphs are required; new native estimation may reject
malformed hidden native storage before cloning.

## Verification and performance

See the companion validation archive for exact final logs and benchmark protocol.
Benchmark timing must be reported separately from budget accounting. No original
performance acceptance protocol was recovered and no historical result is reused.
ARM64 cross compilation is not native ARM64 validation; local checks are not CI.

## v5 optimization relative to preserved v4

The retained v4 snapshot is unchanged. v5 narrows Collect-only container and
vector descriptor allowances using 64-bit Go 1.27 layout analysis: Batch 144
bytes rounded up to 160 plus seventeen 48-byte map headers each allowed 64,
then rounded to 1280 total; each descriptor receives 128 (largest current
native descriptor 120), with unchanged 512 map-growth and 256 backing slack.
These constants inherit the existing map-growth model, not a universal heap
bound for arbitrary runtime versions or adversarial hash distributions.
Layout regression guards cover Batch and all native descriptors. Fan-out's
4096/1024 allowances are unchanged. Small budgets can still reject outputs
formerly charged by payload only; four Int64 now require 2473, not 32.

A fixed-width-only fast path validates and totals ten numeric types in one
walk; nested, variable-width, Decimal and Opaque use the generic model. Tests
cross-check fast and generic admission. Reservation is invoked in Execute
rather than through an interface helper, avoiding a new manager escape.
No nil-map substitution or representation compatibility change is made.
