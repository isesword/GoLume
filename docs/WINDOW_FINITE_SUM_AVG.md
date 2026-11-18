# Finite ROWS Sum and Avg

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`RollingSum` and `RollingAvg` now accept finite PRECEDING, CURRENT ROW and
FOLLOWING endpoints in resident, ordered-state and external-sort execution.
Frames are inclusive, partition-local and clipped at partition boundaries.
The start must not be after the end. This includes entirely past, crossing,
and entirely future frames. Input row order and replacement-column ordering
are preserved. No new public function or result type is introduced.

The exact accumulation and final nearest-even rounding contract in
WINDOW_EXACT_SUM_AVG.md is unchanged. Float32 promotes exactly; integer Avg
never passes through Float64 before accumulation. NULL values are ignored;
empty/all-NULL frames yield NULL. NaN and infinities are reversible frame
members. Signed and unsigned integer Sum keep wide intermediate totals and
check output range only at the completed, requested frame, not during seed,
expiry or insertion. No ordinary group/global aggregate is changed.

## State and admission

The existing owned lookahead and past-endpoint delay support the new frames.
A sequence-indexed queue keeps valid cells and expires them by row position,
not by count of non-NULL elements. Missing future partition rows may advance
many positions in one step. Each expired value is removed from the exact
accumulator, including special float values. Partition boundaries reset state.
Input batches retained across child reads are cloned and budgeted; no borrowed
numeric or partition-key buffer is retained.

Each finite Sum/Avg output owns its state, even when another output has the same
input/frame. This new path does not claim accumulator sharing. The earlier
trailing and resident sharing paths remain unchanged. The fixed descriptor,
value ring, delay ring, partition keys, output buffers and lookahead clones
are admitted through the existing memory manager. Floating Sum and all Avg
admit an additional 320-byte exact accumulator before allocation. Integer Sum
admits a 256-byte envelope for big.Int limbs/growth overlap/temporaries: at most
MaxInt64 cells of at most 64 magnitude bits need at most 127 magnitude bits. These are conservative query admission charges,
not a process RSS limit.
Derived exact plans are rebuilt from the remaining expressions, with a
1-KiB-per-original-slot admission envelope for descriptors, index slices and
temporary grouping storage. This prevents moved outputs or canonical zero-bound
spellings from retaining an obsolete shared leader. Original plans are immutable.

Frame capacity and byte arithmetic are checked; large legal offsets may fail
admission, including on empty input. There is no unlimited resident fallback.
Cancellation and early/error close release state and lookahead reservations.
External mode still requires finite memory and spill quotas. A budgeted mode
alone is not evidence that any spill occurred.

## Scope and validation

This does not add unbounded-following, unbounded-preceding-to-following,
RANGE/GROUPS, calendar/timezone windows or a new rounding policy. It completes
one window substage, not the four-part original roadmap item 18. No SIMD
speedup or performance acceptance is claimed.

`lazy/window_finite_sum_test.go` supplies independent math/big oracles across
ten numeric types, NULL and exceptional floats, finite frame shapes,
partitions/batches, aliases and all three strategies. Avg-only cases ensure
Sum overflow does not hide testing of wide integer averages.
`physical/window_finite_sum_test.go` checks admitted accumulator sizes,
sequence jumps, reversible exceptional state, retained-plan immutability,
cancellation, every exercised reservation-failure point, borrowed-buffer poisoning,
capacity errors and a real multi-run external spill with cleanup assertions.

Focused tests: `go test ./lazy ./physical -run 'FiniteSum|WindowFiniteSum'`.
Run the same tests with GOEXPERIMENT=simd for the existing requested-SIMD mode;
this implementation adds no custom SIMD kernel.
