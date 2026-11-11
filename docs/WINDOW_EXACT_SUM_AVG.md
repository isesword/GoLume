# Exact window Sum and Avg

Floating `RollingSum` and numeric `RollingAvg` now use the same exact
accumulation contract in resident, ordered-state and external-sort execution.
Each frame adds the represented input numbers exactly. Sum rounds that total
to Float64 once; Avg divides the exact total by the non-NULL count before a
single Float64 rounding. Rounding is nearest, ties to even.

This deliberately changes some previous floating results. For example, the
trailing two-row Sum of `[1e16, 1, 1]` ends at `2`, not `1`, and Avg ends at
`1`, not `0.5`. Avg of two `MaxFloat64` values is `MaxFloat64`, not infinity.
A finite Sum may round to infinity, but that output never contaminates the
retained accumulator or subsequent frames when the large values leave.

## Scope and values

- Float32 inputs are promoted exactly. Floating Sum still returns Float64.
- Avg includes integer inputs exactly, without first converting each integer
  to Float64. Its result type remains Float64.
- Integer Sum retains its existing exact integer and output-overflow behavior.
- Ordinary group-by and global Sum/Avg implementations are unchanged.
- NULL inputs are ignored. Empty and all-NULL frames return NULL.
- Any NaN in the current frame produces NaN. Both signs of infinity also
  produce NaN; only one sign produces that infinity. These are valid values
  and count towards Avg's divisor. Payload preservation is not guaranteed.
- Non-finite membership is reversible. Once the exceptional values leave,
  the result comes from the remaining frame, including its finite exact sum.
- Exact zero is positive zero, including cancellation and all-negative-zero
  frames. A negative nonzero average that underflows can round to negative
  zero. Other finite underflow and overflow follow IEEE Float64 rounding.
- Partition changes reset all finite and exceptional state. Chunk boundaries,
  spill/restore boundaries and input permutation within a frame do not change
  a finite result. Frame membership still follows the existing ordering rules.

The accepted frame shapes have not expanded. Resident execution supports its
existing ROWS frames. Ordered/external Sum/Avg still require CURRENT ROW as the
end, with UNBOUNDED PRECEDING, finite PRECEDING or CURRENT ROW as the start.
Finite following-frame support for Count/Min/Max is separate.

## Algorithm and resource bounds

Every finite Float64 is an integer multiple of `2^-1074`. A 34-word unsigned
magnitude plus a sign represents the exact sum in these units. A single input
uses at most 2098 bits; at most `MaxInt64` partition rows need at most 2161
bits, below the 2176-bit capacity. The ordered path rejects partition-position
overflow before updating a rolling accumulator.

Adding or removing a value uses integer carry/borrow operations. Result
conversion extracts the leading numerator bits, divides at most 116 bits by
the integer count, and uses the untouched lower words in the half-ulp decision.
There is no intermediate floating sum, rounded divisor or double rounding.

Each unique ordered exact input/frame admits 320 additional bytes before
allocating its fixed accumulator. Outputs with the same original column, type
and normalized frame share that accumulator and value ring. Sum and Avg still
round independently; an already-rounded Sum is never used to compute Avg. The accumulator never grows with row count,
and add/remove/result performs no heap allocation. Existing finite frame rings
retain copied scalar values; they never borrow input-vector storage across
child reads. Unbounded-prefix frames need no value ring. Partition reset
reuses the state, and Close drops it when releasing the reservation. Resident
execution reuses fixed stack state as the monotonic frame endpoints move,
adding and removing each input at most once per shared frame and partition. It
needs no value ring because input is already materialized. Output vectors remain
separate, including when an output replaces the original input column name.

This stronger numeric contract costs arithmetic work compared with a single
floating add/subtract. It is a correctness change, not a throughput claim.
Benchmark both state updates and complete window streams on the target
workload using `BenchmarkWindowSumAverageState` and
`BenchmarkWindowSumAverageStream`.

## Small-window execution

A frame that can contain at most one row needs no arithmetic accumulator. Its
valid input is the exact sum and average; integer Avg converts the exact integer
to Float64 once. Floating negative zero is explicitly normalized to positive
zero, while NULL stays NULL and NaN/infinity stay valid. Ordered width-one
execution allocates neither an accumulator nor a value ring. Resident execution
also handles clipped, past/future singleton frames without either structure.
This does not add new ordered-frame capabilities.

Immutable source plans group compatible outputs, but each opened stream owns
its mutable state. Input vectors and output pointers are bound once per batch,
only from the original input, and cleared on every return before another child
read. Partition resets advance/reset each shared state once. Execution-state
objects, pointer slots, scalar rings, fixed accumulators and resident target
metadata are admitted before their storage is allocated. These more explicit
charges can reject a very tight budget that previously undercounted descriptors.

The optimization does not approximate, periodically recompute rounded totals,
or use a floating result as retained state. Wider frames retain the exact
accumulator and its existing rounding contract.

## Verification

The independent test oracle enumerates each frame using `math/big.Rat`, without
calling the production frame-range or accumulation helpers. Coverage includes
all three paths, resident frame shapes, batch boundaries, NULL partitions,
Float32, exact 64-bit integer averages, exceptional-value removal, signed zero,
rounding ties, subnormal/normal/overflow boundaries and maximum count divisors.
Random reversible updates compare exact results with the rational oracle.
An external-sort test requires actual spill bytes and checks restored input
order and full resource cleanup. Admission failure, canceled open, repeated
Close, fixed-state size and allocation-free arithmetic have focused tests.
The small-window matrix checks widths 1/2/4/8/16/32/64 with Sum, Avg and both,
including duplicate outputs, canonical equivalent frames, null-only partitions,
alias replacement, poisoned borrowed buffers, separate interleaved streams,
integer singleton rounding and actual spill. Every benchmark result is checked
against an independently enumerated rational oracle.

Use `BenchmarkWindowExactSmallMatrix` for complete query setup/open/read/close
and allocation comparisons, and `BenchmarkWindowExactSmallRepresentative` plus
`BenchmarkWindowExactSpecial` for NULL, mixed-scale, multipartition and spill
workloads. Oracle construction is outside timing; equal result capture and
validation work is included on both sides. Fixture size is 4096 rows with
1024-row input batches. Compare the identical benchmark file against the
previous exact baseline rather than treating earlier approximate sums as the
same numerical contract.

Run `go test ./physical ./lazy -run WindowExact` for this suite and
`scripts/validate.sh` for repository gates. Report unavailable original fixtures
as validation limits; do not replace them or count affected tests as passing.

## Measured small-window comparison

Compared with exact baseline `cb36600a68c69dd307c2c438eb8b85ab056c287a`,
on Linux/amd64, Go 1.27.3, AMD EPYC 9V74, scalar kernels, a fixed CPU
affinity and one Go benchmark thread. Eight baseline/candidate runs alternated order, with 200 ms
per case. These are synthetic 4096-row queries, not a universal throughput
claim or an exclusively reserved host. Medians for same-column Sum + Avg:

| Width | Resident baseline / optimized | Ordered baseline / optimized |
| --- | --- | --- |
| 1 | 3.151 / 1.723 ms | 2.567 / 1.556 ms |
| 2 | 2.886 / 1.841 ms | 2.547 / 1.771 ms |
| 4 | 2.878 / 1.883 ms | 2.430 / 1.809 ms |
| 8 | 2.857 / 1.850 ms | 2.427 / 1.876 ms |
| 16 | 2.993 / 2.053 ms | 2.403 / 1.852 ms |
| 32 | 3.076 / 1.872 ms | 2.429 / 1.894 ms |
| 64 | 2.962 / 1.783 ms | 2.412 / 1.800 ms |

Each combined-output comparison had a per-case benchstat p-value below 0.01.
Some single-output and wider state-core differences were not significant;
there is no claim that every case improves. Width-one state update/output fell
from about 67 ns to 18 ns, with zero allocations per row on both sides. All four
actual-spill workloads retained the same 561076 spilled bytes per query, and
none showed a statistically significant timing change. No multiple-comparison
correction is applied; the full matrix is intentionally available for reruns.

Planning and binding have a small fixed allocation cost. Single-partition
resident Sum + Avg uses 643056 rather than 642136 bytes/query and 250 rather
than 240 allocations/query. The many-partition case reduces allocation count
from 460 to 250 because bindings and output vector descriptors are not rebuilt
for each partition. Ordered width one uses 75968 rather than 76176 bytes/query
and 122 rather than 121 allocations/query. Wider ordered shared frames use
124 rather than 121 allocations/query, while their ring/storage bytes become
smaller from width 8 onward. This is a fixed setup tradeoff, not per-row growth.

Reproduce by compiling the identical small-window benchmark file into both
versions and repeatedly running the following in alternating order on one
pinned CPU. Keep compilation and unrelated CPU workloads outside measured runs.

```sh
go test ./physical -run '^$' \
  -bench '^BenchmarkWindow(ExactSmall|ExactSpecial|SumAverageState)' \
  -benchmem -benchtime=200ms -cpu=1
```

A separate eight-run historical width-one comparison used the unchanged
`BenchmarkWindowSumAverageStream` against pre-exact `b584c1e`. That older harness
reuses source plans and only validates the final result, so its absolute times
must not be mixed with the new complete-query matrix. On its benign input,
resident/ordered medians were 0.864/1.353 ms before exact accumulation and
0.680/0.761 ms after this optimization (21%/44% less time). This is a historical
reference for width one only, not evidence that exact arithmetic has no cost
for wider frames or other workloads.
