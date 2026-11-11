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

Each ordered floating Sum or numeric Avg admits 320 additional bytes before
allocating its fixed accumulator. The accumulator never grows with row count,
and add/remove/result performs no heap allocation. Existing finite frame rings
retain copied scalar values; they never borrow input-vector storage across
child reads. Unbounded-prefix frames need no value ring. Partition reset
reuses the state, and Close drops it when releasing the reservation. Resident
execution reuses fixed stack state as the monotonic frame endpoints move,
adding and removing each input at most once per expression and partition. It
needs no value ring because input is already materialized, and retains its
existing output allocations.

This stronger numeric contract costs arithmetic work compared with a single
floating add/subtract. It is a correctness change, not a throughput claim.
Benchmark both state updates and complete window streams on the target
workload using `BenchmarkWindowSumAverageState` and
`BenchmarkWindowSumAverageStream`.

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

Run `go test ./physical ./lazy -run WindowExact` for this suite and
`scripts/validate.sh` for repository gates. Report unavailable original fixtures
as validation limits; do not replace them or count affected tests as passing.
