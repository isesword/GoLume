# Explicit exact Decimal AVG finalizer (N19 kernel)

This is a low-level `aggregate.DecimalSumState` finalizer, not an expansion of
Engine or lazy aggregate eligibility. Existing `Avg`, SUM, grouped output and
`ExecuteDecimal` contracts are unchanged. Native Decimal query AVG remains
unsupported until its configured descriptor and execution path are integrated.

`FinalizeAverage(DecimalAverageOptions)` takes an explicit `InputScale`, target
`Precision`, target `Scale`, and `Mode`. InputScale must be 0..38; target precision
1..38; target scale 0..precision. Six modes are available: toward zero, away from
zero, floor, ceil, half even, half away from zero. Mode zero is invalid, and all
configuration is validated before empty-state handling. There is no implicit
input-scale output or default rounding policy.

For coefficient sum S, valid-value count C, source scale a and target scale b,
the exact target coefficient is S * 10^(b-a) / C. One full quotient/remainder
rounding operation is performed, then target precision is checked. NULLs must
not be added; C=0 returns NULL, while a nonempty sum that cancels to zero returns
a valid zero. The input coefficients and immutable per-lane scale must be
validated by the owner. State carries no scale and cannot detect merges from
incompatible units. Add and Merge retain their existing MaxInt64 count limit.

The sum is not first narrowed through SUM.Finalize: two copies of the maximum
precision38 value have an overflowing SUM but a legal AVG equal to that value.
Do not average finalized partition averages. Merge coefficient states first,
then finalize once; unequal partition sizes and intermediate rounding otherwise
change the answer. Finalization is read-only, including error paths. Invalid
configuration and final precision overflow are distinct errors.

Legal state magnitude is below 2^190, the scaled numerator below 2^317, and the
scaled denominator below 2^190. Fixed 384-bit unsigned magnitudes are sufficient;
production uses no floating-point or per-value big.Int arithmetic. State layout,
existing state budgets and SUM semantics do not change. The fixed temporary
arithmetic is bounded stack work, not an end-to-end query heap/RSS guarantee or
instruction-level cancellation promise.

The aggregate layer has its own strongly typed rounding vocabulary because expr
already depends on aggregate. Future configured query binding should explicitly
map mode cases and retain all metadata. Existing expr round/divide behavior is
not changed by this kernel. No new ordering, spill, parallel, nested or window
capability is implied. Full-repository validation remains subject to the existing
protected-file gate; scoped validation cannot be described as a full pass.
