# Exact quantile integration validation

The frozen final stage layers configured median/quantile execution over all prior
reviewed fundamentals, including numeric directional fills and variable-width
First/Last. No unreviewed concurrent source is included.

Independent numerical review verified exact rank arithmetic across Float64
probability exponent boundaries and UInt64 count limits, native ordering, 40,000
integer rational interpolation cases and 6,000 floating cancellation cases. It
found an opposite-sign cancellation error near q=.5, corrected with compensated
weight/product/sum arithmetic. Interpolation is not guaranteed correctly rounded.
The pinned 2,738-case Polars 1.35.2 fixture checks all ten numeric types, six modes
and median, including nulls, NaNs, infinities and integer limits. Deliberate
accuracy improvements and output rounding limits are documented.

Independent query review covers shared lanes, all-null groups, mixed ordinary,
moment, distinct and byte-endpoint state, immutable caller configuration,
concurrent execution, stable signed-zero ties, lazy composition and memory-only
execution with a configured spill store. It found and fixed a policy mismatch:
strict bounded quantiles now reject missing spill configuration before source
metadata or data access. Explain no longer advertises an impossible spill path.

full-stage-tests.txt records full scalar/SIMD tests, relevant SIMD race suites,
vet, GoDoc, generated SIMD/registry checks, Linux/arm64 SIMD cross-build, oracle
regeneration, SIMD structure checks and disabled/128/256/512-bit correctness
suites. final-policy-tests.txt repeats full scalar/SIMD, affected race, vet, GoDoc
and ARM64 build after the final policy correction and independent test additions.
query-review.txt is the final independent query race result.

benchmark.txt contains three descriptive samples per mode summarized in
../../docs/EXACT_QUANTILES.md. These shared-host absolute measurements are not a
specialized parallel/SIMD speedup, per-architecture gate or universal regression
bound. ARM64 was compile-only; local gates do not imply remote CI execution.
