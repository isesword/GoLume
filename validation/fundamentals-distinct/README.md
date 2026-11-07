# Exact distinct validation

Tests run on Linux/amd64 AMD EPYC 9V74 with Go1.27.3. The pinned reference script
uses Polars1.35.2. Production output is UInt64 rather than the reference UInt32.

Coverage includes exact equality (NULL, NaN payloads, signed zeros, UInt64
extrema, binary/empty/embedded zeros, changed dictionary codebooks), empty global
input, repeated distinct lane sharing, numeric/statistical mixed output,
filtered original endpoint ordinals, one-pass non-replayable source consumption,
all existing policy routes, real multi-run spill for one hot group and 128 groups,
100KB strings, validity-only opaque COUNT inputs, malformed input vectors and
bitmaps, cancellation, quota/budget failure, explicit Close and natural EOF.

Independent reviews added engine/distinct_review_test.go and
tuplesort/sort_review_test.go. They exposed and drove fixes for malformed input
validation, memory-only spill policy, output overreservation, EOF lifecycle,
failed spill deletion retry and spill-store handle/path/map metadata admission.
The optional metadata admission capability preserves the old SpillManager
interface. Custom stores lacking it cannot serve bounded tuple-sort spills.

`query-race.txt` records SIMD-enabled focused engine/lazy distinct race tests.
The sorter/memory worker independently passed race and vet suites. Broader
frozen-stage checks are recorded by the stage coordinator before publication.

`benchmark.txt` is five 5-iteration samples of 4096 rows/64 groups, one exact-distinct
lane mixed with Sum and Count. Resident uses 4 MiB; spill uses 256 KiB. These are
absolute descriptive timings, not a prior-version speedup comparison. Spill
writes raw ordinary input records as documented in docs/EXACT_DISTINCT.md;
results include filesystem work and can vary. No new SIMD performance gate or
non-amd64 runtime result is claimed.

## Final independent review and isolated gate

The final review additionally found and fixed reservation reentrancy: handle-map
admission now reserves outside the store mutex, rechecks capacity and closed state,
and still completes admission before file creation. Tests exercise synchronous
reclamation, reentrant growth/Close and concurrent writers/deletion. Failed
post-rename durability cleanup retains file, quota and metadata ownership.

The frozen stage passed full scalar/SIMD tests, related SIMD race tests, vet,
GoDoc, generator and kernel-registry checks, a Linux/arm64 SIMD cross-build and
the pinned Polars oracle check. Full logs are in full-stage-tests.txt; the final
additional concurrent metadata regression is covered by final-review-race.txt.
Cross-build is not non-amd64 execution or performance evidence. No remote CI
success is implied by local validation.
