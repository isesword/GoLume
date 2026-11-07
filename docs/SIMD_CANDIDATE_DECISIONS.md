# SIMD candidate decisions

This file records candidates that are deliberately not promoted to Auto. A
rejected candidate remains a formal scalar implementation and must not be
silently reported as SIMD.

## Existing evidence

- **Packed SQL Boolean AND:** the arm64 operation gate in
  `validation/simd-kernels-arm64.txt` measured forced SIMD slower than the
  scalar word implementation (about +13%). Keep the word-level scalar path.
- **Hash64:** the approved arm64 backend is `arm64-unrolled`; it is a four-way
  native scalar multiply pipeline and reports `HardwareSIMD=false`. It is an
  execution optimization, not SIMD coverage.
- **UTF-8 length:** the long-ASCII candidate improved an isolated input but
  regressed the mixed-Unicode expression workload beyond the 3% end-to-end
  limit. It was removed and must not be restored without a new mixed-input
  gate.
- **Hash128 and checked integer SIMD:** the existing gate records did not admit
  these candidates. They remain scalar until a new implementation passes the
  exact operation and end-to-end gates.
- **Native Int8 comparison:** the native-width `kernel.Set` path is implemented
  and differentially tested, but this tree has no architecture-specific Int8
  implementation or same-build benchmark gate. It therefore remains an honest
  scalar backend; no SIMD capability is reported or inferred from the existing
  Int32/Uint32 results.
- **Native Int16/Uint8/Uint16/Float32 comparison and compaction:** formal
  scalar Set entry points and physical callers are covered by differential and
  tail tests. No native architecture samples exist for these IDs, so they are
  deliberately excluded from Auto and hardware-SIMD reporting.
- **Float32 comparison portable SIMD candidate:** Go 1.27 `simd.Float32s`
  is available in forced `ModeSIMD` only, with tail, NULL, NaN and differential
  coverage plus a benchmark. No ten-sample architecture gate or end-to-end
  evidence exists yet, so Auto remains scalar.
- **Float32 arithmetic portable SIMD:** native float32 scalar arithmetic is
  implemented and tested. A three-sample Apple M4 Pro benchmark showed the
  portable SIMD candidate slower (about 0.71 ms vs 0.51 ms for one million
  rows), so the candidate was removed and the formal scalar backend remains.
- **Float32 column-column comparison:** the predicate program now uses a
  dedicated scalar/forced portable SIMD path and combines both validity masks.
  It is not Auto-admitted yet because the operation-specific and end-to-end
  ten-sample gate is still pending.
- **Int32 column-column comparison:** scalar and forced portable SIMD paths
  are wired through the predicate program with dual validity handling. Three
  Apple M4 Pro samples show about 1.20 ms SIMD versus 1.43 ms scalar; Auto
  remains disabled pending the required ten-sample and end-to-end gates.
- **Uint32 column-column comparison:** scalar and forced portable SIMD paths
  preserve unsigned ordering and dual validity semantics. Three Apple M4 Pro
  samples show about 1.20 ms SIMD versus 1.41 ms scalar; Auto remains disabled
  pending the required gate evidence.
- **Int64 column-column comparison:** the scalar path is wired through the
  predicate program with correct signed semantics. The portable SIMD candidate
  was measured at about 1.43 ms versus 1.41 ms scalar on three Apple M4 Pro
  samples and was removed as a regression.
- **Float64 column-column comparison:** the scalar path preserves IEEE NaN,
  infinity and signed-zero comparison semantics. Portable SIMD measured about
  1.44 ms versus 1.40 ms scalar on three Apple M4 Pro samples and was removed.
- **Int8/Int16/Uint8/Uint16/Uint64 column-column comparison:** typed scalar
  paths now use the same predicate program and dual-validity contract. No
  architecture SIMD candidate is claimed for these widths.
- **String equality/prefix/suffix/contains:** ordinary StringView predicates
  now share a formal scalar `kernel.Set` entry using Go byte semantics;
  dictionary predicates retain domain caching. No SIMD implementation is
  claimed before ASCII/mixed-Unicode measurements.
- The scalar baseline on 65,536 rows is about 198-224 microseconds for ASCII
  equality/prefix/suffix and 392-403 microseconds for mixed-Unicode contains,
  with zero allocations. These are baseline measurements, not SIMD admission.
- Ten one-second samples and `validation/simd-string-match-arm64.pprof` were
  generated from the final tree on Apple M4 Pro; they establish reproducible
  scalar evidence, but no SIMD comparison or Auto admission was made.
- The ten-sample expression benchmark (`BenchmarkStringMatchExpressionKernelMode`)
  measured scalar and Auto at roughly 311-321 microseconds with 8 B/op; since
  Auto has no distinct SIMD backend, this is an end-to-end baseline only.

## Required evidence for new candidates

### Reproducible Int32/Uint32 column-column gate

`scripts/bench_column32.sh` runs the operation-specific kernel matrix and the
complete Filter → computed Project lifecycle. It records native/emulated SIMD
state and vector width, then compares forced `mode=simd` with `mode=scalar` in
the same compiled test executable. `mode=auto` is a fallback control, not proof
of a SIMD improvement. Column-column kernels do not currently have KernelIDs;
the gate checks their actual dispatch functions instead of reporting unrelated
column-scalar capability entries.

The kernel matrix covers all five comparisons, streaming/bitmap/nullable-tail,
sparse/dense/equal data, and short/tail inputs. The query matrix covers both
types, GT/EQ, sparse/medium/dense matches and independent operand nulls. It
measures allocations and managed workspace peak in addition to execution time.
Its independent oracle checks every selected and projected value before timing.
Correctness tests additionally cover every bitmap tail, signed/unsigned boundary
values, 25 pairs of validity patterns, untouched input and output suffixes, and
zero kernel allocations.

Default runs use ten one-second samples per case and can take roughly an hour.
`SIMD_BENCHTIME`, `SIMD_BENCHCOUNT`, `COLUMN32_KERNEL_PATTERN`, and
`COLUMN32_QUERY_PATTERN` can narrow exploratory runs; such runs do not establish
complete admission. `GODEBUG=simd=0` checks emulation, and supported native
widths can be tested separately with `GODEBUG=simd=+128/+256/+512`. Each host
and width needs its own evidence. Cross-compilation is not an arm64 measurement.

Backend labels are excluded from benchstat row grouping, so scalar and candidate
results really are compared. Do not average away a regressing operator, null
profile or query. A successful script run produces evidence, not an admission
decision or an Auto dispatch change.

GT/EQ query measurements are not evidence for GE/LT/LE admission; add matching
query cases before promoting those operations. Whole-query mode changes can
also select other forced kernels, so they measure pipeline impact, not isolated
attribution to column comparison. For final admission, repeat balanced mode
orders on an idle native host to rule out time-correlated load. A completed run
has a `-complete.txt` marker; failed partial output is not a completed gate.
Existing evidence prefixes are never overwritten. Source hashes and frozen
executable identities are saved with the environment record.

- Same final build and architecture, at least ten one-second samples.
- At least 10% kernel improvement with `p < 0.05`.
- No related end-to-end regression over 3%.
- Correctness coverage for tail lengths, NULL density, selection density and
  scalar differential output.
