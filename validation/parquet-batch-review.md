# Bounded Parquet batch decoding review

Branch: `fix/foundation-cse-memory-lifecycle`.
Base: `6df109a92df9c80bad6092cca5fde88838d6249e`.

## Scope

The native BatchStream and prepared-partition interfaces decode Parquet row groups
incrementally. Existing NewReader/ReadPartition callers retain their legacy
whole-row-group contract. In particular, legacy engine.Result execution is not
made memory bounded by this change; native/DataFrame stream execution is the
intended bounded path.

The policy bounds managed GoLume decoded payloads and explicit scratch. Arrow-Go
page decompression, codec and dictionary buffers, file metadata, runtime/allocator
overhead, and GC-delayed reclamation are not an exact heap/RSS cap. A single
Parquet value can already cause third-party page decoding before GoLume sees its
length. Byte-limit rejection therefore protects GoLume copying and output size,
not a hard bound on malicious-file decompression or third-party allocations.

No Linux kernel-gate test or GitHub Actions setting is changed. No merge or
deployment is performed.

## Validation environment

Linux/amd64 with official Go 1.27.3 and unchanged dependencies. The source tree
was materialized through the GitHub connector and tests use
`GOFLAGS=-buildvcs=false`. No AGENTS.md or CONTRIBUTING file exists; repository
validation instructions and branch conventions were reviewed.

## Policy and ownership

ReadOptions now carries BatchRows (default 8192), BatchBytes (default 8 MiB),
and an optional MemoryManager. Negative limits are rejected. Pipelines pass
row/byte limits into scans before decoding, and query-owned scans use the
SourceScan workspace budget. Merge/spill joins and resident/sorted/spill
aggregates preserve these options; existing aggregate entry points delegate to
compatible new variants.

Fixed-width columns decode bounded chunks. Variable-width columns inspect one
candidate logical row through borrowed Arrow page views and copy it only after
reservation. If the next row does not fit, the current batch is returned and
that single candidate is retained without advancing its column readers. A row
that cannot fit alone fails with an error wrapping memory.ErrBudgetExceeded;
there is no silent oversized-batch fallback. Row capacity can shrink for a
smaller manager or a large admissible row. Replacement growth charges old-plus-new
allocation overlap and may produce a smaller batch than logical payload size
alone would suggest.

The cap covers row-scaled vector capacities, validity, typed dense/definition
scratch and copied string bytes. Fixed per-stream and per-column bookkeeping,
map/vector objects, inline one-value lookahead headers and reservation objects
are excluded, alongside third-party/runtime memory described above. Lookahead
payloads remain borrowed from Arrow pages and are never treated as owned copies.
BufferedStreamEnabled avoids whole compressed-column buffering, and Arrow's
PageStreamingEnabled is enabled for eligible codecs/encodings; unsupported pages
can still materialize. Column readers close on row-group rollover, failure and
early Close. Canceling a completed Next call's context does not poison the next
call; canceled active calls terminate and release resources.

Prepared partition order/subsets, projection, SQL nulls, narrow primitives,
booleans, UTF-8, dates and normalized timestamps remain compatible. Scan
ReplayUnknown remains unchanged: this patch does not promise projection-stable
batch IDs, nor make materializing downstream operators memory bounded.

## Verification

New regression cases cover row and byte boundaries, multi-column alignment,
prepared reordered/subset/empty partitions, zero-row files/groups, projection
excluding oversized columns, dictionary on/off and data-page v1/v2, all supported
primitive types, nulls/long UTF-8 strings, invalid UTF-8 and timestamp overflow,
manager rejection before output allocation, small manager adaptation, huge row
limits, cancellation/Close/error cleanup, per-call context lifetime, large-row
capacity adjustment, and recovery after a small first row group. Additional
source/pipeline/join/lazy tests verify complete options and query-budget routing.
The native-provider regression was observed failing against the old row-group
adapter before implementation.

Both uncached full scalar and SIMD suites ran. Their only failing test remains
unchanged `internal/kernelgate.TestGateErrors` at pipeline_test.go:211,
`re-gate without force: <nil>`, explicitly excluded by the requested scope.
`go vet ./...`, GoDoc, and SIMD race tests for source, source/parquet, pipeline,
physical, lazy and engine passed. Full-suite fixture tests use the previously
documented synthetic fallback, not the unavailable original benchmark binary.
New Parquet regression fixtures are generated independently in temporary test
directories. No fixture binary or symlink is committed.

Independent review checked Arrow APIs, buffered input, page-view lifetime,
prepared partitions, row alignment, numeric sizing/overflow, reserve-before-copy,
replacement overlap, manager ownership, cancellation and cleanup. Findings were
corrected and regression-tested before publication. Remote Actions is not
repaired or rerun, and local results are not a claim of green remote CI.

## Memory scaling and performance

The fixed-budget fixture uses constant-width values to avoid confusing data
width with input size. With 64-row batches and a 32 KiB batch cap, peak managed
reservations remain 12,280 bytes for 1,024 / 16,384 / 65,536 rows per group and for
1 / 8 / 64 groups. These are reservations, not process RSS measurements.

Benchmarks record first-output latency, rows/op, rows/s, Go allocated B/op and
managed peak separately. The final 64K-rowgroup samples (three runs, 300ms each,
two groups for full scans, 1024-row native batches) show the following medians:

- Mixed/string data: first output 26.56ms legacy versus 0.83ms native. The legacy
  first output contains 65,536 rows; native contains 1,024, so this is latency to
  useful output, not equal-work latency. Full scan of 131,072 rows takes 38.00ms
  versus 51.93ms, a throughput cost for bounded variable-width admission. Go
  allocations fall from about 65.65MB to 2.95MB per full scan; managed native
  peak is 196,360 bytes, below its 256KiB configured cap.
- Numeric-only projection of the same files: first output 5.37ms versus 0.50ms;
  full scan 16.63ms versus 6.37ms. Go allocations are about 9.17MB versus 1.44MB,
  with native managed peak 65,152 bytes.

Raw samples are in [parquet-batch-bench-linux-amd64.txt](parquet-batch-bench-linux-amd64.txt).
Timings are noisy local observations, not a universal throughput guarantee.
The string path performs row-by-row admission; a very large row can shrink
capacity to one for the rest of that row group, which is a further skew-sensitive
throughput tradeoff. Capacity is reconsidered at the next row group. Deterministic
resource/correctness tests guard bounds, not flaky wall-clock thresholds.

Final post-review checks also passed: focused uncached source/pipeline/physical/
engine/lazy tests, SIMD race for affected packages, vet/GoDoc, SIMD codegen and
structure/registry checks, SIMD-disabled affected package tests, and Linux/amd64
SIMD build. Kernel-gate test content and CI configuration remain unchanged.
