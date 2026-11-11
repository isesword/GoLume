# Parquet predicate-statistics reuse

`ApplyPushdown` reuses decoded column statistics within one row-group predicate
analysis. A bounded four-entry cache avoids rebuilding Arrow column metadata and
validating/decoding the same statistics for repeated comparison, NULL, IN, string
or temporal leaves. Each row group and each call has independent cache state.
Immutable source clones and concurrent preparations never share mutable caches.

The cache stores both trusted statistics and unavailable (`nil`) results. Existing
raw-footer presence, null-count and physical-width validation, Arrow writer-version
and sort-order trust checks, SQL three-valued reasoning, NaN behavior and exact
integer/float comparison rules are unchanged. Errors still propagate. Cached
statistics are read-only; binding-specific conversion remains per predicate leaf.

The cache has fixed inline storage and round-robin eviction. At most four decoded
statistics remain cached, regardless of schema or predicate width. Eviction only
causes the existing decoder to run again. A wide or non-repeating predicate may
receive no reuse benefit. The cache is local planning metadata, not a new decoded
batch reservation or an RSS guarantee.

This does not implement page-index pruning, read fewer data pages, change the
residual row filter, cache file footers between streams, or change row-group
selection and pushdown reports. Those are separate optimizations with additional
file-lifetime, page-alignment and conservative-statistics requirements.

## Validation and benchmark

`metadata_cache_test.go` tests cached nil/trusted values, four-entry eviction,
row-group and call isolation, concurrent eight-column clone preparation, cached
versus uncached truth sets, and pushed versus unpruned row counts. It includes
NULLs, malformed/missing statistics, integer limits, values above 2^53, NaN,
infinities, and signed zero. Existing Parquet tests cover the other logical types
and demanded-error behavior.

`BenchmarkParquetMetadataPushdown` constructs a genuine small Parquet file with
128 row groups and eight optional int64 columns. It measures repeated
`ApplyPushdown`, not file scanning or query execution. Controls include absent
statistics, no predicate, single/distinct columns and wide predicates. Fixture
creation and footer loading are outside the timed region. It neither uses nor
replaces the repository's original million-row Parquet fixture.

Run the benchmark with `go test ./source/parquet -run '^$' -bench
'^BenchmarkParquetMetadataPushdown$' -benchmem`. Compare matched baseline/candidate
runs on the same host before making performance claims. Local results do not
establish storage-I/O, end-to-end query or cross-machine speedups.
