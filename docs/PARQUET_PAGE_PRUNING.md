# Parquet page pruning

The native Parquet batch stream can skip data pages within a retained row group.
This is separate from the existing footer-based row-group pruning and metadata
statistics cache. It uses optional Parquet column and offset indexes, then calls
Arrow's indexed `SeekToRow` for every demanded column at the same logical row.
The residual filter still runs on all emitted rows. The retained predicate is the
binder-owned snapshot, so later caller changes to an IN operand slice cannot
change deferred page selection.

## Supported first stage

- One signed INT32, INT64, or DATE32 predicate column supplies page bounds. Other
  predicate columns remain unknown. SQL NULL, AND, OR, NOT and supported IN/range
  expressions use the existing conservative truth-set evaluator.
- Every demanded column must be flat and have a valid offset index. Projected
  INT32/INT64, UINT32/UINT64, DATE32, BOOL and FLOAT32/FLOAT64 are supported.
  Page boundaries may differ between columns.
- Narrow integers, strings, decimals and timestamps disable page pruning for the
  entire scan. Their logical conversions can fail even in a rejected row, so
  sequential decoding is retained. A potentially erroring predicate also disables
  pruning, as it already does for row-group pruning.
- Floating-point columns are allowed as payloads, but do not supply page bounds
  in this stage. Float-only predicates, NaNs and unsupported expressions never
  gain stronger statistics from this implementation.
- Missing/untrusted/inconsistent optional metadata falls back to sequential
  reading. It does not imply that a malformed file is otherwise readable: Arrow
  can reject invalid footer index locations even on the ordinary scan path.

## Correctness checks

The planner checks type-defined signed ordering, Arrow writer-version trust,
exact physical bound widths, bound ordering, NULL-count presence and consistency,
and all-null pages. An absent nullable count is unknown, not zero. All-null pages
need no bounds. Statistics are evaluated over their own page's row count, never
an unrelated projected page or the whole row group.

Before seeking, every demanded offset index must cover its data chunk completely:
strictly increasing row starts beginning at zero, positive page sizes, contiguous
nonoverlapping byte ranges, no unindexed suffix, no dictionary/index/footer
intersection, and no arithmetic overflow. Index read ranges must lie before the
footer. The total coalesced index spans are limited to 1 MiB and each demanded
column to 65,536 pages for this optional optimization. Larger files/indexes remain
readable through the ordinary path. These limits are not an RSS guarantee or a
new aggregate metadata-memory quota. Before Arrow decodes an optional index, a
schema-aware compact-Thrift preflight validates field and list element types,
container counts, binary/string lengths and nesting without allocating declared
containers. Known fields must match Parquet types; unknown fields receive the
same bounded validation. Trusted footer statistics that prove a full match avoid
page-index reads altogether.

Retained adjacent anchor pages are coalesced into logical row ranges. All columns
seek to the same range start, including positions inside their own pages. Output
batches stop at range ends. A seek failure after a reader has moved is terminal;
it cannot fall back with mismatched column positions. Dictionary loading, nullable
levels and data-page V1/V2 remain Arrow's responsibility. Cancellation and ordinary
stream cleanup release readers and managed buffers.

Physically skipped page bodies are not decoded or validated. This optimization
preserves demanded logical-conversion errors through its restricted type domain;
it does not promise to discover corruption in bytes it never reads.

## Measurement and diagnostics

`parquet_page_pruned_rows` is the number of logical rows excluded by page plans.
`parquet_anchor_pages_pruned` counts rejected anchor pages, not physical pages
avoided across all columns. Buffered read-ahead and partially consumed payload
pages can still touch excluded bytes. Neither counter estimates byte savings.
Existing pushdown reports continue to describe row groups and their compressed
sizes; they do not include page-level byte estimates. Source statistics advertise
these row counts as upper bounds when a retained predicate may prune pages, while
partition row counts continue to describe the original retained groups. This
lets direct downstream consumers, including join build sides, admit fewer rows.

`page_pruning_io_test.go` generates real synthetic Parquet files independently of
the repository's original million-row fixture. Its one-row-group controls ensure
that row-group pruning cannot explain a result. It counts actual underlying
reads, separates data/dictionary/metadata traffic, checks untouched interiors of
rejected pages, and compares row IDs, validity, payload alignment and residual
results against an unpruned scan and an independent expected-row oracle. The
matrix includes V1/V2, dictionary/plain, optional/required, different column page
boundaries, multiple selected islands and small/large batches.

Index reads can increase metadata traffic. The planner validates individual
index buffers; Arrow data readers can read coalesced offset-index bytes again.
No speed or byte reduction is
promised for full-match predicates, absent indexes or unsupported types.

## Synthetic benchmark observation

On Linux/amd64, Go 1.27.3, AMD EPYC 9V74, fixed CPU affinity, GOMAXPROCS=1 and
GOGC=100, ten alternating-order pairs with 200 ms samples compared the same
native stream plus residual/checksum consumer. The generated V2 file contains
65,536 rows in one row group and eight projected scalar columns. The selective
predicate leaves 6,144 source candidates and exactly 232 residual rows in both
arms. File contents are cached locally; this is not a remote-storage benchmark.

- Plain selective: median 4.744 ms to 1.048 ms (-77.91%); allocation bytes -6.03%.
- Dictionary selective: median 5.627 ms to 1.239 ms (-77.97%); allocation bytes
  -29.47%.
- Full-match controls: no statistically significant timing difference in these
  samples (p=0.631 plain, p=0.105 dictionary). Allocation bytes increased 0.47%
  and 0.29%, respectively, from eligibility/footer-statistics checking.

These are workload-specific observations, not a general throughput promise or
proof of zero regression. The benchmark is reproducible with
`go test ./source/parquet -run '^$' -bench '^BenchmarkParquetPagePruning$' -benchmem`.
