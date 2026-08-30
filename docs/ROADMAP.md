# Roadmap

## Completed

### Phase 1

- Arrow-compatible vectors and validity bitmaps.
- Scalar / Go 1.27 portable SIMD dispatch.
- Specialized numeric hash aggregation.
- Radix ownership, persistent workers and Parquet adapter.

### Phase 2.1

- PreparedQuery and reusable budget-accounted workspaces.
- SQL 3VL predicate program.
- Dictionary/plain-string and fixed composite group-by.
- Initial reusable join primitives.

### Phase 2.2

- Adaptive hash grow/rehash.
- Nullable single/composite group keys.
- SUM/COUNT/MIN/MAX/AVG SoA aggregate layouts.
- Dictionary/fixed-128 equality join primitives.
- Repository-level GoDoc checking.

### Phase 2.3 — Lazy planning and Parquet pushdown

- immutable LazyFrame and LogicalPlan;
- schema inference;
- Projection and Predicate Pushdown;
- conservative Parquet row-group pruning;
- Explain / ExplainOptimized / ExplainPhysical;
- Lazy -> PreparedQuery lowering.

### Phase 2.4 — Arrow RecordBatch and Lazy Join

- Arrow RecordBatch typed/opaque source;
- UTF-8 dictionary normalization;
- cached whole-batch pruning stats;
- logical Join and PreparedJoin;
- fixed/composite/dictionary equality joins.

### Phase 2.5 — Expression and composable physical DAG

- SelectExprs / WithColumns;
- compiled scalar program and expression dependency pruning;
- physical FilterSource / ProjectSource;
- columnar JoinSource;
- Join -> Filter/Projection/GroupBy/Agg;
- CollectFrame / PrepareFrame.

### Phase 2.6 — Typed expressions and streaming join

- typed scalar inference / Cast / numeric coercion;
- float/string/date/timestamp execution;
- shared ProjectionProgram with CSE;
- streaming JoinSource without probe-side pre-read;
- radix fixed-key join implementation;
- ExplainAnalyze.

### Phase 2.7 — operator diagnostics and parallel radix join

- operator-level Analyze topology/timing/row counters;
- Prepare/Execute phase separation;
- persistent-worker parallel radix build/probe;
- benchmark-backed large fixed-key auto-selection.

### Phase 2.8 — ordering, opaque Take, runtime feedback

- Lazy Limit / Sort / Top-K;
- nested Limit simplification and Sort+Limit -> Top-K rewrite;
- full-materialization budgeted stable Sort;
- bounded O(K) Top-K with candidate-buffer reuse;
- generic opaque Arrow row gather plus retained-array ownership;
- aggregate-result source for GroupBy/Agg -> Sort/TopK/Limit;
- bounded 1024-plan EWMA runtime feedback;
- aggregate CostModel feedback with explicit Hint precedence;
- Sort/Top-K and feedback strategy regressions.

### Phase 2.9 — Top-K key-only source replay

- Top-K Scan/Filter/Projection/ExprProjection first pass now has an independent
  key-only source projection;
- the payload source is replayed only for winner partitions;
- Join and aggregate-result inputs keep one physical input to avoid repeating
  prepared build or aggregation work.

### Phase 2.10 — UTF-8 and UTC temporal expressions

- Parquet UTF-8 STRING, DATE, and UTC TIMESTAMP_MILLIS/MICROS/NANOS scan;
- Arrow RecordBatch timestamp normalization to UTC Unix nanoseconds;
- contains/starts-with/ends-with predicates with SQL NULL semantics;
- Unicode character-count and UTC year/month/day/hour/minute/second expressions;
- direct filtering on string predicates and extracted temporal values.
- explicit UTC `TimestampSeconds`/`TimestampMillis`/`TimestampMicros`
  literals, normalized with compile-time overflow checks;
- checked signed/unsigned integer arithmetic and checked temporal `Add`/`Sub`
  (date32 days, date64 milliseconds, timestamp nanoseconds);
- conservative Parquet statistic pruning for DATE/timestamp comparisons and
  calendar extraction, plus all-NULL string-match row groups.

### Phase 2.11 — expression ordering keys

- `SortBy` / `TopKBy` with direct scalar-expression keys;
- hidden physical key materialization, preserving the logical output schema;
- direct-column ordering fast path retained;
- expression dependency-only Top-K first-pass replay;
- stable multi-key Sort/Top-K with NULL placement independent of direction.

### Phase 2.12 — parallel stable global sort

- materialized global sorts split contiguous source-order ordinals into
  independently stable runs, scheduled on the engine's bounded worker pool;
- a deterministic k-way merge uses the original ordinal as the equality
  tie-breaker, retaining full stable-sort semantics across run boundaries;
- legacy physical construction remains serial by default, while Lazy sessions
  use their configured worker count; this is CPU parallelism only—external
  sort/spill remains Phase 3 work.

### Phase 2.13 — RFC3339 and duration expressions

- explicit string-to-timestamp casts accept RFC3339 offset/Z values and the
  documented no-offset UTC form, then normalize to UTC nanoseconds;
- Go-duration literals and string-to-duration casts use nanoseconds, with
  checked timestamp/duration and duration/duration arithmetic;
- Arrow Duration source values normalize seconds, milliseconds, microseconds,
  and nanoseconds at ingestion; named IANA timezone conversion remains
  intentionally unsupported.

### Phase 2.14 — literal UTF-8 string transforms

- Unicode-whitespace `Trim`/`TrimStart`/`TrimEnd` and Unicode simple-case
  `Lower`/`Upper` projections;
- literal-only `Replace`/`ReplaceAll`, Unicode-code-point `Slice`, and
  Unicode-code-point `Find` with a direct column/literal filter path;
- `ByteLength` alongside the existing character-count `Length`;
- dictionary-string transformation materialization, NULL propagation, and
  UTF-8/empty-string/negative-slice regression coverage.

### Phase 2.15 — Window foundation

- `NewWindow(PartitionBy(...), OrderBy(...))` and `LazyFrame.WithWindows`;
- partition-local `row_number`, `rank`, and `dense_rank`, with input row order
  restored after computation;
- `ROWS BETWEEN` rolling count, sum, min, max, and average over numeric
  columns, including NULL/NaN/infinity and checked integer-result overflow;
- one shared materialization, ordering pass, and partition scan per window
  node, with hidden expression ordering keys removed before output;
- strict optimizer boundary: Filter, Limit, and Sort above a Window do not
  move beneath it; unused window outputs permit eliminating the full node;
- single global output partition, execution-time memory reservations,
  cancellation, and Arrow/Parquet row-pipeline coverage.

### Phase 2.16 — bounded regular expressions

- literal-pattern `RegexMatch` filters over plain and dictionary strings;
- capture-aware `RegexExtract`, with group zero selecting the full match and
  NULL for no match or an unmatched optional group;
- compile-time RE2 validation, 64-KiB pattern bounds, reusable compiled
  programs, and projection CSE without row-materializing pattern metadata;
- SQL three-valued predicate behavior plus Unicode, empty-match, invalid
  pattern/group, long-input, and Lazy pipeline coverage.

### Phase 2.17 — List<String> foundation

- Arrow-shaped `TypeList` fields with one explicit nullable/non-nullable string
  child and canonical uint32 parent/child offsets;
- specialized `StringListVector` preserving NULL parent, valid empty list,
  NULL element, and valid empty-string semantics;
- clone, filter/compact, Take, stable Sort/Top-K/Limit materialization,
  projection passthrough, window payload, and inner/outer join support;
- specialized Arrow `List<String>` and validated InMemory ingestion, while
  unsupported nested types remain opaque;
- nested byte accounting, strict malformed-offset/nullability rejection, and
  scalar/SIMD/Lazy/Arrow/race regression coverage;
- no new Auto-SIMD kernel for source-time offset validation or irregular
  variable-length gather; single-byte Split scanning remains benchmark-gated.

### Phase 2.18 — list-producing string expressions

- row-dependent UTF-8 `Split`, including NULL propagation, empty-element
  preservation, and Unicode code-point behavior for an empty delimiter;
- literal-pattern, capture-aware `RegexExtractAll`, preserving NULL optional
  captures and distinguishing no matches from a NULL input;
- fused `StringListVector` output with projection CSE, checked uint32 bounds,
  and session-budgeted workspace growth;
- a reproducible single-byte scanner benchmark; no custom Auto-SIMD kernel was
  admitted because `bytes.IndexByte` did not beat the scalar baseline by a
  stable margin on the validation host.

### Phase 2.19 — DataFrame ergonomics

- private owned `DataFrame` state with a compatibility `Frame` alias, safe
  value copies, idempotent Close, and result memory charged until closure;
- zero-copy borrowed Head/Tail/Slice views and typed chunked Series spanning
  multiple physical batches;
- explicit flattening APIs and a clearly slow `ToAnySlice` escape hatch;
- bounded grapheme-aware display with stable NULL, empty string, empty list,
  and nested NULL formatting.

### Phase 2.20 — unified DataFrame collection

- `Collect` and `Prepare` now produce DataFrame results for row and terminal
  aggregate plans through one columnar source pipeline;
- the specialized aggregate diagnostic API remains available as
  `CollectResult` and `PrepareResult`;
- deprecated `CollectFrame` and `PrepareFrame` spellings preserve a migration
  window while the old `Collect` return type intentionally changes.

### Phase 2.21 — bounded Struct and explicit-schema JSON

- specialized `TypeStruct` vectors with independent parent validity and
  row-aligned scalar or `List<String>` child vectors;
- `JSONDecode(value, targetSchema)` with stable schemas, SQL/JSON NULL
  semantics, ignored unknown fields, strict required children, path-aware
  errors, checked numeric conversion, and no inference;
- Struct payload support across clone, Filter/Take, stable Sort, Join,
  DataFrame/Series, bounded display, and nested memory accounting;
- specialized Arrow Struct ingestion for the same bounded child set; recursive
  Struct/List, Map, dictionary children, and schema evolution remain rejected.

### Phase 3.0–3.4 — streaming contract, spill, bounded aggregation, and Join

- one destination-fill `BatchStream` physical protocol with explicit borrowing,
  replay, cancellation, and idempotent close semantics;
- demand-driven pipelines plus query-owned, byte-bounded Exchange/fan-in/fan-out
  with stable sequence handling and source replay contracts;
- `EngineBudget -> QueryBudget -> OperatorBudget` accounting across retained
  state, in-flight batches, workspaces, output, and spill buffers;
- serialized soft/hard-limit spill arbitration with an explicit finite disk
  quota and lazily created query-scoped secure directories;
- atomic, checksummed, versioned spill files and an Arrow-shaped columnar run
  codec for fixed vectors, String/dictionary, `List<String>`, and Struct;
- no spill-codec SIMD or default compression: both remain evidence-gated, while
  existing batch kernels retain scalar/Auto/forced/no-SIMD validation.
- one GroupBy logical path with sorted-adjacent, radix-spill, and resident
  physical strategies; fixed-memory mode streams aggregate output by Batch and
  recursively repartitions oversized state runs;
- ExplainAnalyze reports aggregate spill, peak-memory, radix-depth, and skew
  diagnostics without replacing the native stream with a compatibility reader.
- adaptive Join planning selects byte-proven Broadcast Hash, ordering-proven
  Merge, or recursive Partitioned Hash Spill in the same physical graph;
- all six equality Join types preserve SQL NULL, dictionary-domain, composite
  key, and nested payload semantics while chunking high-amplification output;
- Join runtime feedback records build/probe bytes, multiplicity, amplification,
  skew, and spill; a partition-hash SIMD candidate was measured and rejected
  after a 3.389% end-to-end regression on darwin/arm64.

## Next — richer analytical operators and calibrated planning

Priority order:

1. **Nested type breadth**
   - generic `List<T>`, recursive Struct/List, Map, and schema inference remain
     separate work after the bounded Phase 2.21 contract;
   - normalization, case-folding, and collation only with a coherent
     equality/grouping/sorting policy.

2. **Ordering quality**
   - dictionary-aware/collation-aware ordering hooks;

3. **Runtime planning feedback breadth**
   - platform-calibrated ARM/x86 thresholds;
   - planner confidence/sample decay and source-version invalidation.

## Phase 3 — bounded streaming physical execution

- demand-driven pipelines and byte-bounded backpressure;
- query-level memory arbitration and cancellation-safe spill cleanup;
- spillable GroupBy/Join/Sort and external merge;
- streaming partial/final aggregation and batch/file sinks;
- explicit pipeline, blocking, memory and spill state in Explain/Analyze;
- operation-specialized SIMD kernels under architecture-specific benchmark
  gates.

The complete design and acceptance checklist is maintained in
[PHASE3_STREAMING_ENGINE_TODO.md](PHASE3_STREAMING_ENGINE_TODO.md). Execution
defaults, bounded-memory guarantees, fallback behavior, and public result APIs
are fixed by [ADR_PHASE3_EXECUTION_UX.md](ADR_PHASE3_EXECUTION_UX.md).

## Phase 4 — user-facing analytical engine

- broader DataFrame facade;
- optional SQL frontend targeting the same LogicalPlan;
- full window expressions;
- Arrow IPC/Flight/ADBC;
- Iceberg/Delta adapters;
- WASM validation after native execution is stable.

## Performance rule

Optimize work avoidance and data movement before micro-kernels. A physical
strategy is not enabled because it is theoretically attractive: it must win an
end-to-end benchmark on representative hardware and data distributions.
