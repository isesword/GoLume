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

## Next — richer analytical operators and calibrated planning

Priority order:

1. **DataFrame result layer**
   - Phase 2.19: owned `DataFrame`, borrowed views, typed chunked `Series`, and
     bounded display;
   - Phase 2.20: unified DataFrame `Collect` and aggregate-result compatibility;
   - Phase 2.21: bounded Struct vectors and explicit-schema JSON decoding;
   - JSON only after Struct/Map vectors and output schema contracts exist;
   - normalization, case-folding, and collation only with a coherent
     equality/grouping/sorting policy.

2. **Ordering quality**
   - dictionary-aware/collation-aware ordering hooks;

3. **Runtime planning feedback breadth**
   - feed build/probe bytes, skew and duplicate multiplicity into Join planning;
   - platform-calibrated ARM/x86 thresholds;
   - planner confidence/sample decay and source-version invalidation.

## Phase 3 — bounded memory and spill

- query-level shared memory credits;
- spillable group-by partitions;
- spillable/radix joins;
- external sort/merge;
- streaming partial/final aggregation;
- cancellation-safe spill cleanup.

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
