# Phase 2.8 validation report

Validation date: 2026-08-24

## Toolchain and host

- Go: 1.27.0
- Arrow-Go: 18.7.0
- Host: linux/amd64, shared AMD EPYC 9V74 virtualized environment

The production module depends only on upstream Arrow-Go. The validation container has no normal outbound module access, so tests used a temporary external dependency closure (`go.offline.mod` plus optional-codec/mmap compatibility tags). Sandbox-only files are excluded from the release archive.

Raw outputs are retained under `validation/phase2.8-final/`.

## Final matrix

```text
gofmt ./...                                      PASS
go test ./... (scalar)                           PASS
GOEXPERIMENT=simd go test ./...                  PASS
GODEBUG=simd=0 targeted execution packages       PASS
GODEBUG=simd=+128 targeted packages              PASS
GODEBUG=simd=+256 targeted packages              PASS
GODEBUG=simd=+512 targeted packages              PASS
race lazy/physical/engine/optimizer/memory        PASS
race source/arrowrecord                           PASS
go vet ./...                                     PASS
GoDoc package/exported API convention check      PASS
```

## Limit / Sort / Top-K correctness

Validated:

- global Limit after residual Filter;
- multi-partition stable Sort;
- explicit Top-K;
- optimizer rewrite `Sort + Limit -> TopK`;
- projection dependency keeps ordering keys while pruning unrelated columns;
- predicate does not cross Limit/Top-K cardinality boundaries;
- aggregate output can feed Top-K/Limit;
- NULL ordering modes;
- Top-K stable tie behavior;
- Sort memory-budget rejection;
- Top-K memory-budget rejection and reservation release.

## Top-K allocation regression

The first Top-K implementation allocated a fresh key-cell slice for every input row. On the 262K-row Top100 fixture it produced approximately:

```text
22.8 ms/op
16.8 MB/op
262,351 allocs/op
```

The final implementation compares non-winning rows directly against the heap root and snapshots only admitted candidates. Retained cell/string buffers are reused when the root is replaced.

Final pinned single-CPU benchmark fixture:

```text
rows       = 262,144
columns    = id:int64, score:int64, payload:int64
ordering   = score DESC NULLS LAST
Top-K      = 100
GOMAXPROCS = 1
3 iterations per sample, 3 samples
```

Raw samples:

```text
Global Sort:
  671.099 ms/op   40,085,936 B/op   147 allocs/op
  653.670 ms/op   40,085,936 B/op   147 allocs/op
  700.708 ms/op   40,085,936 B/op   147 allocs/op

Top-K 100:
   14.320 ms/op       49,288 B/op   306 allocs/op
   13.761 ms/op       49,288 B/op   306 allocs/op
   14.129 ms/op       49,288 B/op   306 allocs/op
```

Medians:

```text
Sort median  = 671.099 ms
Top-K median =  14.129 ms
ratio        = ~47.5x
```

The allocation count for Top-K is not zero because it includes reader/output bookkeeping, winner maps, and K retained candidate structures. The important regression is that allocation is no longer proportional to N; bytes remain approximately 49 KB for this Top100 fixture.

These timings are algorithm-selection evidence for this validation host, not cross-machine performance claims.

## Opaque Arrow Take/materialization

A real Arrow `List<int32>` payload was passed through:

```text
Arrow RecordBatch
 -> Filter(x > 1)
 -> TopK(2, x DESC)
 -> Select(x,nested)
 -> CollectFrame
```

The resulting opaque nested rows remained:

```text
[40,41]
[30,31,32]
```

The output `OpaqueVector` owns a retained Arrow reference. `Frame.Close()` releases it. Generic row selection uses Arrow slices and concatenate rather than converting nested values to Go row objects.

## Aggregate -> ordering DAG

Validated:

```text
GroupBy(key)
 -> SUM(value), COUNT(*)
 -> Sort(sum DESC)
 -> Limit(2)
```

The optimizer rewrites the ordering tail to Top-K and the aggregate result is exposed as an internal typed columnar Source. Expected output keys/sums were returned exactly.

## Runtime feedback correctness

Feedback store properties validated:

```text
max plan signatures = 1024
update               = EWMA alpha 0.25
new plans            = FIFO bounded eviction
Reset                 = data + eviction order cleared
```

A two-sample direct store test produced expected selectivity:

```text
0.50 then 1.00
=> 0.625
```

and cardinality:

```text
20 then 4
=> rounded EWMA 16
```

## Runtime feedback changes CostModel

Regression fixture:

```text
rows        = 300,000
partitions  = 4
actual keys = 2 distinct int32 groups
aggregate   = SUM(value), COUNT(*)
workers     = 4
```

Before any observation, static defaults are:

```text
selectivity = 0.5
cardinality = 65,536
```

They satisfy the current radix threshold, so first Prepare selects `radix`.

After one successful Execute:

```text
selected rows = 300,000
actual groups = 2
```

The second Prepare reports one feedback sample, estimated cardinality 2, and selects a non-radix strategy. This proves the full feedback-to-CostModel loop changes physical planning rather than merely storing diagnostics.

A separate regression confirms explicit `WithHints` values override stored feedback while feedback sample count remains observable.

## Reference 1M-row Parquet regression

Query:

```text
ScanParquet
 -> Filter(pred > 32767)
 -> Select(key,value)
 -> GroupBy(key)
 -> SUM(value), COUNT(*)
```

Every execution returned:

```text
groups   = 219196
selected = 472475
checksum = 236716301
```

Final `ExplainAnalyze` sample:

```text
projection columns = [pred,key,value]
row groups         = 8/8
strategy           = radix
workers            = 4
radix_bits          = 8

ANALYZE
prepare = 86.548 us
execute = 47.066 ms
output_rows = 219196
selected_rows = 472475
```

After the Analyze execution populated runtime feedback, the subsequent physical plan reported:

```text
feedback_samples = 1
estimated_selectivity = 0.4506
estimated_cardinality = 219196
```

Prepared runs in that shared-host sample returned:

```text
19.990 ms
10.115 ms
9.637 ms
```

These timings are regression evidence only; the host is shared and they are not comparable with prior Apple M4 measurements.

## Current boundary

Implemented/validated:

- Limit / stable in-memory Sort / bounded Top-K;
- Sort+Limit -> Top-K rule;
- ordering-key projection dependencies and conservative predicate boundaries;
- memory-budgeted Sort and engine-managed Top-K;
- opaque Arrow gather/materialization and deterministic owned-reference release;
- aggregate-result source for downstream row operators;
- bounded EWMA aggregate planner feedback;
- user Hint precedence;
- existing Lazy/Pushdown/Prepared/Join/SIMD/Parquet regression suite.

Not claimed yet:

- external/spill sort;
- multi-worker merge sort;
- arbitrary expression sort keys without prior `WithColumns`;
- Top-K key-only first-pass child projection;
- collation-aware UTF-8 ordering;
- window functions;
- runtime-feedback-driven Join threshold adaptation;
- generic compute kernels for every opaque Arrow type.

## Clean release staging

The final archive must be created from a clean staging directory after removing sandbox-only dependency files. The staged tree is re-tested separately and must contain no:

- `go.offline.mod`;
- `tools/offline-poc`;
- local filesystem `replace` directive;
- validation-host absolute path in production source or user documentation.

### Release staging result

The clean staging tree itself was validated after sandbox-only files were removed:

```text
release hygiene path/replace scan                  PASS
scalar go test ./...                              PASS
GOEXPERIMENT=simd go test ./...                   PASS
race lazy/physical/engine/optimizer/memory         PASS
race source/arrowrecord                            PASS
go vet ./...                                      PASS
GoDoc checker                                     PASS
```

Raw clean-staging outputs are included as `validation/phase2.8-final/release-*.log`.
