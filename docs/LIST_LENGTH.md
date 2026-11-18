# Native list element count

`expr.ListLength(value)` and `lazy.ListLength(value)` return an Int64 element
count for native `List<String>` and `List<Int64>`, the bounded flat Struct-item
profile, and checked two-level Int64/String Lists (see LIST_STRUCT.md and
NESTED_LIST.md). The outer count includes NULL inner List slots.
They do not change the string-only Unicode semantics of `Length`, or byte-count
semantics of `ByteLength`.

- A structurally valid NULL parent yields NULL, regardless of its hidden child values.
- A valid empty list yields zero.
- NULL child elements count as elements.
- Child string byte lengths do not affect the element count.
- Shapes outside those explicit profiles, opaque Arrow inputs and
  non-list inputs are rejected during binding.

```go
q := session.FromSource(src).
    Filter(expr.GT(lazy.ListLength(lazy.Col("tags")), lazy.Lit(1))).
    SelectExprs(lazy.ListLength(lazy.Col("tags")).Alias("tag_count"))
```

The constructor is also composable with existing list-producing expressions,
for example `expr.ListLength(expr.Split(expr.Str("a,,b"), expr.Str(",")))`
returns 3. Scalar numeric outputs can be used in If/Case, comparisons and
ordinary downstream numeric consumers. This is not general nested arithmetic
or a new list storage representation.

## Execution and safety boundary

The operator uses existing unary compilation, CSE and budgeted Int64 workspace
storage. The new logical operator is appended to the enumeration; previous
operator values are unchanged. String-specific length comparison specialization
is deliberately not extended. Fixed-width selected evaluation is not enabled
for nested inputs: Filter → Project retains its existing materialized fallback.
No SIMD speedup or performance acceptance is claimed.

Native column vectors may contain physical rows beyond Batch.Rows. The operator
validates and reads only the logical prefix, accepting nonzero child-offset
views under the existing range-materialization contract. It does not relax
canonical source-admission checks. Parent offsets, required validity words,
child bounds and referenced string offsets (including NULL child payload) are
validated before offset subtraction. Compiled child type must match actual
native storage. Result validity uses the expression register's parent validity,
including the active mask inside guarded If/Case regions. When a ListLength region executes, malformed structural storage is not treated
as a suppressible row-value error. A wholly inactive guarded region is skipped,
including its column validation.

Input storage is borrowed and not modified. Output storage and validity belong
to the workspace and expire at its next evaluation or Close. Budget growth
failure remains retryable; semantic error and cancellation follow existing
workspace closing semantics. Validation and result loops check cancellation.
Child payloads are not copied merely to compute their element counts.

## Focused validation

Tests are in `expr/list_length_test.go`, `lazy/list_length_test.go`, and
`physical/list_length_test.go`. Coverage includes scalar/requested-SIMD modes,
130/65-row validity boundaries, empty/shrinking/growing batches, parent and child
NULLs, logical prefixes and borrowed offset views, malformed native storage,
string-producing composition, actual guarded cast failures, budget denial and
retry, cancellation checkpoints, repeated lazy collection, generic length
comparison and proof of the materialized Filter → Project fallback.

```
go test -json ./expr ./lazy ./physical -run ListLength -count=1
GOEXPERIMENT=simd go test -json ./expr ./lazy ./physical -run ListLength -count=1
```

Initial development baseline: ab96126ba20029ce118b9bcdaf4dec9e2e746786.
Integrated on Collect v5 baseline 59fb785212c89164ef8f80927df347cbb7a1f2db.
Full gates and independent review are recorded separately.
