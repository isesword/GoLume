# Unordered resident Decimal grouping (N18 candidate)

For input without a global ordering proof, this slice accepts the existing
single nullable Int64 key and root native Decimal SUM/COUNT shape only under
explicit `ExecutionMemoryOnly`, with a finite actual query budget. It retains
per-group state in memory: memory is O(groups × lanes), not constant with group
cardinality. Admission failure is an error, not a hidden sort, spill or unlimited
fallback. Auto/Bounded unordered plans still require an explicit Sort or a
conscious MemoryOnly choice. Proven ordered input continues through N17 adjacent
grouping with its prior behavior and fees.

At least one native Decimal SUM is required; optional COUNT(*) and COUNT(root
native Decimal) preserve their existing NULL/validity semantics. SUM is nullable
Decimal(38,input scale), exact signed256 accumulation and final precision38
checking. Empty/fully filtered input has zero groups. NULL is one distinct key,
not a valid zero key, and MinInt64/MaxInt64 are ordinary distinct keys.
Unordered result group order is not guaranteed. Any current first-appearance
order is an implementation detail, not an API contract; use an explicit output
Sort when order matters.

The operator consumes and closes its input, then preflights all groups' final
SUM results before exposing the first output page. Thus a later group's final
overflow fails before any page is returned. Output pages are bounded to64 groups
and copied into their own paid storage. Key index/arena, state arrays, scratch,
old/new growth overlap, predicate/input and pages are separate ownership costs.
Cancellation follows checkpoints, not an instantaneous interruption guarantee.
Finite query accounting is not a total process/RSS or third-party allocation
proof; downstream Collect retains results under its own accounting.

This path is single-worker resident state, not parallel merging or aggregate
spill. It does not add other key types, multiple keys, Decimal keys, AVG/Min/Max,
COUNT-only native Decimal or generic nested aggregates. N16 ExecuteDecimal
remains global-only even with Materialize=false. Existing Execute/ExecuteTyped
materialized record restrictions also remain. Supported grouped outputs are
BatchStream and lazy Collect/IPC/Parquet.

Tests compare by independent key maps, not observed hash/encounter order, with
unknown source statistics, unsorted repeated/NULL/extreme keys, exact cancellation,
all-NULL/filtered groups and independent Decimal/count oracles. They retain the
original64MiB limit for195 distinct groups, validate NativeNested Parquet input
through unsorted GroupBy to IPC/PQ and retain ordered N17 regressions. Actual
peaks, fault coverage and historical failures are reported separately. No
performance claim follows from this candidate.
