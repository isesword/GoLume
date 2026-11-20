# Configured native Decimal AVG with ordered groups (N21 candidate)

Extends N20's explicit precision, scale and rounding options to the existing
ordered Decimal grouping path with one Int64 key. The source must provide proven
global order on that key (including its NULL ordering), for example via an
explicit upstream Sort. There is no unordered fallback, even in MemoryOnly.
The key is retained and each group yields its own nullable Decimal result,
rounded exactly once after the wide sum is divided by the non-NULL count.
SUM and supported COUNT aggregates can share the query.

The current group and bounded output page reuse the existing grouped
SUM/COUNT budget. Previous pages already consumed are not retracted on a
later failure. The aggregation itself does not spill; a preceding Sort may
spill. ExecuteDecimal's prepared caller-owned scalar result remains
global-only; use grouped BatchStream or Lazy output. No window AVG or
implicit rounding is introduced.
