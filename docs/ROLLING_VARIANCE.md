# Exact reversible rolling variance

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`RollingVarPop(column, frame, alias)` and `RollingVarSamp(column, frame, alias)`
return nullable plain Float64. They accept the ten native signed, unsigned and
floating numeric types. Integers are not converted to Float64 before accumulation.
Both return the existing four-field WindowExpr, and can be wrapped in UseWindow
alongside configured NTILE. Enum values 16 and 17 are appended; old public
struct shapes and enum values remain unchanged.

Supported frames are finite ROWS endpoints (including preceding and following)
or UNBOUNDED PRECEDING through CURRENT ROW. Other frames, reversed bounds,
noncanonical offsets and overflowing ring capacities are rejected consistently.
This is not full SQL RANGE/GROUPS or arbitrary unbounded-following support.
Explicit ORDER BY remains required; results return in original input order.

NULL values do not enter the count. Population variance requires at least one
non-NULL value; sample variance requires at least two. The minimum-count check
comes before NaN/Inf handling. A frame meeting the count requirement that contains
any NaN or infinity returns a valid NaN. Removing that value restores finite
statistics. A finite singleton has population variance +0 and sample NULL.
Empty and all-NULL frames produce NULL.

Finite sums and sums of squares are maintained exactly with reversible native
integer arithmetic, not Welford deletion or rounded x*x. The result uses
(N*sumSquares - sum*sum)/(N*(N-ddof)), with ddof 0 or 1. Floating inputs retain
their exact binary values; final Float64 rounding alone can overflow to +Inf or
underflow to +0. No negative-result clamp masks cancellation error. Standard deviation is documented in [ROLLING_STDDEV.md](ROLLING_STDDEV.md);
it is computed from exact moments, never by taking the square root of an already
overflowed/underflowed Float64 variance.

Resident uses the true-capacity owned reader and separate result batches. A
single admitted moment object is reset and reused sequentially across aliases
and partitions. Monotone frame endpoints add/remove each input at most once;
resident does not rescan every frame. Outputs remain separately charged and
input aliases remain immutable until all expressions have completed.

Ordered trailing frames use one admitted moment state per alias plus the
existing finite ring when needed. Finite following/past endpoints reuse the
admitted lookahead/delay machinery and native cells. Each state includes the
kernel's full scratch bound. NULL bitmap, result vectors, rings, input borrows,
lookahead owners and external sort overlaps remain separately charged.

Pure variance needs no full-partition spool. External execution reports actual
front_sort and restore_sort spill counters; partition_spool stays unobserved
unless mixed PR/CD/NTILE really creates one. Such mixed queries still share one
source read and the original spool lifecycle. The flat native payload profile
is unchanged; opaque, Decimal and nested payloads remain rejected. No deferred
metadata pagination is added.

Verification must compare independent rational oracles, integer extremes,
subnormals, near-equal large values, special-value removal, NULL and clipped
frames across all three paths, plus actual spill, sinks/owned collection,
resource rejection, cancellation and cleanup. Fixed budgets and prior failures
remain recorded. Source completion is not a test pass or completion of item 18;
the protected original full gate and unpublished status remain unchanged.

## External-only finite lookahead pages

For an external query containing variance but no PR/CD/NTILE distribution
function, the newly created lookahead may copy smaller internal pages when all
expressions moved into that wrapper are variance. A requested reader page below
1024 rows is respected; zero defaults to 1024 and larger requests are capped at
1024 internally. This bounds producer-sized copies between the two sorts. It
does not alter old-only queries, ordered output batch boundaries, mixed
distribution queries, or the restore sort's public output batching.

The wrapper retains the original source batch and its unconsumed tail. It never
calls child Next until that tail has been cloned. The upstream sort owner remains
charged until the actual Next/Close; cloning a page is not permission to retire
it early. Each page retains the original conservative descriptor plus four-times
payload admission. Dictionary offsets/data are copied and charged in full for
every page, not estimated from selected codes. Smaller clones also reduce real
intermediate output and restore-sort input sizes. The original resource failure
and its diagnostics remain evidence; this design alone does not establish that
the unchanged fixed-budget case passes.

The same internal paging eligibility now also accepts the new standard-deviation
functions and Var/StdDev mixtures, but still requires all moved expressions to
be moments and excludes distribution windows. Old Var-only eligibility and
public batching remain unchanged.
