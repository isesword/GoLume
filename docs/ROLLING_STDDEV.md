# Standard deviation from exact rolling moments

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`RollingStdDevPop(column, frame, alias)` and
`RollingStdDevSamp(column, frame, alias)` return the existing WindowExpr. The new
logical and physical enum values are 18 and 19; all prior values and public
struct shapes remain unchanged. UseWindow allows either function to accompany
configured NTILE and other windows without new configuration fields.

The ten native numeric input types yield canonical nullable Float64. NULL values
are excluded. Population requires one non-NULL value and sample requires two;
that count check precedes special values. A qualifying frame containing any
NaN or infinity returns valid NaN; removing it recovers finite statistics.
A single finite value has population +0, sample NULL. Empty/all-NULL frames
are NULL. Integer values retain their exact native magnitude.

Finite results are derived by taking the square root of the exact rational
(N*sumSquares - sum*sum)/(N*(N-ddof)), not sqrt of a rounded variance. Thus a
Float64 variance overflowing or underflowing does not prematurely destroy a
representable standard deviation. The exact-moments kernel determines the final
Float64 rounding, including midpoint ties, subnormal results and final overflow.
The existing variance result method remains a separate dispatch target.

Frame scope matches variance: finite ROWS endpoints, including following, or
UNBOUNDED PRECEDING through CURRENT ROW. Explicit ORDER BY is required. Unsupported
frames, malformed parameters and nonnumeric input are rejected in all three
paths. This does not add RANGE/GROUPS, arbitrary unbounded following, nested,
opaque or Decimal payloads. All paths retain original input order.

Resident resets and reuses one already-admitted moments object across sequential
aliases and partitions while sliding bounds monotonically. Ordered and finite
lookahead use a distinct admitted moments state per alias plus the existing
rings/delay buffers. The common moment-result dispatcher selects population or
sample and variance or standard deviation explicitly. Sum/Avg exact plans are
unchanged. Generic unadmitted evaluation is rejected, not a fallback allocation.
Any kernel scratch growth is included in windowExactMomentsBytes at every
admission site; output vectors, NULL bitmaps, payload owners and ring costs remain
separate. This is live owned-memory accounting, not a process RSS bound.

The already established external-only lookahead paging may also serve pure
standard-deviation and Var/StdDev queries. It still requires no PR/CD/NTILE and
all moved expressions to be moments. It does not enable paging for moved Shift
or other old future expressions, old-only queries, or ordered public output.
The existing 1024-row internal cap, explicit smaller BatchRows, full dictionary
admission, unconsumed source-tail ownership, and final restore-sort batching are
unchanged. Both external sorts retain independent actual spill diagnostics.

Verification must cover independent exact midpoint/square oracles, finite
standard deviation when variance overflows or underflows, integer extremes,
subnormals, NULL and special-value removal, all supported frames and three
execution paths. Old variance tests remain applicable. Real resource/sink/owned
collection tests use unchanged budgets. No test pass or complete item-18/full
repository acceptance is asserted by this document. Earlier failures, protected
original gate and unpublished-state restrictions remain recorded.
