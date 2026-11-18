# Configured NTILE windows

> Integration note: scope and semantics below are retained from the reviewed
> feature candidate. Stage-local test results and unpublished-state wording
> are historical; see [integration validation](DATE_WINDOW_INTEGRATION.md) for
> this branch update and its remaining limits.

`frame.WithConfiguredWindows(spec, lazy.NTile(4, "quartile"),
lazy.UseWindow(lazy.PercentRank("percent")))` evaluates both outputs using the
same partition and explicit ORDER BY. Existing WithWindows and its expression
struct remain unchanged. Logical/physical WindowNTile is appended as value 15.

NTILE accepts a positive constant Int64 bucket count. Each output is non-null
Int64, starting at 1. For N rows and B buckets, the first N%B buckets contain
N/B+1 rows, and the rest contain N/B rows. B>N produces buckets 1 through N.
Empty input produces no rows. NULL input rows count; ordering uses existing
NULL/NaN/descending behavior. Peers can split across buckets. Ties retain the
existing stable original-ordinal order. Results restore the original row order.
The implementation avoids overflowing position*B and handles MaxInt64 parameters.

ConfiguredWindowExpr has Expr and Buckets fields; configuration stays attached
to its expression through copying and projection pruning. NTILE requires an
empty Column and a strictly zero Frame. Other functions require Buckets=0 and
retain their original frame rules. Invalid, missing and irrelevant parameters
are rejected rather than silently ignored. No parameter is encoded in a column
name or disguised as a ROWS bound. The old public structs gain no fields.

Resident execution still captures all input. It computes from sorted positions
into separately admitted result vectors before replacing any input alias, then
serves bounded output pages while retaining the owner until borrow invalidation.
Ordered and external paths share the existing partition spool. Each distinct
bucket count needs an admitted private Int64 vector; equal counts can share one.
NTILE alone needs no peer lookahead. Mixed PR/CD/NTILE reads its source once;
only CumeDist needs the second cursor. Alias collisions cannot overwrite inputs
used by other expressions. External execution retains both sorts.

Logical plan/configuration objects have the same caller/plan lifetime as existing
window descriptors; they are not execution payload. New execution preparation
clones and bucket metadata require admission, as do resident output vectors and
per-page hidden vectors. Source tail borrows, encoded/decode ownership, output
buffers, and restore sort overlaps remain real costs. Live-page metadata may
exhaust the hard limit. This does not implement metadata pagination, deeper
nested payload support, or an input-independent resident bound.

Validation must cover all three paths, unequal buckets, ties, empty and singleton
partitions, multiple bucket counts, mixed windows, alias replacement, optimizer
pruning, actual spills, sink/Collect ownership, cancellation and resource errors.
Fixed existing budgets are not relaxed. This document is not a test-pass claim;
this isolated increment does not complete item 18 or unblock the protected
original full-repository gate. Prior failures and unpublished status remain.
