# Timestamp RANGE: staged physical-only milestone

This slice adds a dedicated physical source, not general Window, Lazy lowering,
Auto planning, sorting, FILTER, partitioning, spill or calendar intervals.
Existing WindowFrame and logical.RowsFrame layouts remain unchanged.

API: NewTimestampRangeSource(input, TimestampRangeSpec) returns source.Source.
The spec contains OrderBy, Column, Frame and CountRowsAlias, CountAlias,
SumAlias, AvgAlias. Nonempty distinct aliases choose outputs, at least one.
TimestampRangeFrame contains Kind and PrecedingNS. WindowFrameRows is zero;
only explicit WindowFrameRange is admitted. PrecedingNS must be nonnegative.
Input is exactly a required plain nanosecond Timestamp and a plain Int64 value
(which may be nullable). Results retain both inputs. Counts are non-null Int64;
SUM is nullable Int64; AVG is nullable Float64 rounded once, nearest ties-even,
from the exact integer sum divided by valid count. Empty valid sets give NULL
SUM/AVG; intermediate sums do not cause overflow. Requested SUM is checked only
once the complete peer window is known; AVG-only does not require SUM to fit.

Original scan ordering must prove ascending timestamp. Only concrete direct,
identity ProjectSource and DropColumnsSource wrappers may intervene (depth 16);
operator-name strings alone do not establish projection lineage. Runtime order
is nondecreasing and timestamps are required. A peer is not complete until a
larger timestamp or EOF has been observed, including across source batches.
All rows of that peer share inclusive [t-delta,t]. If subtraction would go below
MinInt64, the lower bound saturates at MinInt64; expiry uses strictly less-than.
Zero delta still includes all future equal-timestamp peers.

Execution ownership: one budgeted dynamic ring owns active timestamp,
value and validity records. Current peers use that same ring, not a second
unbounded buffer. A source cursor retains the entire charged upstream batch as
lookahead until consumed. Complete peers finalize before any of their rows
are published in pages of at most min(normalized BatchRows,1024). The caller
must not retain borrowed output past Next/Close. A finite context QueryBudget
is mandatory and supplies both SourceScan and operator allocations. No separate
independent manager can evade the shared limit. Old/new ring growth, active
queue, upstream batch, fixed exact state and output page are simultaneously
charged. Total rows are capped at MaxInt64; backing capacities additionally
obey checked platform-size limits. A large peer may fail finite admission; it
is not silently truncated or spilled. No whole-process RSS guarantee is made.

The original admission/schema checkpoint deliberately returned
ErrTimestampRangeNotImplemented before opening input. Its independent oracle
failures are archived as staged expected failures, not old ROWS regressions.
Execution now implements peer-complete processing using the existing fixed
exact accumulator with signed add/evict; it does not allocate big integers.
Source EOF Close errors and cancellation are checked before the final peer is
published. A decreasing lookahead timestamp rejects the preceding unclosed
peer. Runtime vectors must match the exact two-column shape, and each delivered
batch validates required keys/value domains before use. Open revalidates the
current original-source proof and schema before opening the child.

The shared query supplies SourceScan and TimestampRange operator managers;
ReadOptions.MemoryManager is intentionally replaced, not trusted as an
unverifiable alternate budget. Internal same-package fault tests may wrap the
already-bound operator manager without changing its underlying accounting.
The initial fixed 4096-byte state reservation precedes that internal test hook.

Lazy/optimizer/Limit integration remains unclaimed. Physical consumers must
not truncate the input before this operator. A downstream Limit may stop output
only after the operator has already closed the required peer; the lookahead
need not imply reading the remainder of the source. Previous successful peers
can be observed before a later peer error, but no incomplete or overflowing
peer is partially published. Output storage is a separately admitted fixed-size
page and expires on the next Next call or Close.

Hard output limits are enforced before page reservation/allocation: full output
vector buffers (including nullable masks and retained input columns) must fit
both BatchBytes and a nonzero BatchPayloadBytes. Pages shrink within the complete
peer to fit; if even one output row cannot fit, a ResourceError wrapping
ErrBudgetExceeded is returned without publication. Descriptor/header allowances
are separately retained in the query reservation. The soft payload target is
still unsupported. A Next context without a query inherits the captured query
for the complete child call chain; a different query is rejected.
