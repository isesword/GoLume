# Explicit Lazy timestamp RANGE

`LazyFrame.WithTimestampRange(lazy.TimestampRangeSpec)` creates a dedicated
logical TimestampRange node. The spec mirrors the physical milestone: OrderBy,
Column, Frame, CountRowsAlias, CountAlias, SumAlias and AvgAlias. Nonempty aliases
select outputs. `TimestampRangeFrame{Kind: WindowFrameRange, PrecedingNS: delta}`
is explicit; WindowFrameRows is zero and is rejected by this dedicated API.
Old ROWS frames and general Window APIs are unchanged.

The original pre-optimization plan must consist only of Scan and direct column
Projection upstream, at most 16 projection levels. The scan must be unfiltered,
expose a real ascending OrderingProvider on the original required canonical
nanosecond Timestamp key, and identify a pipeline Scan. The operator input is
exactly that key plus one plain Int64 value (which may be nullable). No implicit
sort, FILTER, expression projection/renaming, upstream Limit, partitioning,
spill, Auto or Bounded-policy fallback is accepted. Execution requires explicit
MemoryOnly and a finite session engine budget. These checks run before optimizer
rewrites, even when an invalid operation could otherwise be removed.

The optimizer retains the entire TimestampRange spec and both input dependencies.
Requested outputs are not eliminated by Select or aggregate projection pruning:
a requested SUM overflow remains an effect even if only key/count is selected.
Filters and Limits are not pushed through the peer boundary. Limit(1) must let
the physical source complete its peer; existing Limit(0) may avoid reading input,
but does not bypass original-plan/schema/policy admission. COUNT remains non-null
Int64, SUM nullable Int64, AVG nullable Float64 with N43 exact finalization.

Source-specific pushdown is deliberately not applied beneath this node. The
lowering uses the original source and actual identity physical
ProjectSource objects, not a synthetic ordering witness or a profile wrapper
which only claims the right operator name. Safe initial evidence uses a real
ordered in-memory source; it does not claim ordinary Parquet scans provide an
ordering proof. Profiling support must describe only actual nodes, not invent a
source/ordering lineage.

The dedicated lowering connects the original source and concrete identity
projections to N43 TimestampRange. Execution profiling is explicitly rejected
before opening the source: generic profile wrappers would obscure the original
lineage. Ordinary logical/physical explanation remains available and describes
the real TimestampRange node, not a synthetic ROWS operator.

The finite query budget is supplied by the normal prepared batch execution.
N43 retains that same query across child reads, peer accumulation, lookahead and
output publication, including Next calls with a plain context. Source scan and
operator reservations share the query; caller-owned source backing and external
codec/library RSS are not thereby certified as query-accounted memory. Output
batches retain the existing borrowed reader lifetime. Closing or cancelling the
reader releases operator/source resources according to the N43 contract.

Before this implementation was connected, independently compiled tests recorded
a real staged unsupported-lowering failure with zero source opens and zero
query usage. Those snapshots preserve the development evidence; the temporary
exported unsupported sentinel is not part of the final public API.
