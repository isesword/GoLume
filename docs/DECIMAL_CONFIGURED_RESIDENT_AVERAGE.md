# Configured native Decimal AVG with unordered resident groups (N22 candidate)

The explicit target precision, scale and rounding mode for Decimal AVG now
also apply to unordered input with one nullable Int64 key, only under
MemoryOnly with a finite query budget. SUM and supported COUNT aggregates may
share the query. Output ordering is not guaranteed. Existing proven-ordered
queries still use the current-group streaming path.

The resident operator retains budgeted per-group states, then preflights exact
finalization for every group before yielding any result page. It recomputes AVG
when emitting output rather than retaining a second materialized result table.
This does not imply a performance improvement. Overflow during finalization
returns no page and cleans up retained state, index and output allocations.

There is no grouped aggregate spill, automatic sorting, additional key types,
implicit rounding or new public result layout. ExecuteDecimal remains global-only;
use grouped BatchStream or Lazy output.
