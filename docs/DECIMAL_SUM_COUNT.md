# Global native Decimal SUM with COUNT (N15 candidate)

This slice extends the dedicated global Decimal SUM stream, not the general
numeric aggregate protocol. At least one root native Decimal SUM is required.
Alongside it, `Count(alias)` counts selected rows including NULL values, and
`CountColumn(column, alias)` counts selected non-NULL values of a root native
Decimal column. Both COUNT results are nonnullable Uint64 and return zero for
empty or all-filtered input. SUM remains nullable Decimal(38, input scale), with
NULL ignored and empty/all-NULL returning NULL.

COUNT(*) retains its existing row-count contract: a manually populated Column
member does not turn it into COUNT(column). COUNT(column) with an empty, missing,
non-Decimal, nested or Opaque field is not accepted by this new lane. COUNT-only
plans on a native Decimal input are not newly authorized; existing optimizations
that prune unreferenced Decimal columns retain their previous behavior. Grouped
aggregation, other aggregate mixtures, AVG/Min/Max and nested Decimal arithmetic
remain outside this slice.

Output aliases and requested order are preserved. States are independent per
output slot, not CSE-deduplicated. SUM still uses signed256 intermediates, valid
count <= MaxInt64 and final precision38 overflow checks. Counts use checked
Uint64 accumulation. SelectedRows independently counts predicate-true rows,
including NULL, and is limited to MaxInt with its own error. Thus count storage
width does not promise executions with more than MaxInt selected rows.

The original result paths are BatchStream and lazy Collect/IPC/Parquet; N16 adds
the separate ExecuteDecimal endpoint described below. Legacy
Execute/ExecuteTyped materialization remains rejected for the whole mixed plan;
Materialize=false returns execution statistics and zero summary Checksum, not
a Decimal-value digest. Resident/hash and spillable aggregate entry points do
not substitute for the dedicated global sorted stream. Spill configuration may
coexist with bounded fixed-state execution; it does not enable aggregate state
spill or ignoring an incompatible forced strategy.

Pure SUM admission fees are unchanged. Mixed plans additionally prepay two
Uint64 count arrays (16 bytes per output slot) alongside the existing conservative
state/output envelopes. Predicate and input exchange allocations have separate
owners. Prepared schema metadata and third-party source allocations are not
claimed to be exactly covered by these query charges. Cancellation follows
existing checkpoint granularity, not arbitrary-instruction interruption.

Tests retain original memory limits and use exact coefficient/count oracles,
empty/all-NULL/filter cases, output order, independent IPC arrays and Parquet
native reentry. Test specifications are not complete-gate claims. Historical
failures and protected full-repository restrictions remain in force.

N16 adds the explicit Engine/PreparedQuery `ExecuteDecimal` materialization
endpoint with a separate `DecimalResult`; see [public Decimal results](DECIMAL_RESULT.md).
Legacy Execute/ExecuteTyped materialization restrictions and existing TypedValue
layout remain unchanged. This result API does not expand aggregate eligibility.

N17 separately adds [ordered single-Int64-key grouped SUM/COUNT](DECIMAL_GROUPED_SUM.md)
through BatchStream/Lazy only. Global identities, fees and ExecuteDecimal scope
remain unchanged; the new grouped path is not a general hash/spill aggregate.

N18 separately permits [unordered resident Decimal grouping](DECIMAL_RESIDENT_GROUPED_SUM.md)
under explicit finite-budget ExecutionMemoryOnly. N17 ordered behavior is retained;
the new path is O(groups), has no result-order promise and does not expand
ExecuteDecimal beyond global plans.
