# Ordered grouped native Decimal SUM (N17 candidate)

The new grouped slice accepts one direct nullable or nonnullable Int64 key and
at least one root native Decimal SUM. It also permits COUNT(*) and COUNT(root
native Decimal), retaining N15's count and NULL rules. Input must have a proven
global ordering prefix on that key, across batches and partitions. For ordinary
unordered sources, explicitly request `Sort(Asc("key"))` before `GroupBy("key")`.
An explicit incompatible physical strategy is rejected, not silently replaced.

Ascending/descending and NULL-first/NULL-last follow the established key order.
NULL keys form one group regardless of hidden payload bits. Groups with no
selected rows are omitted: empty or completely filtered grouped input returns
zero rows, unlike the global one-row identity. A surviving all-NULL group has
NULL SUM and valid counts. Key appears first, followed by aggregates in requested
order; SUM is nullable Decimal(38,input scale), counts are nonnullable Uint64.

The adjacent operator uses exact signed256 SUM state with per-group valid-count
bounds and only final precision38 overflow, not float or an intermediate128 sum.
COUNT and selected-row/group statistics retain checked integer bounds. It
validates runtime key order against the declared proof, including direction and
NULL placement, with constant last-key state. Participation/bindings and valid
coefficients are checked for whole input batches before selection; NULL poisoned
values are ignored, not parsed as valid coefficients. Only the new grouped path
accepts unknown row/partition count -1; other negative counts and existing old
operator rules are not globally relaxed.

Output is a bounded borrowed page (at most64 groups, respecting smaller read and
output limits), not an unbounded result table. Current group, scratch, pending
input and output-page ownership overlap is charged. The stream uses one ordered
input worker rather than claiming parallel group merging. Earlier pages already
returned by direct BatchStream cannot be retracted if a later group overflows.
Collect fails without returning partial results; file sinks retain their existing
atomic commit/temporary-file cleanup semantics.

Supported grouped result interfaces are BatchStream and lazy Collect/IPC/Parquet.
N16 ExecuteDecimal remains global-only, even for Materialize=false: DecimalResult
has no group-key representation. Execute/ExecuteTyped grouped materialization
also remains unsupported for this lane. No arbitrary key types, multiple keys,
Decimal keys, COUNT-only native Decimal plans, AVG/Min/Max, resident hash grouping,
aggregate-state spill or generic nested Decimal are opened.

An explicit upstream N13 external Sort may spill Decimal payloads before adjacent
grouping. Those run/merge files belong to Sort, not the aggregate state. A bounded
aggregate does not make the whole Sort+GroupBy plan constant-memory/no-spill;
downstream Collect also retains all result groups within its own budget. Query
accounting does not claim complete third-party allocator/RSS coverage, and
cancellation remains checkpoint-based.

Independent tests use key/null-aware big.Int and count oracles, cross-batch
ordering, more than64 groups, exact IPC arrays, native Parquet reentry, a genuine
upstream multi-run/multi-pass sort and late overflow sink cleanup. Formal gates
and historical failures are reported separately, not erased by this candidate.

N18 separately permits [unordered resident Decimal grouping](DECIMAL_RESIDENT_GROUPED_SUM.md)
under explicit finite-budget ExecutionMemoryOnly. N17 ordered behavior is retained;
the new path is O(groups), has no result-order promise and does not expand
ExecuteDecimal beyond global plans.
