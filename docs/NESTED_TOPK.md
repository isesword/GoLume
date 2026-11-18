# TopK with existing bounded containers (N9 candidate)

TopK carries existing recursive Struct, flat14 StructList and two-level
Int64/String List payloads under scalar keys. It does not add a container shape,
nested key, Decimal ordering or selected-container expression mode.

When both payload and key sources promise stable replay, key selection can scan
a narrow source and then read winning payload ordinals from the full source.
Batch boundaries may differ; logical row sequence and total count must match.
Otherwise TopK scans payload once, never opens the separate key source and
retains owned winning snapshots. Equal keys retain global input order. K=0 does
not consume rows, but opening/closing the selected stream remains allowed.

Retention is bounded by candidate count, not total input cardinality; individual
nested payloads, key cells, metadata and source buffers still consume memory.
For new extended snapshots, a replacement candidate must be fully admitted
before the old winner is released, so the temporary peak may include K+1
payloads. Existing flat snapshot replacement semantics remain unchanged. Failure
is terminal and cleans owned state; no full-sort/spill/unlimited fallback is
promised. Huge K and unknown statistics must not preallocate a huge candidate
array before rows are seen.

New typed, non-Opaque snapshot output is assembled into owned pages of at most
64 rows, honoring a smaller positive `ReadOptions.BatchRows`. A winner is
released only after its row has been successfully copied into the paid page.
The page and remaining snapshots overlap in memory and are both charged;
page construction is not an unbudgeted transfer or full-sort fallback. Mixed
Opaque snapshot payload keeps the established one-row output path rather than
concatenating arbitrary Arrow values. These page bounds apply to the new
snapshot path, not a new batching contract for replay or legacy flat TopK.

Owned Collect survives upstream closure. Reader output is a revocable borrow
until next read or Close. Native-only IPC and Parquet use existing schema
profiles. Mixed opaque Arrow payload is supported only through its established
transport: generic IPC still rejects TypeArrow without an explicit adapter.
Projecting native/Binary fields can use the existing exporter. StructList item
dictionary storage remains unsupported; ordinary Struct runtime dictionaries
retain their separate existing contract. Old temporal and top-level Decimal
input capability does not imply new Parquet mappings or Decimal sorting.

The candidate acceptance tests distinguish replay and one-pass routing using
observed sources, unknown Stats, mismatched batch sizes, stable tie oracles,
maximum existing depths, NULL/empty values, K0/1/K>N, budgets/cancellation,
borrow expiry, source-close ownership and sink readback. These assertions are
not a claim that current gates passed. Historical failures, opaque-export test
scope corrections and protected full-repository restrictions remain recorded.
